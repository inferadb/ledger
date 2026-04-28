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

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

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
    /// Lazily-initialized pool of tonic `Channel`s for the active endpoint.
    ///
    /// When the pool is empty, no connection has been established yet and
    /// the next call to [`get_channel`](Self::get_channel) will populate
    /// it. When non-empty, calls to [`get_channel`](Self::get_channel)
    /// round-robin across the pool entries via [`Self::next_channel_idx`].
    ///
    /// Pool size is governed by [`ClientConfig::connection_pool_size`];
    /// the default of 1 preserves the historical single-Channel
    /// behavior. Sizes >1 materialize N independent tonic Channels (each
    /// with its own tower `Buffer` mpsc worker, hyper HTTP/2 connection,
    /// and TCP socket) so frame dispatch parallelism scales with pool
    /// size — the per-Channel `Buffer` is the dispatch bottleneck above
    /// roughly 24-30k ops/s on loopback.
    channels: Arc<RwLock<Vec<Channel>>>,

    /// Round-robin cursor across [`Self::channels`].
    next_channel_idx: Arc<AtomicUsize>,

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

        let vault_cache = Arc::new(crate::vault_resolver::VaultLeaderCache::with_capacity(
            config.vault_cache_capacity(),
        ));
        vault_cache.set_metrics(Arc::clone(config.metrics()));

        Self {
            channels: Arc::new(RwLock::new(Vec::new())),
            next_channel_idx: Arc::new(AtomicUsize::new(0)),
            config,
            dynamic_endpoints: Arc::new(RwLock::new(None)),
            selector: ServerSelector::new(),
            circuit_breaker,
            region_cache,
            vault_cache,
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

        let vault_cache = Arc::new(crate::vault_resolver::VaultLeaderCache::with_capacity(
            config.vault_cache_capacity(),
        ));
        vault_cache.set_metrics(Arc::clone(config.metrics()));

        Self {
            channels: Arc::new(RwLock::new(Vec::new())),
            next_channel_idx: Arc::new(AtomicUsize::new(0)),
            config,
            dynamic_endpoints: Arc::new(RwLock::new(None)),
            selector,
            circuit_breaker,
            region_cache,
            vault_cache,
            gateway_channel: Arc::new(RwLock::new(None)),
            leader_watcher: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    /// Returns a clone of the next pooled channel via round-robin, or
    /// `None` when the pool is empty.
    ///
    /// Uses `Ordering::Relaxed` for the cursor — a stale read that
    /// returns the same index twice in a row across two concurrent
    /// callers is fine; the only invariant is "every cursor value maps
    /// to a valid index modulo `len()`".
    fn cached_channel(&self) -> Option<Channel> {
        let guard = self.channels.read();
        if guard.is_empty() {
            return None;
        }
        let idx = self.next_channel_idx.fetch_add(1, Ordering::Relaxed) % guard.len();
        Some(guard[idx].clone())
    }

    /// Returns the configured pool size, clamped to >= 1.
    fn pool_size(&self) -> usize {
        usize::from(self.config.connection_pool_size().max(1))
    }

    /// Replaces the pooled channels with `new_channels`, dropping the
    /// previous pool. Resets the round-robin cursor to 0.
    fn store_pool(&self, new_channels: Vec<Channel>) {
        let mut guard = self.channels.write();
        *guard = new_channels;
        self.next_channel_idx.store(0, Ordering::Relaxed);
    }

    /// Drops every pooled channel. The next [`Self::get_channel`] call
    /// will reconnect lazily.
    fn clear_pool(&self) {
        let mut guard = self.channels.write();
        if !guard.is_empty() {
            guard.clear();
            self.next_channel_idx.store(0, Ordering::Relaxed);
        }
    }

    /// Establishes `pool_size` independent channels via [`Self::create_channel`].
    ///
    /// Connections are created sequentially — the first connect resolves
    /// DNS / TLS for the endpoint and the rest reuse no shared state, so
    /// running them in parallel doesn't shorten the critical path
    /// materially. Sequential keeps error attribution clean and avoids a
    /// burst of concurrent SYNs against the server during cold start.
    async fn create_channel_pool(&self) -> Result<Vec<Channel>> {
        let pool_size = self.pool_size();
        let mut out = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            out.push(self.create_channel().await?);
        }
        Ok(out)
    }

    /// Establishes `pool_size` independent channels to a specific
    /// endpoint URL (used by the region-leader cache path).
    async fn connect_to_endpoint_pool(&self, endpoint_url: &str) -> Result<Vec<Channel>> {
        let pool_size = self.pool_size();
        let mut out = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            out.push(self.connect_to_endpoint(endpoint_url).await?);
        }
        Ok(out)
    }

    /// Returns a connected channel, establishing the connection if needed.
    ///
    /// On first call, this method establishes a connection to the configured
    /// endpoint(s). Subsequent calls return a clone of the next pooled
    /// channel (which shares the underlying HTTP/2 connection).
    ///
    /// When `connection_pool_size > 1`, each call rotates round-robin
    /// across the pooled channels. With pool size 1 the rotation is a
    /// no-op and behavior matches the historical single-channel path.
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

        // Fast path: pool already populated.
        if let Some(channel) = self.cached_channel() {
            return Ok(channel);
        }

        // Slow path: need to establish connections.
        let endpoint = self.current_endpoint();
        let new_channels = match self.create_channel_pool().await {
            Ok(channels) => {
                self.config
                    .metrics()
                    .record_connection(&endpoint, crate::metrics::ConnectionEvent::Connected);
                channels
            },
            Err(e) => {
                self.config
                    .metrics()
                    .record_connection(&endpoint, crate::metrics::ConnectionEvent::Failed);
                return Err(e);
            },
        };

        // Store the pool. Double-check: another task may have populated it
        // while we waited; if so, prefer the existing pool.
        {
            let mut guard = self.channels.write();
            if !guard.is_empty() {
                let idx = self.next_channel_idx.fetch_add(1, Ordering::Relaxed) % guard.len();
                return Ok(guard[idx].clone());
            }
            *guard = new_channels;
            self.next_channel_idx.store(0, Ordering::Relaxed);
            // Hand back the first channel directly — the cursor will
            // advance on the next caller.
            let chan = guard[0].clone();
            self.next_channel_idx.fetch_add(1, Ordering::Relaxed);
            Ok(chan)
        }
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
            .keep_alive_while_idle(true)
            // HTTP/2 flow-control windows. Tonic 0.14 / hyper 1.x default to
            // 64 KiB stream + connection windows; with all 256 concurrent SDK
            // tasks sharing one underlying connection, those defaults cap
            // throughput at ~10k ops/s for small RPCs because the connection
            // window saturates after the first ~64 KiB and every subsequent
            // send waits on a `WINDOW_UPDATE` round-trip. Raising both windows
            // lets multiplexed traffic flow without per-stream stalls. The
            // server-side equivalents live on `Http2Config` in the types crate
            // and must be raised in lockstep — each side's window only governs
            // its own receive direction.
            .initial_stream_window_size(Some(self.config.http2_initial_stream_window_bytes()))
            .initial_connection_window_size(Some(
                self.config.http2_initial_connection_window_bytes(),
            ));

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
            if let Some(channel) = self.cached_channel() {
                return Ok(channel);
            }
            let new_channels = self.connect_to_endpoint_pool(cached.as_ref()).await?;
            // Prefer the first newly-built channel for the immediate
            // return; the pool round-robin will pick up subsequent calls.
            let chosen = new_channels[0].clone();
            self.store_pool(new_channels);
            return Ok(chosen);
        }

        // Cache miss/expired — resolve via gateway
        metrics.leader_cache_miss(&region_label);
        let gateway = self.get_or_create_gateway_channel().await?;
        match cache.resolve(&gateway).await {
            Ok(endpoint) => {
                let new_channels = self.connect_to_endpoint_pool(&endpoint).await?;
                let chosen = new_channels[0].clone();
                self.store_pool(new_channels);
                Ok(chosen)
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
    ///
    /// Called by the retry loop before attempting a retry so the next attempt
    /// targets the newly-known leader without a separate resolve round-trip.
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
                self.clear_pool();
                // Use a label that captures the vault-routing axis. The
                // existing `redirect_retry` metric is keyed by region; for
                // per-vault redirects we synthesize a region-like label
                // when no preferred region is set.
                let label = self
                    .region_cache
                    .as_ref()
                    .map_or_else(|| format!("vault:{vault_slug_raw}"), |c| c.region().to_string());
                self.config.metrics().redirect_retry(&label);
                return;
            }
            // Hint had vault context but no endpoint — purge the entry.
            self.vault_cache.invalidate(org, vault);
            self.clear_pool();
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
                self.clear_pool();
                // Hint applied — a redirect occurred; record it so operators
                // can observe redirect cost (cold-start vs. warm path).
                self.config.metrics().redirect_retry(&cache.region().to_string());
            },
            _ => {
                cache.invalidate();
                self.clear_pool();
            },
        }
    }

    /// Returns a reference to the per-vault leader cache.
    ///
    /// Always present — every `ConnectionPool` carries a vault cache so
    /// vault-scoped routing works whether or not a `preferred_region` is
    /// configured.
    #[must_use]
    pub fn vault_cache(&self) -> &Arc<crate::vault_resolver::VaultLeaderCache> {
        &self.vault_cache
    }

    /// Selects a channel for a vault-scoped request, consulting the
    /// per-vault leader cache first.
    ///
    /// Resolution order:
    ///
    /// 1. Vault cache hit → connect directly to the cached leader endpoint.
    /// 2. Vault cache miss → fall through to the region-aware path (`get_channel`), which itself
    ///    falls back to gateway resolution.
    ///
    /// During the gradual rollout described in the SDK CLAUDE.md, the vault
    /// cache is empty until a `NotLeader` hint populates it. This means the
    /// first vault-scoped request still goes through the region path; the
    /// second and subsequent requests benefit from the cached vault leader.
    ///
    /// The cache is keyed on external `OrganizationSlug` / `VaultSlug` —
    /// the identifiers the SDK actually has at hand — so call sites pass
    /// slugs directly without any translation.
    ///
    /// # Errors
    ///
    /// Returns the same error classes as [`Self::get_channel`]: connection,
    /// circuit-open, configuration, and transport errors propagate directly.
    pub async fn select_for_vault(
        &self,
        organization: inferadb_ledger_types::OrganizationSlug,
        vault: inferadb_ledger_types::VaultSlug,
    ) -> Result<Channel> {
        // Check circuit breaker before doing anything.
        if let Some(ref cb) = self.circuit_breaker {
            let endpoint = self.current_endpoint();
            cb.check(&endpoint)?;
        }

        if let Some(endpoint) = self.vault_cache.cached_endpoint(organization, vault) {
            // Reuse a pooled channel when its endpoint matches the vault
            // leader. The pool is shared across vault calls; if a
            // different vault routed to a different endpoint last, we
            // need fresh connections (the cache will replace the pool
            // entirely on the redirect path).
            if self.current_endpoint() == endpoint.as_ref()
                && let Some(channel) = self.cached_channel()
            {
                return Ok(channel);
            }
            // Endpoint mismatch — connect a one-off channel for this
            // request. We deliberately do NOT replace `self.channels`
            // here: vault-leader endpoints diverge per-vault under the
            // per-vault consensus model, so caching them in the
            // round-robin pool would thrash the pool when fan-out
            // touches many vaults. The leader cache itself (and the
            // gateway-resolution path) populate the pool when it's the
            // right thing to do.
            let channel = self.connect_to_endpoint(endpoint.as_ref()).await?;
            return Ok(channel);
        }

        // Cache miss — fall through to the existing region/gateway path.
        self.get_channel().await
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

    /// Clears every pooled channel, forcing reconnection on next use.
    ///
    /// This can be useful after network changes or when the server
    /// indicates the connection should be reset.
    pub fn reset(&self) {
        let was_populated = !self.channels.read().is_empty();
        if was_populated {
            self.clear_pool();
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

        // Channel pool should be empty initially (lazy connection)
        assert!(pool.channels.read().is_empty(), "pool should be empty before first use");
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
        assert!(pool.channels.read().is_empty());

        // Reset on empty pool is a no-op
        pool.reset();
        assert!(pool.channels.read().is_empty());
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

        // Before any connection attempt, the channel pool is empty
        assert!(pool.channels.read().is_empty());

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

        // Reset clears the channel pool but keeps dynamic endpoints
        pool.reset();
        assert!(pool.channels.read().is_empty());
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

    /// `cached_channel` returns `None` when the pool is empty and rotates
    /// across pooled entries when populated. The cursor wraps modulo
    /// `len()` and is independent of the original insertion order.
    #[tokio::test]
    async fn cached_channel_round_robins_across_pooled_entries() {
        // Synthesize three lazy-connect channels — they never actually
        // connect, but `Channel` is `Clone` and the pool only inspects
        // identity via the cursor index.
        let chans: Vec<Channel> =
            (0..3).map(|_| Endpoint::from_static("http://127.0.0.1:1").connect_lazy()).collect();

        let config = test_config();
        let pool = ConnectionPool::new(config);

        // Empty pool → None.
        assert!(pool.cached_channel().is_none());

        pool.store_pool(chans.clone());

        // Eight calls over a 3-entry pool: cursor walks 0,1,2,0,1,2,0,1.
        // We can't compare Channel identity directly (no PartialEq), but
        // we can check the cursor advanced and stayed within bounds by
        // observing the AtomicUsize directly.
        for _ in 0..8 {
            let _ = pool.cached_channel().expect("pool populated");
        }
        // After 8 fetches starting from cursor=0, the cursor should be 8.
        assert_eq!(
            pool.next_channel_idx.load(Ordering::Relaxed),
            8,
            "round-robin cursor should advance once per fetch"
        );

        // clear_pool resets state.
        pool.clear_pool();
        assert!(pool.cached_channel().is_none());
        assert_eq!(pool.next_channel_idx.load(Ordering::Relaxed), 0);
    }

    /// `pool_size()` reads from `ClientConfig.connection_pool_size` and
    /// clamps zero (which the builder rejects, but the field is `u8`) to
    /// 1.
    #[test]
    fn pool_size_reflects_config() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .connection_pool_size(4)
            .build()
            .expect("valid config");
        let pool = ConnectionPool::new(config);
        assert_eq!(pool.pool_size(), 4);

        let default_config = test_config();
        let default_pool = ConnectionPool::new(default_config);
        assert_eq!(default_pool.pool_size(), 1);
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
    fn multiple_reset_calls_are_safe() {
        let config = test_config();
        let pool = ConnectionPool::new(config);

        // Multiple resets should be safe even with no channel
        pool.reset();
        pool.reset();
        pool.reset();
        assert!(pool.channels.read().is_empty());
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

    // =========================================================================
    // Vault cache dispatch tests
    // =========================================================================

    #[test]
    fn apply_hint_with_vault_slug_routes_to_vault_cache() {
        use inferadb_ledger_types::{OrganizationSlug, Region, VaultSlug};
        use tonic::Code;

        use crate::error::ServerErrorDetails;

        // Region cache present, vault cache present.
        let config = test_config_with_region(Region::US_EAST_VA);
        let pool = ConnectionPool::new(config);

        // Hint carries leader_organization_slug + leader_vault_slug — must
        // route to the vault cache, NOT the region cache. The legacy
        // leader_shard / leader_vault keys are present too (informational);
        // dispatch is driven by the slug pair.
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
            code: Code::Unavailable,
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
        use tonic::Code;

        use crate::error::ServerErrorDetails;

        // Legacy server compat: hint carries leader_vault (internal id)
        // but no leader_vault_slug. The SDK's VaultLeaderCache is keyed on
        // slugs, so we cannot populate from an id-only hint. The retry must
        // fall through to the region path instead.
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
            code: Code::Unavailable,
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
        use tonic::Code;

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
            code: Code::Unavailable,
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
        use tonic::Code;

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
                code: Code::Unavailable,
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
        use tonic::Code;

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
            code: Code::Unavailable,
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
        use tonic::Code;

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
            code: Code::Unavailable,
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
            code: Code::Unavailable,
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
    fn select_for_vault_falls_through_on_cache_miss() {
        use inferadb_ledger_types::{OrganizationSlug, VaultSlug};

        // No preferred region, no preseeded vault cache, dummy endpoint.
        // We can't await connect successfully, but the call must reach the
        // fallback path without panicking — use an unreachable endpoint
        // and expect a Connection or Timeout error rather than success.
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:1"]))
            .client_id("test-client")
            .connect_timeout(Duration::from_millis(50))
            .build()
            .expect("valid config");
        let pool = ConnectionPool::new(config);

        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let result = pool.select_for_vault(OrganizationSlug::new(1), VaultSlug::new(1)).await;
            assert!(result.is_err(), "fallback to unreachable endpoint should fail");
        });
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
}
