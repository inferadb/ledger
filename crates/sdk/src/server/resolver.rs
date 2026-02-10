//! Server resolution from DNS domains and file manifests.

use std::{net::IpAddr, path::PathBuf, sync::Arc, time::Duration};

use hickory_resolver::{Resolver, config::ResolverConfig, name_server::TokioConnectionProvider};
use parking_lot::RwLock;
use tokio::sync::Notify;

/// Default DNS refresh interval (30 seconds).
const DEFAULT_DNS_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Default file refresh interval (60 seconds).
const DEFAULT_FILE_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Default port for discovered servers.
const DEFAULT_PORT: u16 = 50051;

/// Errors that can occur during server resolution.
#[derive(Debug, thiserror::Error)]
pub enum ResolverError {
    /// DNS resolution failed.
    #[error("DNS resolution failed for {domain}: {source}")]
    DnsResolution { domain: String, source: hickory_resolver::ResolveError },

    /// File manifest read failed.
    #[error("Failed to read server manifest from {}: {source}", path.display())]
    FileRead { path: PathBuf, source: std::io::Error },

    /// File manifest parse failed.
    #[error("Failed to parse server manifest from {}: {source}", path.display())]
    FileParse { path: PathBuf, source: serde_json::Error },

    /// No servers found.
    #[error("No servers found from {source_description}")]
    NoServers { source_description: String },
}

/// Result type for resolver operations.
pub type Result<T> = std::result::Result<T, ResolverError>;

/// Configuration for DNS-based server discovery.
#[derive(Debug, Clone, bon::Builder)]
#[builder(derive(Debug))]
pub struct DnsConfig {
    /// DNS domain name to resolve (e.g., `ledger.default.svc.cluster.local`).
    #[builder(into)]
    domain: String,

    /// Port to use for discovered addresses.
    #[builder(default = DEFAULT_PORT)]
    port: u16,

    /// How often to re-resolve DNS.
    #[builder(default = DEFAULT_DNS_REFRESH_INTERVAL)]
    refresh_interval: Duration,

    /// Use TLS for discovered peers.
    #[builder(default)]
    use_tls: bool,
}

impl DnsConfig {
    /// Returns the DNS domain.
    #[must_use]
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Returns the port for discovered servers.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Returns the refresh interval.
    #[must_use]
    pub fn refresh_interval(&self) -> Duration {
        self.refresh_interval
    }

    /// Returns whether TLS should be used.
    #[must_use]
    pub fn use_tls(&self) -> bool {
        self.use_tls
    }
}

/// Configuration for file-based server discovery.
#[derive(Debug, Clone, bon::Builder)]
#[builder(derive(Debug))]
pub struct FileConfig {
    /// Path to the server manifest JSON file.
    #[builder(into)]
    path: PathBuf,

    /// How often to re-read the file.
    #[builder(default = DEFAULT_FILE_REFRESH_INTERVAL)]
    refresh_interval: Duration,
}

impl FileConfig {
    /// Returns the file path.
    #[must_use]
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Returns the refresh interval.
    #[must_use]
    pub fn refresh_interval(&self) -> Duration {
        self.refresh_interval
    }
}

/// A server discovered through resolution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResolvedServer {
    /// Server address (host:port or IP:port).
    pub address: String,
    /// Whether to use TLS for this server.
    pub use_tls: bool,
}

impl ResolvedServer {
    /// Creates a new resolved server.
    #[must_use]
    pub fn new(address: impl Into<String>, use_tls: bool) -> Self {
        Self { address: address.into(), use_tls }
    }

    /// Returns the full URL for this server.
    #[must_use]
    pub fn url(&self) -> String {
        let scheme = if self.use_tls { "https" } else { "http" };
        format!("{scheme}://{}", self.address)
    }
}

/// Server manifest JSON format.
#[derive(Debug, serde::Deserialize)]
struct ServerManifest {
    servers: Vec<ServerEntry>,
}

/// A server entry in the manifest.
#[derive(Debug, serde::Deserialize)]
struct ServerEntry {
    address: String,
    #[serde(default)]
    tls: bool,
}

/// Source for discovering cluster servers.
#[derive(Debug, Clone)]
pub enum ServerSource {
    /// Static list of endpoint URLs.
    Static(Vec<String>),

    /// DNS domain to query for A records.
    Dns(DnsConfig),

    /// JSON file path containing server manifest.
    File(FileConfig),
}

impl ServerSource {
    /// Creates a static server source from endpoint URLs.
    pub fn from_static(endpoints: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::Static(endpoints.into_iter().map(Into::into).collect())
    }

    /// Creates a DNS server source.
    pub fn dns(config: DnsConfig) -> Self {
        Self::Dns(config)
    }

    /// Creates a file server source.
    pub fn file(config: FileConfig) -> Self {
        Self::File(config)
    }

    /// Auto-detects the server source type from a string.
    ///
    /// - If the string looks like a file path (starts with `/` or `.`), uses file discovery
    /// - If the string contains no `:` and no `://`, assumes DNS domain
    /// - Otherwise treats as a static endpoint URL
    pub fn auto_detect(value: impl Into<String>) -> Self {
        let value = value.into();

        // Check for file path
        if value.starts_with('/') || value.starts_with('.') {
            return Self::File(FileConfig::builder().path(PathBuf::from(value)).build());
        }

        // Check for URL (has scheme)
        if value.contains("://") {
            return Self::Static(vec![value]);
        }

        // Check for host:port format (likely static endpoint without scheme)
        if value.contains(':') && !value.contains('.') {
            // Ambiguous: could be DNS with port or localhost:port
            // Treat as static endpoint
            return Self::Static(vec![format!("http://{value}")]);
        }

        // Assume DNS domain
        Self::Dns(DnsConfig::builder().domain(value).build())
    }

    /// Returns the refresh interval for this source.
    #[must_use]
    pub fn refresh_interval(&self) -> Option<Duration> {
        match self {
            Self::Static(_) => None,
            Self::Dns(config) => Some(config.refresh_interval),
            Self::File(config) => Some(config.refresh_interval),
        }
    }
}

/// Resolves servers from various sources.
///
/// The resolver handles DNS A record queries and file manifest loading,
/// caching results and providing periodic refresh capabilities.
#[derive(Debug)]
pub struct ServerResolver {
    /// The server source configuration.
    source: ServerSource,

    /// Cached resolved servers.
    servers: Arc<RwLock<Vec<ResolvedServer>>>,

    /// DNS resolver (lazily initialized).
    dns_resolver: Arc<RwLock<Option<Resolver<TokioConnectionProvider>>>>,

    /// Shutdown signal.
    shutdown: Arc<Notify>,
}

impl ServerResolver {
    /// Creates a new server resolver.
    #[must_use]
    pub fn new(source: ServerSource) -> Self {
        Self {
            source,
            servers: Arc::new(RwLock::new(Vec::new())),
            dns_resolver: Arc::new(RwLock::new(None)),
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Returns the current resolved servers.
    ///
    /// If no servers have been resolved yet, returns an empty list.
    #[must_use]
    pub fn servers(&self) -> Vec<ResolvedServer> {
        self.servers.read().clone()
    }

    /// Resolves servers from the configured source.
    ///
    /// This performs the actual resolution (DNS query or file read) and
    /// updates the cached server list.
    ///
    /// # Errors
    ///
    /// Returns `ResolverError::DnsResolution` if DNS lookup fails,
    /// `ResolverError::FileRead` or `ResolverError::FileParse` if the
    /// manifest file cannot be read or parsed, or
    /// `ResolverError::NoServers` if resolution yields zero servers.
    pub async fn resolve(&self) -> Result<Vec<ResolvedServer>> {
        let servers = match &self.source {
            ServerSource::Static(endpoints) => self.resolve_static(endpoints),
            ServerSource::Dns(config) => self.resolve_dns(config).await?,
            ServerSource::File(config) => self.resolve_file(config).await?,
        };

        if servers.is_empty() {
            return Err(ResolverError::NoServers { source_description: self.source_description() });
        }

        // Update cache
        *self.servers.write() = servers.clone();

        Ok(servers)
    }

    /// Resolves static endpoints to servers.
    fn resolve_static(&self, endpoints: &[String]) -> Vec<ResolvedServer> {
        endpoints
            .iter()
            .map(|url| {
                let use_tls = url.starts_with("https://");
                // Strip scheme for address
                let address = url
                    .strip_prefix("https://")
                    .or_else(|| url.strip_prefix("http://"))
                    .unwrap_or(url);
                ResolvedServer::new(address, use_tls)
            })
            .collect()
    }

    /// Resolves DNS domain to servers.
    async fn resolve_dns(&self, config: &DnsConfig) -> Result<Vec<ResolvedServer>> {
        let resolver = self.get_or_create_dns_resolver().await;

        let lookup = resolver.lookup_ip(&config.domain).await.map_err(|source| {
            ResolverError::DnsResolution { domain: config.domain.clone(), source }
        })?;

        let servers = lookup
            .iter()
            .map(|ip: IpAddr| {
                let address = format!("{}:{}", ip, config.port);
                ResolvedServer::new(address, config.use_tls)
            })
            .collect();

        Ok(servers)
    }

    /// Resolves file manifest to servers.
    async fn resolve_file(&self, config: &FileConfig) -> Result<Vec<ResolvedServer>> {
        let content = tokio::fs::read_to_string(&config.path)
            .await
            .map_err(|source| ResolverError::FileRead { path: config.path.clone(), source })?;

        let manifest: ServerManifest = serde_json::from_str(&content)
            .map_err(|source| ResolverError::FileParse { path: config.path.clone(), source })?;

        let servers =
            manifest.servers.into_iter().map(|e| ResolvedServer::new(e.address, e.tls)).collect();

        Ok(servers)
    }

    /// Gets or creates the DNS resolver.
    async fn get_or_create_dns_resolver(&self) -> Resolver<TokioConnectionProvider> {
        // Check if already created
        {
            let guard = self.dns_resolver.read();
            if let Some(ref resolver) = *guard {
                return resolver.clone();
            }
        }

        // Create new resolver using system configuration
        let resolver = Resolver::builder_with_config(
            ResolverConfig::default(),
            TokioConnectionProvider::default(),
        )
        .build();

        // Store it
        {
            let mut guard = self.dns_resolver.write();
            if guard.is_none() {
                *guard = Some(resolver.clone());
            }
        }

        resolver
    }

    /// Returns a description of the server source for error messages.
    fn source_description(&self) -> String {
        match &self.source {
            ServerSource::Static(_) => "static endpoints".to_string(),
            ServerSource::Dns(config) => format!("DNS domain {}", config.domain),
            ServerSource::File(config) => format!("file {}", config.path.display()),
        }
    }

    /// Starts a background refresh task that periodically re-resolves servers.
    ///
    /// Returns a handle that can be used to stop the refresh task.
    pub fn start_refresh_task(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<()>> {
        let interval = self.source.refresh_interval()?;

        let resolver = Arc::clone(self);
        let shutdown = Arc::clone(&self.shutdown);

        Some(tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.tick().await; // Skip first immediate tick

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        if let Err(e) = resolver.resolve().await {
                            tracing::warn!(error = %e, "Server refresh failed");
                        }
                    }
                    _ = shutdown.notified() => {
                        tracing::debug!("Server resolver refresh task shutting down");
                        break;
                    }
                }
            }
        }))
    }

    /// Signals the refresh task to stop.
    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }
}

impl Clone for ServerResolver {
    fn clone(&self) -> Self {
        Self {
            source: self.source.clone(),
            servers: Arc::clone(&self.servers),
            dns_resolver: Arc::clone(&self.dns_resolver),
            shutdown: Arc::clone(&self.shutdown),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_dns_config_defaults() {
        let config = DnsConfig::builder().domain("example.com").build();

        assert_eq!(config.domain(), "example.com");
        assert_eq!(config.port(), DEFAULT_PORT);
        assert_eq!(config.refresh_interval(), DEFAULT_DNS_REFRESH_INTERVAL);
        assert!(!config.use_tls());
    }

    #[test]
    fn test_dns_config_custom() {
        let config = DnsConfig::builder()
            .domain("ledger.default.svc.cluster.local")
            .port(8080)
            .refresh_interval(Duration::from_secs(60))
            .use_tls(true)
            .build();

        assert_eq!(config.domain(), "ledger.default.svc.cluster.local");
        assert_eq!(config.port(), 8080);
        assert_eq!(config.refresh_interval(), Duration::from_secs(60));
        assert!(config.use_tls());
    }

    #[test]
    fn test_file_config_defaults() {
        let config = FileConfig::builder().path("/etc/ledger/servers.json").build();

        assert_eq!(config.path(), &PathBuf::from("/etc/ledger/servers.json"));
        assert_eq!(config.refresh_interval(), DEFAULT_FILE_REFRESH_INTERVAL);
    }

    #[test]
    fn test_file_config_custom() {
        let config = FileConfig::builder()
            .path("/custom/path.json")
            .refresh_interval(Duration::from_secs(120))
            .build();

        assert_eq!(config.path(), &PathBuf::from("/custom/path.json"));
        assert_eq!(config.refresh_interval(), Duration::from_secs(120));
    }

    #[test]
    fn test_resolved_server_url() {
        let http = ResolvedServer::new("10.0.0.1:50051", false);
        assert_eq!(http.url(), "http://10.0.0.1:50051");

        let https = ResolvedServer::new("10.0.0.1:443", true);
        assert_eq!(https.url(), "https://10.0.0.1:443");
    }

    #[test]
    fn test_server_source_from_static() {
        let source =
            ServerSource::from_static(["http://localhost:50051", "http://localhost:50052"]);

        match source {
            ServerSource::Static(endpoints) => {
                assert_eq!(endpoints.len(), 2);
                assert_eq!(endpoints[0], "http://localhost:50051");
            },
            _ => panic!("Expected Static variant"),
        }
    }

    #[test]
    fn test_server_source_auto_detect_file() {
        let source = ServerSource::auto_detect("/etc/ledger/servers.json");
        assert!(matches!(source, ServerSource::File(_)));

        let source = ServerSource::auto_detect("./servers.json");
        assert!(matches!(source, ServerSource::File(_)));
    }

    #[test]
    fn test_server_source_auto_detect_url() {
        let source = ServerSource::auto_detect("http://localhost:50051");
        match source {
            ServerSource::Static(endpoints) => assert_eq!(endpoints[0], "http://localhost:50051"),
            _ => panic!("Expected Static variant"),
        }

        let source = ServerSource::auto_detect("https://secure.example.com:443");
        match source {
            ServerSource::Static(endpoints) => {
                assert_eq!(endpoints[0], "https://secure.example.com:443")
            },
            _ => panic!("Expected Static variant"),
        }
    }

    #[test]
    fn test_server_source_auto_detect_dns() {
        let source = ServerSource::auto_detect("ledger.default.svc.cluster.local");
        match source {
            ServerSource::Dns(config) => {
                assert_eq!(config.domain(), "ledger.default.svc.cluster.local")
            },
            _ => panic!("Expected Dns variant"),
        }
    }

    #[test]
    fn test_server_source_refresh_interval() {
        let static_source = ServerSource::from_static(["http://localhost:50051"]);
        assert!(static_source.refresh_interval().is_none());

        let dns_source = ServerSource::dns(DnsConfig::builder().domain("example.com").build());
        assert_eq!(dns_source.refresh_interval(), Some(DEFAULT_DNS_REFRESH_INTERVAL));

        let file_source =
            ServerSource::file(FileConfig::builder().path("/etc/servers.json").build());
        assert_eq!(file_source.refresh_interval(), Some(DEFAULT_FILE_REFRESH_INTERVAL));
    }

    #[tokio::test]
    async fn test_resolve_static() {
        let resolver = ServerResolver::new(ServerSource::from_static([
            "http://10.0.0.1:50051",
            "https://10.0.0.2:443",
        ]));

        let servers = resolver.resolve().await.expect("resolution should succeed");

        assert_eq!(servers.len(), 2);
        assert_eq!(servers[0].address, "10.0.0.1:50051");
        assert!(!servers[0].use_tls);
        assert_eq!(servers[1].address, "10.0.0.2:443");
        assert!(servers[1].use_tls);
    }

    #[tokio::test]
    async fn test_resolve_static_empty_fails() {
        let resolver = ServerResolver::new(ServerSource::Static(vec![]));

        let result = resolver.resolve().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No servers found"));
    }

    #[tokio::test]
    async fn test_resolve_file() {
        let manifest = r#"{"servers": [{"address": "10.0.0.1:50051", "tls": false}, {"address": "10.0.0.2:443", "tls": true}]}"#;

        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let manifest_path = temp_dir.path().join("servers.json");
        tokio::fs::write(&manifest_path, manifest).await.expect("write manifest");

        let resolver = ServerResolver::new(ServerSource::file(
            FileConfig::builder().path(manifest_path).build(),
        ));

        let servers = resolver.resolve().await.expect("resolution should succeed");

        assert_eq!(servers.len(), 2);
        assert_eq!(servers[0].address, "10.0.0.1:50051");
        assert!(!servers[0].use_tls);
        assert_eq!(servers[1].address, "10.0.0.2:443");
        assert!(servers[1].use_tls);
    }

    #[tokio::test]
    async fn test_resolve_file_not_found() {
        let resolver = ServerResolver::new(ServerSource::file(
            FileConfig::builder().path("/nonexistent/path.json").build(),
        ));

        let result = resolver.resolve().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Failed to read"));
    }

    #[tokio::test]
    async fn test_resolve_file_invalid_json() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let manifest_path = temp_dir.path().join("invalid.json");
        tokio::fs::write(&manifest_path, "not valid json").await.expect("write file");

        let resolver = ServerResolver::new(ServerSource::file(
            FileConfig::builder().path(manifest_path).build(),
        ));

        let result = resolver.resolve().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Failed to parse"));
    }

    #[tokio::test]
    async fn test_servers_returns_cached() {
        let resolver = ServerResolver::new(ServerSource::from_static(["http://10.0.0.1:50051"]));

        // Initially empty
        assert!(resolver.servers().is_empty());

        // After resolve, cached
        resolver.resolve().await.expect("resolve should succeed");
        let cached = resolver.servers();
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].address, "10.0.0.1:50051");
    }

    #[test]
    fn test_resolver_clone_shares_state() {
        let resolver1 = ServerResolver::new(ServerSource::from_static(["http://10.0.0.1:50051"]));
        let resolver2 = resolver1.clone();

        // Modify through resolver1
        resolver1.servers.write().push(ResolvedServer::new("10.0.0.2:50051", false));

        // Should be visible through resolver2
        assert_eq!(resolver2.servers().len(), 1);
    }
}
