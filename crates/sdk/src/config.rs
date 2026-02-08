//! Client configuration with builder pattern.

use std::time::Duration;

use bon::bon;
use inferadb_ledger_types::config::ValidationConfig;

use crate::{
    error::{Result, SdkError},
    server::ServerSource,
    tracing::TraceConfig,
};

/// Default request timeout (30 seconds).
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Default connection timeout (5 seconds).
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Configuration for the Ledger SDK client.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Server source for discovering cluster servers.
    pub(crate) servers: ServerSource,

    /// Unique client identifier for idempotency tracking.
    pub(crate) client_id: String,

    /// Request timeout.
    pub(crate) timeout: Duration,

    /// Connection establishment timeout.
    pub(crate) connect_timeout: Duration,

    /// Retry policy for transient failures.
    pub(crate) retry_policy: RetryPolicy,

    /// Enable gzip compression for requests.
    pub(crate) compression: bool,

    /// TLS configuration for secure connections.
    pub(crate) tls: Option<TlsConfig>,

    /// Distributed tracing configuration.
    pub(crate) trace: TraceConfig,

    /// Input validation configuration for client-side request validation.
    pub(crate) validation: ValidationConfig,

    /// Circuit breaker configuration for per-endpoint failure protection.
    pub(crate) circuit_breaker: Option<crate::circuit_breaker::CircuitBreakerConfig>,

    /// SDK-side metrics collector.
    pub(crate) metrics: std::sync::Arc<dyn crate::metrics::SdkMetrics>,
}

#[bon]
impl ClientConfig {
    /// Creates a new client configuration with validation.
    ///
    /// # Arguments
    ///
    /// * `servers` - Server source for discovering cluster servers.
    /// * `client_id` - Unique client identifier for idempotency tracking.
    /// * `timeout` - Request timeout. Default: 30 seconds.
    /// * `connect_timeout` - Connection establishment timeout. Default: 5 seconds.
    /// * `retry_policy` - Retry policy for transient failures. Default: 3 attempts with exponential
    ///   backoff.
    /// * `compression` - Enable gzip compression for requests. Default: false.
    /// * `tls` - TLS configuration for secure connections.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Static endpoints are empty or invalid
    /// - Timeout is zero
    /// - Connect timeout is zero
    /// - Client ID is empty
    /// - TLS configuration is invalid
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{ClientConfig, TlsConfig, ServerSource};
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// // Static endpoints
    /// let config = ClientConfig::builder()
    ///     .servers(ServerSource::from_static(["http://localhost:50051"]))
    ///     .client_id("my-client")
    ///     .build()?;
    ///
    /// // DNS discovery
    /// use inferadb_ledger_sdk::DnsConfig;
    /// let config = ClientConfig::builder()
    ///     .servers(ServerSource::dns(DnsConfig::builder().domain("ledger.default.svc").build()))
    ///     .client_id("my-client")
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[builder]
    pub fn new(
        servers: ServerSource,
        #[builder(into)] client_id: String,
        #[builder(default = DEFAULT_TIMEOUT)] timeout: Duration,
        #[builder(default = DEFAULT_CONNECT_TIMEOUT)] connect_timeout: Duration,
        #[builder(default)] retry_policy: RetryPolicy,
        #[builder(default)] compression: bool,
        tls: Option<TlsConfig>,
        #[builder(default)] trace: TraceConfig,
        #[builder(default)] validation: ValidationConfig,
        circuit_breaker: Option<crate::circuit_breaker::CircuitBreakerConfig>,
        #[builder(default = crate::metrics::default_metrics())] metrics: std::sync::Arc<
            dyn crate::metrics::SdkMetrics,
        >,
    ) -> Result<Self> {
        // Validate static endpoints
        if let ServerSource::Static(ref endpoints) = servers {
            if endpoints.is_empty() {
                return Err(SdkError::Config {
                    message: "at least one endpoint is required for static server source"
                        .to_owned(),
                });
            }

            for endpoint in endpoints {
                validate_url(endpoint)?;
            }
        }

        if client_id.is_empty() {
            return Err(SdkError::Config { message: "client_id cannot be empty".to_owned() });
        }
        if timeout.is_zero() {
            return Err(SdkError::Config { message: "timeout cannot be zero".to_owned() });
        }
        if connect_timeout.is_zero() {
            return Err(SdkError::Config { message: "connect_timeout cannot be zero".to_owned() });
        }

        Ok(Self {
            servers,
            client_id,
            timeout,
            connect_timeout,
            retry_policy,
            compression,
            tls,
            trace,
            validation,
            circuit_breaker,
            metrics,
        })
    }
}

impl ClientConfig {
    /// Returns the server source configuration.
    #[must_use]
    pub fn servers(&self) -> &ServerSource {
        &self.servers
    }

    /// Returns the client identifier.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the request timeout.
    #[must_use]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Returns the connection timeout.
    #[must_use]
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Returns the retry policy.
    #[must_use]
    pub fn retry_policy(&self) -> &RetryPolicy {
        &self.retry_policy
    }

    /// Returns whether compression is enabled.
    #[must_use]
    pub fn compression(&self) -> bool {
        self.compression
    }

    /// Returns the TLS configuration if enabled.
    #[must_use]
    pub fn tls(&self) -> Option<&TlsConfig> {
        self.tls.as_ref()
    }

    /// Returns the trace configuration.
    #[must_use]
    pub fn trace(&self) -> &TraceConfig {
        &self.trace
    }

    /// Returns the validation configuration.
    #[must_use]
    pub fn validation(&self) -> &ValidationConfig {
        &self.validation
    }

    /// Returns the circuit breaker configuration if enabled.
    #[must_use]
    pub fn circuit_breaker(&self) -> Option<&crate::circuit_breaker::CircuitBreakerConfig> {
        self.circuit_breaker.as_ref()
    }

    /// Returns the SDK metrics collector.
    #[must_use]
    pub fn metrics(&self) -> &std::sync::Arc<dyn crate::metrics::SdkMetrics> {
        &self.metrics
    }
}

// Serde default functions for RetryPolicy
const fn default_max_attempts() -> u32 {
    3
}
fn default_initial_backoff() -> Duration {
    Duration::from_millis(100)
}
fn default_max_backoff() -> Duration {
    Duration::from_secs(10)
}
const fn default_multiplier() -> f64 {
    2.0
}
const fn default_jitter() -> f64 {
    0.25
}

/// Retry policy configuration.
#[derive(Debug, Clone, bon::Builder, serde::Serialize, serde::Deserialize)]
#[builder(derive(Debug))]
pub struct RetryPolicy {
    /// Maximum number of retry attempts (including initial attempt).
    #[builder(default = 3)]
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,

    /// Initial backoff duration before first retry.
    #[builder(default = Duration::from_millis(100))]
    #[serde(default = "default_initial_backoff")]
    pub initial_backoff: Duration,

    /// Maximum backoff duration.
    #[builder(default = Duration::from_secs(10))]
    #[serde(default = "default_max_backoff")]
    pub max_backoff: Duration,

    /// Backoff multiplier for exponential increase.
    #[builder(default = 2.0)]
    #[serde(default = "default_multiplier")]
    pub multiplier: f64,

    /// Jitter factor (0.0 to 1.0) for randomizing backoff.
    #[builder(default = 0.25)]
    #[serde(default = "default_jitter")]
    pub jitter: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl RetryPolicy {
    /// Creates a policy that never retries.
    #[must_use]
    pub fn no_retry() -> Self {
        Self::builder().max_attempts(1).build()
    }
}

/// Validates that a URL is well-formed HTTP(S).
fn validate_url(url: &str) -> Result<()> {
    // Basic validation - must start with http:// or https://
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err(SdkError::InvalidUrl {
            url: url.to_owned(),
            message: "URL must start with http:// or https://".to_owned(),
        });
    }

    // Check there's something after the scheme
    let rest = url.strip_prefix("http://").or_else(|| url.strip_prefix("https://")).unwrap_or("");

    if rest.is_empty() {
        return Err(SdkError::InvalidUrl {
            url: url.to_owned(),
            message: "URL must have a host".to_owned(),
        });
    }

    // Check for invalid characters
    if rest.contains(char::is_whitespace) {
        return Err(SdkError::InvalidUrl {
            url: url.to_owned(),
            message: "URL cannot contain whitespace".to_owned(),
        });
    }

    Ok(())
}

/// Default peer discovery refresh interval (60 seconds).
const DEFAULT_DISCOVERY_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Configuration for peer discovery.
///
/// When enabled, the SDK periodically queries the cluster for peer information
/// and updates its endpoint list for failover and load distribution.
///
/// # Example
///
/// ```
/// use std::time::Duration;
/// use inferadb_ledger_sdk::DiscoveryConfig;
///
/// let config = DiscoveryConfig::enabled()
///     .with_refresh_interval(Duration::from_secs(30));
/// ```
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// Whether discovery is enabled.
    enabled: bool,

    /// How often to refresh peer information.
    refresh_interval: Duration,
}

/// TLS configuration for secure connections.
///
/// Supports both PEM and DER certificate formats. When using DER format,
/// the SDK automatically converts to PEM internally for tonic compatibility.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::{TlsConfig, CertificateData};
/// // Simple TLS with native root certificates
/// let tls = TlsConfig::builder()
///     .use_native_roots(true)
///     .build()
///     .expect("valid TLS config");
///
/// // TLS with custom CA certificate
/// let tls = TlsConfig::builder()
///     .ca_cert(CertificateData::Pem(b"cert-data".to_vec()))
///     .build()
///     .expect("valid TLS config");
///
/// // Mutual TLS with client certificate
/// let tls = TlsConfig::builder()
///     .ca_cert(CertificateData::Pem(b"ca-cert".to_vec()))
///     .client_cert(CertificateData::Pem(b"client-cert".to_vec()))
///     .client_key(b"client-key".to_vec())
///     .domain_name("custom.example.com")
///     .build()
///     .expect("valid mTLS config");
/// ```
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// CA certificate for server verification.
    ca_cert: Option<CertificateData>,

    /// Client certificate for mutual TLS.
    client_cert: Option<CertificateData>,

    /// Client private key for mutual TLS.
    client_key: Option<Vec<u8>>,

    /// Domain name to verify against server certificate.
    /// If not set, the hostname from the endpoint URL is used.
    domain_name: Option<String>,

    /// Whether to use the system's native root certificates.
    use_native_roots: bool,
}

/// Certificate data that can be either PEM or DER encoded.
#[derive(Debug, Clone)]
pub enum CertificateData {
    /// PEM-encoded certificate data.
    Pem(Vec<u8>),
    /// DER-encoded certificate data.
    Der(Vec<u8>),
}

impl CertificateData {
    /// Converts the certificate to PEM format.
    ///
    /// If already PEM, returns as-is. If DER, wraps with PEM headers.
    #[must_use]
    pub fn to_pem(&self) -> Vec<u8> {
        match self {
            Self::Pem(data) => data.clone(),
            Self::Der(der) => {
                // Convert DER to PEM by base64 encoding and adding headers
                use std::io::Write;

                let base64 = base64_encode(der);
                let mut pem = Vec::new();
                writeln!(pem, "-----BEGIN CERTIFICATE-----").ok();
                // Write in 64-character lines
                for chunk in base64.as_bytes().chunks(64) {
                    pem.extend_from_slice(chunk);
                    pem.push(b'\n');
                }
                writeln!(pem, "-----END CERTIFICATE-----").ok();
                pem
            },
        }
    }
}

/// Simple base64 encoding for DER to PEM conversion.
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut result = String::new();
    let mut i = 0;

    while i < data.len() {
        let b0 = data[i];
        let b1 = data.get(i + 1).copied().unwrap_or(0);
        let b2 = data.get(i + 2).copied().unwrap_or(0);

        let n = (u32::from(b0) << 16) | (u32::from(b1) << 8) | u32::from(b2);

        result.push(ALPHABET[(n >> 18) as usize & 0x3F] as char);
        result.push(ALPHABET[(n >> 12) as usize & 0x3F] as char);

        if i + 1 < data.len() {
            result.push(ALPHABET[(n >> 6) as usize & 0x3F] as char);
        } else {
            result.push('=');
        }

        if i + 2 < data.len() {
            result.push(ALPHABET[n as usize & 0x3F] as char);
        } else {
            result.push('=');
        }

        i += 3;
    }

    result
}

#[bon]
impl TlsConfig {
    /// Creates a new TLS configuration with validation.
    ///
    /// # Arguments
    ///
    /// * `ca_cert` - CA certificate for server verification (PEM or DER format)
    /// * `client_cert` - Client certificate for mutual TLS
    /// * `client_key` - Client private key for mutual TLS
    /// * `domain_name` - Override domain for server certificate verification
    /// * `use_native_roots` - Whether to use system's native root certificates
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Client certificate is set but key is missing
    /// - Neither CA cert nor native roots are configured
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{TlsConfig, CertificateData};
    /// // With native roots
    /// let tls = TlsConfig::builder()
    ///     .use_native_roots(true)
    ///     .build()
    ///     .expect("valid TLS config");
    ///
    /// // With CA certificate
    /// let tls = TlsConfig::builder()
    ///     .ca_cert(CertificateData::Pem(b"cert-data".to_vec()))
    ///     .build()
    ///     .expect("valid TLS config");
    ///
    /// // Mutual TLS with client certificate
    /// let tls = TlsConfig::builder()
    ///     .ca_cert(CertificateData::Pem(b"ca-cert".to_vec()))
    ///     .client_cert(CertificateData::Pem(b"client-cert".to_vec()))
    ///     .client_key(b"client-key".to_vec())
    ///     .build()
    ///     .expect("valid mTLS config");
    /// ```
    #[builder]
    pub fn new(
        ca_cert: Option<CertificateData>,
        client_cert: Option<CertificateData>,
        client_key: Option<Vec<u8>>,
        #[builder(into)] domain_name: Option<String>,
        #[builder(default)] use_native_roots: bool,
    ) -> Result<Self> {
        // If client cert is set, key must also be set
        if client_cert.is_some() && client_key.is_none() {
            return Err(SdkError::Config {
                message: "client certificate requires a private key".to_owned(),
            });
        }

        // Must have some way to verify server certificate
        if ca_cert.is_none() && !use_native_roots {
            return Err(SdkError::Config {
                message: "TLS requires either a CA certificate or native roots".to_owned(),
            });
        }

        Ok(Self { ca_cert, client_cert, client_key, domain_name, use_native_roots })
    }

    /// Creates a TLS configuration that uses the system's native root certificates.
    ///
    /// This is a convenience method equivalent to:
    /// ```no_run
    /// # use inferadb_ledger_sdk::TlsConfig;
    /// TlsConfig::builder().use_native_roots(true).build()
    /// # ;
    /// ```
    ///
    /// # Errors
    ///
    /// This method should not fail as native roots satisfy validation requirements.
    pub fn with_native_roots() -> Result<Self> {
        Self::builder().use_native_roots(true).build()
    }

    /// Returns the CA certificate data if configured.
    #[must_use]
    pub fn ca_cert(&self) -> Option<&CertificateData> {
        self.ca_cert.as_ref()
    }

    /// Returns the client certificate data if configured.
    #[must_use]
    pub fn client_cert(&self) -> Option<&CertificateData> {
        self.client_cert.as_ref()
    }

    /// Returns the client private key if configured.
    #[must_use]
    pub fn client_key(&self) -> Option<&[u8]> {
        self.client_key.as_deref()
    }

    /// Returns the domain name override if configured.
    #[must_use]
    pub fn domain_name(&self) -> Option<&str> {
        self.domain_name.as_deref()
    }

    /// Returns whether native root certificates should be used.
    #[must_use]
    pub fn use_native_roots(&self) -> bool {
        self.use_native_roots
    }
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self { enabled: false, refresh_interval: DEFAULT_DISCOVERY_REFRESH_INTERVAL }
    }
}

impl DiscoveryConfig {
    /// Creates a disabled discovery configuration.
    #[must_use]
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Creates an enabled discovery configuration with default settings.
    #[must_use]
    pub fn enabled() -> Self {
        Self { enabled: true, refresh_interval: DEFAULT_DISCOVERY_REFRESH_INTERVAL }
    }

    /// Sets the refresh interval for peer discovery.
    ///
    /// Default: 60 seconds.
    #[must_use]
    pub fn with_refresh_interval(mut self, interval: Duration) -> Self {
        self.refresh_interval = interval;
        self
    }

    /// Returns whether discovery is enabled.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Returns the refresh interval.
    #[must_use]
    pub fn refresh_interval(&self) -> Duration {
        self.refresh_interval
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_config() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .build();

        assert!(config.is_ok());
        let config = config.unwrap();
        assert!(matches!(config.servers(), ServerSource::Static(_)));
        assert_eq!(config.client_id(), "test-client");
        assert_eq!(config.timeout(), DEFAULT_TIMEOUT);
        assert_eq!(config.connect_timeout(), DEFAULT_CONNECT_TIMEOUT);
    }

    #[test]
    fn test_config_with_multiple_endpoints() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://node1:50051", "http://node2:50051"]))
            .client_id("test-client")
            .build();

        assert!(config.is_ok());
        let config = config.unwrap();
        match config.servers() {
            ServerSource::Static(endpoints) => assert_eq!(endpoints.len(), 2),
            _ => panic!("Expected Static variant"),
        }
    }

    #[test]
    fn test_missing_endpoints() {
        // Note: With bon builders, missing required fields are now
        // enforced at compile-time, not runtime. This test verifies that an
        // *empty* endpoints vector fails at runtime validation.
        let result = ClientConfig::builder()
            .servers(ServerSource::Static(vec![]))
            .client_id("test-client")
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("endpoint"));
    }

    #[test]
    fn test_missing_client_id() {
        // Note: With bon builders, missing required fields (client_id) are now
        // enforced at compile-time, not runtime. This test verifies that an
        // *empty* client_id fails at runtime validation.
        let result = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("")
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("client_id"));
    }

    #[test]
    fn test_empty_client_id() {
        let result = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("")
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_url_no_scheme() {
        let result = ClientConfig::builder()
            .servers(ServerSource::from_static(["localhost:50051"]))
            .client_id("test-client")
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("http://"));
    }

    #[test]
    fn test_invalid_url_empty_host() {
        let result = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://"]))
            .client_id("test-client")
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_url_whitespace() {
        let result = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://local host:50051"]))
            .client_id("test-client")
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_zero_timeout() {
        let result = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .timeout(Duration::ZERO)
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("timeout"));
    }

    #[test]
    fn test_zero_connect_timeout() {
        let result = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .connect_timeout(Duration::ZERO)
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_custom_timeouts() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .build()
            .unwrap();

        assert_eq!(config.timeout(), Duration::from_secs(60));
        assert_eq!(config.connect_timeout(), Duration::from_secs(10));
    }

    #[test]
    fn test_compression_setting() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .compression(true)
            .build()
            .unwrap();

        assert!(config.compression());
    }

    #[test]
    fn test_custom_retry_policy() {
        let policy = RetryPolicy::builder()
            .max_attempts(5)
            .initial_backoff(Duration::from_millis(200))
            .max_backoff(Duration::from_secs(30))
            .multiplier(3.0)
            .jitter(0.5)
            .build();

        assert_eq!(policy.max_attempts, 5);
        assert_eq!(policy.initial_backoff, Duration::from_millis(200));
        assert_eq!(policy.max_backoff, Duration::from_secs(30));
        assert_eq!(policy.multiplier, 3.0);
        assert_eq!(policy.jitter, 0.5);
    }

    #[test]
    fn test_retry_policy_defaults() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_attempts, 3);
        assert_eq!(policy.initial_backoff, Duration::from_millis(100));
        assert_eq!(policy.max_backoff, Duration::from_secs(10));
        assert_eq!(policy.multiplier, 2.0);
        assert_eq!(policy.jitter, 0.25);
    }

    #[test]
    fn test_no_retry_policy() {
        let policy = RetryPolicy::no_retry();
        assert_eq!(policy.max_attempts, 1);
    }

    #[test]
    fn test_retry_policy_builder_equals_default() {
        let from_builder = RetryPolicy::builder().build();
        let from_default = RetryPolicy::default();

        assert_eq!(from_builder.max_attempts, from_default.max_attempts);
        assert_eq!(from_builder.initial_backoff, from_default.initial_backoff);
        assert_eq!(from_builder.max_backoff, from_default.max_backoff);
        assert_eq!(from_builder.multiplier, from_default.multiplier);
        assert_eq!(from_builder.jitter, from_default.jitter);
    }

    #[test]
    fn test_retry_policy_serde_round_trip() {
        let policy = RetryPolicy::builder()
            .max_attempts(5)
            .initial_backoff(Duration::from_millis(200))
            .max_backoff(Duration::from_secs(30))
            .multiplier(3.0)
            .jitter(0.5)
            .build();

        let json = serde_json::to_string(&policy).expect("serialize");
        let deserialized: RetryPolicy = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(deserialized.max_attempts, 5);
        assert_eq!(deserialized.initial_backoff, Duration::from_millis(200));
        assert_eq!(deserialized.max_backoff, Duration::from_secs(30));
        assert_eq!(deserialized.multiplier, 3.0);
        assert_eq!(deserialized.jitter, 0.5);
    }

    #[test]
    fn test_retry_policy_serde_with_defaults() {
        // Empty JSON should deserialize with all defaults
        let json = "{}";
        let policy: RetryPolicy = serde_json::from_str(json).expect("deserialize");

        assert_eq!(policy.max_attempts, 3);
        assert_eq!(policy.initial_backoff, Duration::from_millis(100));
        assert_eq!(policy.max_backoff, Duration::from_secs(10));
        assert_eq!(policy.multiplier, 2.0);
        assert_eq!(policy.jitter, 0.25);
    }

    #[test]
    fn test_https_url_valid() {
        let result = ClientConfig::builder()
            .servers(ServerSource::from_static(["https://secure.example.com:443"]))
            .client_id("test-client")
            .build();

        assert!(result.is_ok());
    }

    #[test]
    fn test_discovery_config_default_disabled() {
        let config = DiscoveryConfig::default();
        assert!(!config.is_enabled());
        assert_eq!(config.refresh_interval(), DEFAULT_DISCOVERY_REFRESH_INTERVAL);
    }

    #[test]
    fn test_discovery_config_disabled() {
        let config = DiscoveryConfig::disabled();
        assert!(!config.is_enabled());
    }

    #[test]
    fn test_discovery_config_enabled() {
        let config = DiscoveryConfig::enabled();
        assert!(config.is_enabled());
        assert_eq!(config.refresh_interval(), DEFAULT_DISCOVERY_REFRESH_INTERVAL);
    }

    #[test]
    fn test_discovery_config_custom_refresh_interval() {
        let config = DiscoveryConfig::enabled().with_refresh_interval(Duration::from_secs(30));
        assert!(config.is_enabled());
        assert_eq!(config.refresh_interval(), Duration::from_secs(30));
    }

    // TLS Configuration Tests

    #[test]
    fn test_tls_config_with_native_roots() {
        let tls = TlsConfig::with_native_roots().expect("valid config");
        assert!(tls.use_native_roots());
        assert!(tls.ca_cert().is_none());
    }

    #[test]
    fn test_tls_config_with_ca_cert_pem_bytes() {
        let pem_data = b"-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----\n";
        let tls = TlsConfig::builder()
            .ca_cert(CertificateData::Pem(pem_data.to_vec()))
            .build()
            .expect("valid config");

        assert!(tls.ca_cert().is_some());
        match tls.ca_cert().expect("has ca_cert") {
            CertificateData::Pem(data) => assert_eq!(data, pem_data),
            CertificateData::Der(_) => panic!("Expected PEM data"),
        }
    }

    #[test]
    fn test_tls_config_with_ca_cert_der_bytes() {
        let der_data = vec![0x30, 0x82, 0x01, 0x22]; // Mock DER header
        let tls = TlsConfig::builder()
            .ca_cert(CertificateData::Der(der_data.clone()))
            .build()
            .expect("valid config");

        assert!(tls.ca_cert().is_some());
        match tls.ca_cert().expect("has ca_cert") {
            CertificateData::Der(data) => assert_eq!(*data, der_data),
            CertificateData::Pem(_) => panic!("Expected DER data"),
        }
    }

    #[test]
    fn test_tls_config_with_client_cert_pem_bytes() {
        let cert_pem = b"-----BEGIN CERTIFICATE-----\ncert\n-----END CERTIFICATE-----\n";
        let key_pem = b"-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----\n";

        let tls = TlsConfig::builder()
            .ca_cert(CertificateData::Pem(b"ca".to_vec()))
            .client_cert(CertificateData::Pem(cert_pem.to_vec()))
            .client_key(key_pem.to_vec())
            .build()
            .expect("valid config");

        assert!(tls.client_cert().is_some());
        assert!(tls.client_key().is_some());
        assert_eq!(tls.client_key().expect("has key"), key_pem.as_slice());
    }

    #[test]
    fn test_tls_config_with_domain_name() {
        let tls = TlsConfig::builder()
            .ca_cert(CertificateData::Pem(b"cert".to_vec()))
            .domain_name("custom.example.com")
            .build()
            .expect("valid config");

        assert_eq!(tls.domain_name(), Some("custom.example.com"));
    }

    #[test]
    fn test_tls_config_validation_requires_ca_or_native_roots() {
        // Neither CA cert nor native roots — should fail validation at build()
        let result = TlsConfig::builder().build();

        assert!(result.is_err());
        let err = result.expect_err("should fail");
        assert!(
            err.to_string().contains("CA certificate") || err.to_string().contains("native roots")
        );
    }

    #[test]
    fn test_tls_config_validation_native_roots_is_sufficient() {
        let result = TlsConfig::builder().use_native_roots(true).build();

        assert!(result.is_ok());
    }

    #[test]
    fn test_tls_config_validation_ca_cert_is_sufficient() {
        let result = TlsConfig::builder().ca_cert(CertificateData::Pem(b"cert".to_vec())).build();

        assert!(result.is_ok());
    }

    #[test]
    fn test_tls_config_validation_client_cert_requires_key() {
        // Client cert without key should fail validation at build()
        let result = TlsConfig::builder()
            .ca_cert(CertificateData::Pem(b"ca".to_vec()))
            .client_cert(CertificateData::Pem(b"cert".to_vec()))
            // Missing client_key
            .build();

        assert!(result.is_err());
        let err = result.expect_err("should fail");
        assert!(err.to_string().contains("private key"));
    }

    #[test]
    fn test_tls_config_validation_client_cert_with_key_ok() {
        let result = TlsConfig::builder()
            .ca_cert(CertificateData::Pem(b"ca".to_vec()))
            .client_cert(CertificateData::Pem(b"cert".to_vec()))
            .client_key(b"key".to_vec())
            .build();

        assert!(result.is_ok());
    }

    #[test]
    fn test_certificate_data_pem_to_pem() {
        let pem_data = b"-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----\n";
        let cert = CertificateData::Pem(pem_data.to_vec());

        let converted = cert.to_pem();
        assert_eq!(converted, pem_data.to_vec());
    }

    #[test]
    fn test_certificate_data_der_to_pem() {
        // Simple DER data
        let der_data = vec![0x30, 0x03, 0x01, 0x01, 0xFF]; // Simple ASN.1 sequence
        let cert = CertificateData::Der(der_data);

        let pem = cert.to_pem();
        let pem_str = String::from_utf8(pem).unwrap();

        assert!(pem_str.contains("-----BEGIN CERTIFICATE-----"));
        assert!(pem_str.contains("-----END CERTIFICATE-----"));
    }

    #[test]
    fn test_base64_encode_basic() {
        // Test base64 encoding
        let data = b"Hello, World!";
        let encoded = base64_encode(data);
        assert_eq!(encoded, "SGVsbG8sIFdvcmxkIQ==");
    }

    #[test]
    fn test_base64_encode_empty() {
        let data = b"";
        let encoded = base64_encode(data);
        assert_eq!(encoded, "");
    }

    #[test]
    fn test_client_config_with_tls() {
        let tls = TlsConfig::with_native_roots().expect("valid TLS config");

        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["https://secure.example.com:443"]))
            .client_id("test-client")
            .tls(tls)
            .build();

        assert!(config.is_ok());
        let config = config.expect("valid config");
        assert!(config.tls().is_some());
        assert!(config.tls().expect("has tls").use_native_roots());
    }

    #[test]
    fn test_client_config_with_invalid_tls_fails() {
        // Building TlsConfig without CA cert or native roots should fail at build()
        let result = TlsConfig::builder().build();

        assert!(result.is_err());
    }

    #[test]
    fn test_tls_config_builder_chaining() {
        let tls = TlsConfig::builder()
            .ca_cert(CertificateData::Pem(b"ca-cert".to_vec()))
            .client_cert(CertificateData::Pem(b"client-cert".to_vec()))
            .client_key(b"client-key".to_vec())
            .domain_name("example.com")
            .build()
            .expect("valid config");

        assert!(tls.ca_cert().is_some());
        assert!(tls.client_cert().is_some());
        assert!(tls.client_key().is_some());
        assert_eq!(tls.domain_name(), Some("example.com"));
    }

    // ==================== TDD tests for bon builder API ====================

    #[test]
    fn test_tls_config_builder_with_native_roots() {
        // New bon builder API: TlsConfig::builder().use_native_roots(true).build()
        let result = TlsConfig::builder().use_native_roots(true).build();
        assert!(result.is_ok());
        let tls = result.unwrap();
        assert!(tls.use_native_roots());
    }

    #[test]
    fn test_tls_config_builder_with_ca_cert() {
        let ca_data = b"-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----\n";
        let result = TlsConfig::builder().ca_cert(CertificateData::Pem(ca_data.to_vec())).build();
        assert!(result.is_ok());
        let tls = result.unwrap();
        assert!(tls.ca_cert().is_some());
    }

    #[test]
    fn test_tls_config_builder_with_mtls() {
        let ca_data = b"ca-cert";
        let client_data = b"client-cert";
        let key_data = b"client-key";

        let result = TlsConfig::builder()
            .ca_cert(CertificateData::Pem(ca_data.to_vec()))
            .client_cert(CertificateData::Pem(client_data.to_vec()))
            .client_key(key_data.to_vec())
            .build();

        assert!(result.is_ok());
        let tls = result.unwrap();
        assert!(tls.ca_cert().is_some());
        assert!(tls.client_cert().is_some());
        assert!(tls.client_key().is_some());
    }

    #[test]
    fn test_tls_config_builder_validation_fails_without_ca_or_native() {
        // Neither CA cert nor native roots — should fail validation at build()
        let result = TlsConfig::builder().build();
        assert!(result.is_err());
        let err = result.expect_err("should fail");
        assert!(
            err.to_string().contains("CA certificate") || err.to_string().contains("native roots")
        );
    }

    #[test]
    fn test_tls_config_builder_validation_fails_client_cert_without_key() {
        let result = TlsConfig::builder()
            .ca_cert(CertificateData::Pem(b"ca".to_vec()))
            .client_cert(CertificateData::Pem(b"cert".to_vec()))
            // Missing client_key
            .build();

        assert!(result.is_err());
        let err = result.expect_err("should fail");
        assert!(err.to_string().contains("private key"));
    }

    #[test]
    fn test_tls_config_builder_with_domain_name() {
        let result =
            TlsConfig::builder().use_native_roots(true).domain_name("custom.example.com").build();

        assert!(result.is_ok());
        let tls = result.unwrap();
        assert_eq!(tls.domain_name(), Some("custom.example.com"));
    }
}
