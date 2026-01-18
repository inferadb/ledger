//! Client configuration with builder pattern.
//!
//! Provides type-safe configuration for SDK clients including:
//! - Endpoint URLs
//! - Timeouts and connection settings
//! - Retry policies
//! - Compression options

use std::time::Duration;

use snafu::ensure;

use crate::error::{ConfigSnafu, InvalidUrlSnafu, Result};

/// Default request timeout (30 seconds).
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Default connection timeout (5 seconds).
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Configuration for the Ledger SDK client.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Server endpoint URLs (e.g., `http://localhost:50051`).
    pub(crate) endpoints: Vec<String>,

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
}

impl ClientConfig {
    /// Creates a new configuration builder.
    #[must_use]
    pub fn builder() -> ClientConfigBuilder {
        ClientConfigBuilder::default()
    }

    /// Returns the configured endpoints.
    #[must_use]
    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
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
}

/// Builder for [`ClientConfig`].
#[derive(Debug, Default)]
pub struct ClientConfigBuilder {
    endpoints: Vec<String>,
    client_id: Option<String>,
    timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    retry_policy: Option<RetryPolicy>,
    compression: bool,
    tls: Option<TlsConfig>,
}

impl ClientConfigBuilder {
    /// Sets the server endpoint URLs.
    ///
    /// At least one endpoint must be provided. URLs must be valid HTTP(S) URLs.
    #[must_use]
    pub fn with_endpoints<I, S>(mut self, endpoints: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.endpoints = endpoints.into_iter().map(Into::into).collect();
        self
    }

    /// Adds a single endpoint URL.
    #[must_use]
    pub fn with_endpoint<S: Into<String>>(mut self, endpoint: S) -> Self {
        self.endpoints.push(endpoint.into());
        self
    }

    /// Sets the client identifier for idempotency tracking.
    ///
    /// This ID must be unique per client instance. It's used to track
    /// sequence numbers for duplicate request detection.
    #[must_use]
    pub fn with_client_id<S: Into<String>>(mut self, client_id: S) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Sets the request timeout.
    ///
    /// Default: 30 seconds.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets the connection establishment timeout.
    ///
    /// Default: 5 seconds.
    #[must_use]
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Sets the retry policy for transient failures.
    ///
    /// Default: [`RetryPolicy::default()`].
    #[must_use]
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    /// Enables gzip compression for requests.
    ///
    /// Default: disabled.
    #[must_use]
    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.compression = enabled;
        self
    }

    /// Sets the TLS configuration for secure connections.
    ///
    /// When TLS is enabled, endpoints should use `https://` URLs.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{ClientConfig, TlsConfig};
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = ClientConfig::builder()
    ///     .with_endpoint("https://secure.example.com:443")
    ///     .with_client_id("my-client")
    ///     .with_tls(TlsConfig::new()
    ///         .with_ca_cert_pem("/path/to/ca.pem"))
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Builds the configuration, validating all settings.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No endpoints provided
    /// - Any endpoint URL is invalid
    /// - Timeout is zero
    /// - Connect timeout is zero
    /// - Client ID is empty or not provided
    pub fn build(self) -> Result<ClientConfig> {
        ensure!(
            !self.endpoints.is_empty(),
            ConfigSnafu { message: "at least one endpoint is required" }
        );

        for endpoint in &self.endpoints {
            validate_url(endpoint)?;
        }

        let client_id = self
            .client_id
            .ok_or_else(|| ConfigSnafu { message: "client_id is required" }.build())?;

        ensure!(!client_id.is_empty(), ConfigSnafu { message: "client_id cannot be empty" });

        let timeout = self.timeout.unwrap_or(DEFAULT_TIMEOUT);
        ensure!(!timeout.is_zero(), ConfigSnafu { message: "timeout cannot be zero" });

        let connect_timeout = self.connect_timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT);
        ensure!(
            !connect_timeout.is_zero(),
            ConfigSnafu { message: "connect_timeout cannot be zero" }
        );

        // Validate TLS config if provided
        if let Some(ref tls) = self.tls {
            tls.validate()?;
        }

        Ok(ClientConfig {
            endpoints: self.endpoints,
            client_id,
            timeout,
            connect_timeout,
            retry_policy: self.retry_policy.unwrap_or_default(),
            compression: self.compression,
            tls: self.tls,
        })
    }
}

/// Retry policy configuration.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts (including initial attempt).
    pub max_attempts: u32,

    /// Initial backoff duration before first retry.
    pub initial_backoff: Duration,

    /// Maximum backoff duration.
    pub max_backoff: Duration,

    /// Backoff multiplier for exponential increase.
    pub multiplier: f64,

    /// Jitter factor (0.0 to 1.0) for randomizing backoff.
    pub jitter: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            multiplier: 2.0,
            jitter: 0.25,
        }
    }
}

impl RetryPolicy {
    /// Creates a new retry policy builder.
    #[must_use]
    pub fn builder() -> RetryPolicyBuilder {
        RetryPolicyBuilder::default()
    }

    /// Creates a policy that never retries.
    #[must_use]
    pub fn no_retry() -> Self {
        Self { max_attempts: 1, ..Default::default() }
    }
}

/// Builder for [`RetryPolicy`].
#[derive(Debug, Default)]
pub struct RetryPolicyBuilder {
    max_attempts: Option<u32>,
    initial_backoff: Option<Duration>,
    max_backoff: Option<Duration>,
    multiplier: Option<f64>,
    jitter: Option<f64>,
}

impl RetryPolicyBuilder {
    /// Sets the maximum number of attempts.
    #[must_use]
    pub fn with_max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = Some(attempts);
        self
    }

    /// Sets the initial backoff duration.
    #[must_use]
    pub fn with_initial_backoff(mut self, backoff: Duration) -> Self {
        self.initial_backoff = Some(backoff);
        self
    }

    /// Sets the maximum backoff duration.
    #[must_use]
    pub fn with_max_backoff(mut self, backoff: Duration) -> Self {
        self.max_backoff = Some(backoff);
        self
    }

    /// Sets the backoff multiplier.
    #[must_use]
    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = Some(multiplier);
        self
    }

    /// Sets the jitter factor (0.0 to 1.0).
    #[must_use]
    pub fn with_jitter(mut self, jitter: f64) -> Self {
        self.jitter = Some(jitter);
        self
    }

    /// Builds the retry policy.
    #[must_use]
    pub fn build(self) -> RetryPolicy {
        let defaults = RetryPolicy::default();
        RetryPolicy {
            max_attempts: self.max_attempts.unwrap_or(defaults.max_attempts),
            initial_backoff: self.initial_backoff.unwrap_or(defaults.initial_backoff),
            max_backoff: self.max_backoff.unwrap_or(defaults.max_backoff),
            multiplier: self.multiplier.unwrap_or(defaults.multiplier),
            jitter: self.jitter.unwrap_or(defaults.jitter),
        }
    }
}

/// Validates that a URL is well-formed HTTP(S).
fn validate_url(url: &str) -> Result<()> {
    // Basic validation - must start with http:// or https://
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return InvalidUrlSnafu { url, message: "URL must start with http:// or https://" }.fail();
    }

    // Check there's something after the scheme
    let rest = url.strip_prefix("http://").or_else(|| url.strip_prefix("https://")).unwrap_or("");

    if rest.is_empty() {
        return InvalidUrlSnafu { url, message: "URL must have a host" }.fail();
    }

    // Check for invalid characters
    if rest.contains(char::is_whitespace) {
        return InvalidUrlSnafu { url, message: "URL cannot contain whitespace" }.fail();
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
/// # use inferadb_ledger_sdk::TlsConfig;
/// // Simple TLS with CA certificate only (server verification)
/// let tls = TlsConfig::new()
///     .with_ca_cert_pem("/path/to/ca.pem");
///
/// // Mutual TLS with client certificate
/// let mtls = TlsConfig::new()
///     .with_ca_cert_pem("/path/to/ca.pem")
///     .with_client_cert_pem("/path/to/client.pem", "/path/to/client.key");
///
/// // With custom domain name override
/// let tls = TlsConfig::new()
///     .with_ca_cert_pem("/path/to/ca.pem")
///     .with_domain_name("custom.example.com");
/// ```
#[derive(Debug, Clone, Default)]
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

impl TlsConfig {
    /// Creates a new TLS configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a TLS configuration that uses the system's native root certificates.
    ///
    /// This is useful for connecting to servers with certificates signed by
    /// well-known certificate authorities.
    #[must_use]
    pub fn with_native_roots() -> Self {
        Self { use_native_roots: true, ..Self::default() }
    }

    /// Sets the CA certificate from a PEM file path.
    ///
    /// The certificate is read and stored. Any I/O errors will surface
    /// when the configuration is applied.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the PEM-encoded CA certificate file.
    #[must_use]
    pub fn with_ca_cert_pem(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        if let Ok(data) = std::fs::read(path.into()) {
            self.ca_cert = Some(CertificateData::Pem(data));
        }
        self
    }

    /// Sets the CA certificate from PEM bytes.
    #[must_use]
    pub fn with_ca_cert_pem_bytes(mut self, pem: impl AsRef<[u8]>) -> Self {
        self.ca_cert = Some(CertificateData::Pem(pem.as_ref().to_vec()));
        self
    }

    /// Sets the CA certificate from a DER file path.
    #[must_use]
    pub fn with_ca_cert_der(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        if let Ok(data) = std::fs::read(path.into()) {
            self.ca_cert = Some(CertificateData::Der(data));
        }
        self
    }

    /// Sets the CA certificate from DER bytes.
    #[must_use]
    pub fn with_ca_cert_der_bytes(mut self, der: impl AsRef<[u8]>) -> Self {
        self.ca_cert = Some(CertificateData::Der(der.as_ref().to_vec()));
        self
    }

    /// Sets the client certificate and key from PEM file paths for mutual TLS.
    ///
    /// # Arguments
    ///
    /// * `cert_path` - Path to the PEM-encoded client certificate file.
    /// * `key_path` - Path to the PEM-encoded private key file.
    #[must_use]
    pub fn with_client_cert_pem(
        mut self,
        cert_path: impl Into<std::path::PathBuf>,
        key_path: impl Into<std::path::PathBuf>,
    ) -> Self {
        if let (Ok(cert), Ok(key)) =
            (std::fs::read(cert_path.into()), std::fs::read(key_path.into()))
        {
            self.client_cert = Some(CertificateData::Pem(cert));
            self.client_key = Some(key);
        }
        self
    }

    /// Sets the client certificate and key from PEM bytes for mutual TLS.
    #[must_use]
    pub fn with_client_cert_pem_bytes(
        mut self,
        cert: impl AsRef<[u8]>,
        key: impl AsRef<[u8]>,
    ) -> Self {
        self.client_cert = Some(CertificateData::Pem(cert.as_ref().to_vec()));
        self.client_key = Some(key.as_ref().to_vec());
        self
    }

    /// Sets the client certificate and key from DER file paths for mutual TLS.
    ///
    /// Note: The private key should still be in PEM format as DER keys have
    /// various formats (PKCS#1, PKCS#8, SEC1) that require different handling.
    #[must_use]
    pub fn with_client_cert_der(
        mut self,
        cert_path: impl Into<std::path::PathBuf>,
        key_path: impl Into<std::path::PathBuf>,
    ) -> Self {
        if let (Ok(cert), Ok(key)) =
            (std::fs::read(cert_path.into()), std::fs::read(key_path.into()))
        {
            self.client_cert = Some(CertificateData::Der(cert));
            self.client_key = Some(key);
        }
        self
    }

    /// Sets the domain name for server certificate verification.
    ///
    /// Use this when the server's certificate CN/SAN doesn't match
    /// the hostname used in the endpoint URL.
    #[must_use]
    pub fn with_domain_name(mut self, domain: impl Into<String>) -> Self {
        self.domain_name = Some(domain.into());
        self
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

    /// Validates the TLS configuration.
    ///
    /// Returns an error if:
    /// - Client certificate is set but key is missing
    /// - Neither CA cert nor native roots are configured
    pub fn validate(&self) -> Result<()> {
        // If client cert is set, key must also be set
        if self.client_cert.is_some() && self.client_key.is_none() {
            return ConfigSnafu { message: "client certificate requires a private key" }.fail();
        }

        // Must have some way to verify server certificate
        if self.ca_cert.is_none() && !self.use_native_roots {
            return ConfigSnafu { message: "TLS requires either a CA certificate or native roots" }
                .fail();
        }

        Ok(())
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
            .with_endpoint("http://localhost:50051")
            .with_client_id("test-client")
            .build();

        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.endpoints(), &["http://localhost:50051"]);
        assert_eq!(config.client_id(), "test-client");
        assert_eq!(config.timeout(), DEFAULT_TIMEOUT);
        assert_eq!(config.connect_timeout(), DEFAULT_CONNECT_TIMEOUT);
    }

    #[test]
    fn test_config_with_multiple_endpoints() {
        let config = ClientConfig::builder()
            .with_endpoints(["http://node1:50051", "http://node2:50051"])
            .with_client_id("test-client")
            .build();

        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.endpoints().len(), 2);
    }

    #[test]
    fn test_missing_endpoints() {
        let result = ClientConfig::builder().with_client_id("test-client").build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("endpoint"));
    }

    #[test]
    fn test_missing_client_id() {
        let result = ClientConfig::builder().with_endpoint("http://localhost:50051").build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("client_id"));
    }

    #[test]
    fn test_empty_client_id() {
        let result = ClientConfig::builder()
            .with_endpoint("http://localhost:50051")
            .with_client_id("")
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_url_no_scheme() {
        let result = ClientConfig::builder()
            .with_endpoint("localhost:50051")
            .with_client_id("test-client")
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("http://"));
    }

    #[test]
    fn test_invalid_url_empty_host() {
        let result =
            ClientConfig::builder().with_endpoint("http://").with_client_id("test-client").build();

        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_url_whitespace() {
        let result = ClientConfig::builder()
            .with_endpoint("http://local host:50051")
            .with_client_id("test-client")
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_zero_timeout() {
        let result = ClientConfig::builder()
            .with_endpoint("http://localhost:50051")
            .with_client_id("test-client")
            .with_timeout(Duration::ZERO)
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("timeout"));
    }

    #[test]
    fn test_zero_connect_timeout() {
        let result = ClientConfig::builder()
            .with_endpoint("http://localhost:50051")
            .with_client_id("test-client")
            .with_connect_timeout(Duration::ZERO)
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_custom_timeouts() {
        let config = ClientConfig::builder()
            .with_endpoint("http://localhost:50051")
            .with_client_id("test-client")
            .with_timeout(Duration::from_secs(60))
            .with_connect_timeout(Duration::from_secs(10))
            .build()
            .unwrap();

        assert_eq!(config.timeout(), Duration::from_secs(60));
        assert_eq!(config.connect_timeout(), Duration::from_secs(10));
    }

    #[test]
    fn test_compression_setting() {
        let config = ClientConfig::builder()
            .with_endpoint("http://localhost:50051")
            .with_client_id("test-client")
            .with_compression(true)
            .build()
            .unwrap();

        assert!(config.compression());
    }

    #[test]
    fn test_custom_retry_policy() {
        let policy = RetryPolicy::builder()
            .with_max_attempts(5)
            .with_initial_backoff(Duration::from_millis(200))
            .with_max_backoff(Duration::from_secs(30))
            .with_multiplier(3.0)
            .with_jitter(0.5)
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
    fn test_https_url_valid() {
        let result = ClientConfig::builder()
            .with_endpoint("https://secure.example.com:443")
            .with_client_id("test-client")
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
    fn test_tls_config_default() {
        let tls = TlsConfig::new();
        assert!(tls.ca_cert().is_none());
        assert!(tls.client_cert().is_none());
        assert!(tls.client_key().is_none());
        assert!(tls.domain_name().is_none());
        assert!(!tls.use_native_roots());
    }

    #[test]
    fn test_tls_config_with_native_roots() {
        let tls = TlsConfig::with_native_roots();
        assert!(tls.use_native_roots());
        assert!(tls.ca_cert().is_none());
    }

    #[test]
    fn test_tls_config_with_ca_cert_pem_bytes() {
        let pem_data = b"-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----\n";
        let tls = TlsConfig::new().with_ca_cert_pem_bytes(pem_data);

        assert!(tls.ca_cert().is_some());
        match tls.ca_cert().unwrap() {
            CertificateData::Pem(data) => assert_eq!(data, pem_data),
            CertificateData::Der(_) => panic!("Expected PEM data"),
        }
    }

    #[test]
    fn test_tls_config_with_ca_cert_der_bytes() {
        let der_data = vec![0x30, 0x82, 0x01, 0x22]; // Mock DER header
        let tls = TlsConfig::new().with_ca_cert_der_bytes(&der_data);

        assert!(tls.ca_cert().is_some());
        match tls.ca_cert().unwrap() {
            CertificateData::Der(data) => assert_eq!(*data, der_data),
            CertificateData::Pem(_) => panic!("Expected DER data"),
        }
    }

    #[test]
    fn test_tls_config_with_client_cert_pem_bytes() {
        let cert_pem = b"-----BEGIN CERTIFICATE-----\ncert\n-----END CERTIFICATE-----\n";
        let key_pem = b"-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----\n";

        let tls = TlsConfig::new().with_client_cert_pem_bytes(cert_pem, key_pem);

        assert!(tls.client_cert().is_some());
        assert!(tls.client_key().is_some());
        assert_eq!(tls.client_key().unwrap(), key_pem.as_slice());
    }

    #[test]
    fn test_tls_config_with_domain_name() {
        let tls =
            TlsConfig::new().with_ca_cert_pem_bytes(b"cert").with_domain_name("custom.example.com");

        assert_eq!(tls.domain_name(), Some("custom.example.com"));
    }

    #[test]
    fn test_tls_config_validation_requires_ca_or_native_roots() {
        let tls = TlsConfig::new();
        let result = tls.validate();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("CA certificate") || err.to_string().contains("native roots")
        );
    }

    #[test]
    fn test_tls_config_validation_native_roots_is_sufficient() {
        let tls = TlsConfig::with_native_roots();
        let result = tls.validate();

        assert!(result.is_ok());
    }

    #[test]
    fn test_tls_config_validation_ca_cert_is_sufficient() {
        let tls = TlsConfig::new().with_ca_cert_pem_bytes(b"cert");
        let result = tls.validate();

        assert!(result.is_ok());
    }

    #[test]
    fn test_tls_config_validation_client_cert_requires_key() {
        // Manually construct a TlsConfig with cert but no key
        let tls = TlsConfig {
            ca_cert: Some(CertificateData::Pem(b"ca".to_vec())),
            client_cert: Some(CertificateData::Pem(b"cert".to_vec())),
            client_key: None,
            domain_name: None,
            use_native_roots: false,
        };

        let result = tls.validate();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("private key"));
    }

    #[test]
    fn test_tls_config_validation_client_cert_with_key_ok() {
        let tls = TlsConfig::new()
            .with_ca_cert_pem_bytes(b"ca")
            .with_client_cert_pem_bytes(b"cert", b"key");

        let result = tls.validate();
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
        let tls = TlsConfig::with_native_roots();

        let config = ClientConfig::builder()
            .with_endpoint("https://secure.example.com:443")
            .with_client_id("test-client")
            .with_tls(tls)
            .build();

        assert!(config.is_ok());
        let config = config.unwrap();
        assert!(config.tls().is_some());
        assert!(config.tls().unwrap().use_native_roots());
    }

    #[test]
    fn test_client_config_with_invalid_tls_fails() {
        let tls = TlsConfig::new(); // Invalid: no CA cert or native roots

        let result = ClientConfig::builder()
            .with_endpoint("https://secure.example.com:443")
            .with_client_id("test-client")
            .with_tls(tls)
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_tls_config_chaining() {
        let tls = TlsConfig::new()
            .with_ca_cert_pem_bytes(b"ca-cert")
            .with_client_cert_pem_bytes(b"client-cert", b"client-key")
            .with_domain_name("example.com");

        assert!(tls.ca_cert().is_some());
        assert!(tls.client_cert().is_some());
        assert!(tls.client_key().is_some());
        assert_eq!(tls.domain_name(), Some("example.com"));
    }
}
