//! Client configuration with builder pattern.
//!
//! Provides type-safe configuration for SDK clients including:
//! - Endpoint URLs
//! - Timeouts and connection settings
//! - Retry policies
//! - Compression options

use std::time::Duration;

use crate::error::{ConfigSnafu, InvalidUrlSnafu, Result};
use snafu::ensure;

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
            ConfigSnafu {
                message: "at least one endpoint is required"
            }
        );

        for endpoint in &self.endpoints {
            validate_url(endpoint)?;
        }

        let client_id = self.client_id.ok_or_else(|| {
            ConfigSnafu {
                message: "client_id is required",
            }
            .build()
        })?;

        ensure!(
            !client_id.is_empty(),
            ConfigSnafu {
                message: "client_id cannot be empty"
            }
        );

        let timeout = self.timeout.unwrap_or(DEFAULT_TIMEOUT);
        ensure!(
            !timeout.is_zero(),
            ConfigSnafu {
                message: "timeout cannot be zero"
            }
        );

        let connect_timeout = self.connect_timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT);
        ensure!(
            !connect_timeout.is_zero(),
            ConfigSnafu {
                message: "connect_timeout cannot be zero"
            }
        );

        Ok(ClientConfig {
            endpoints: self.endpoints,
            client_id,
            timeout,
            connect_timeout,
            retry_policy: self.retry_policy.unwrap_or_default(),
            compression: self.compression,
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
        Self {
            max_attempts: 1,
            ..Default::default()
        }
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
        return InvalidUrlSnafu {
            url,
            message: "URL must start with http:// or https://",
        }
        .fail();
    }

    // Check there's something after the scheme
    let rest = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .unwrap_or("");

    if rest.is_empty() {
        return InvalidUrlSnafu {
            url,
            message: "URL must have a host",
        }
        .fail();
    }

    // Check for invalid characters
    if rest.contains(char::is_whitespace) {
        return InvalidUrlSnafu {
            url,
            message: "URL cannot contain whitespace",
        }
        .fail();
    }

    Ok(())
}

#[cfg(test)]
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
        let result = ClientConfig::builder()
            .with_endpoint("http://localhost:50051")
            .build();

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
        let result = ClientConfig::builder()
            .with_endpoint("http://")
            .with_client_id("test-client")
            .build();

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
}
