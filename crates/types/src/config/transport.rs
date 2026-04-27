//! Transport-layer configuration.
//!
//! Tunables for the gRPC server and SDK client transport stacks. These knobs
//! affect how the underlying HTTP/2 connection is sized — flow-control
//! windows, per-connection stream concurrency limits, etc. Defaults are
//! tuned for high-concurrency RPC workloads where many in-flight streams
//! share a single multiplexed connection.
//!
//! HTTP/2 flow-control windows directly bound per-connection throughput.
//! Tonic 0.14 / hyper 1.x default to 64 KiB stream and connection windows;
//! at 256 concurrent streams sharing one connection, the connection window
//! saturates instantly and the writer side stalls waiting for `WINDOW_UPDATE`
//! frames. The defaults below — 2 MiB per stream, 8 MiB per connection,
//! 2048 max concurrent streams — give multiplexed traffic enough headroom
//! to avoid head-of-line blocking on small RPCs without exposing the server
//! to memory-pressure attacks.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::ConfigError;

/// Default HTTP/2 initial stream window size in bytes (2 MiB).
///
/// Each individual HTTP/2 stream's flow-control window. The default 64 KiB
/// (per RFC 9113) stalls a stream every 64 KiB of unacked data — fine for
/// large request/response sizes but cripples small-RPC throughput when many
/// streams compete for the connection.
const fn default_http2_initial_stream_window_bytes() -> u32 {
    2 * 1024 * 1024
}

/// Default HTTP/2 initial connection window size in bytes (8 MiB).
///
/// The connection-level window must be at least as large as
/// `stream_window * expected_concurrent_streams` to avoid the connection
/// becoming the bottleneck before any single stream does.
const fn default_http2_initial_connection_window_bytes() -> u32 {
    8 * 1024 * 1024
}

/// Default maximum concurrent HTTP/2 streams per connection (2048).
///
/// Caps the number of in-flight streams a single client connection may open.
/// Default 2048 covers high-concurrency client workloads (256 concurrent
/// SDK tasks × multiple in-flight RPCs each) with headroom; production
/// deployments behind aggressive proxies can lower this.
const fn default_http2_max_concurrent_streams() -> u32 {
    2048
}

/// HTTP/2 transport tuning for the gRPC server.
///
/// All fields apply at connection-establishment time on the
/// [`tonic::transport::Server`] builder, so changes require a server
/// restart to take effect. The corresponding client-side knobs live on
/// [`ClientConfig`](crate::config) in the SDK; the two sides should be
/// raised together because each side's window only governs its own
/// receive direction.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::Http2Config;
/// let config = Http2Config::builder()
///     .initial_stream_window_bytes(4 * 1024 * 1024)
///     .initial_connection_window_bytes(16 * 1024 * 1024)
///     .max_concurrent_streams(4096)
///     .build()
///     .expect("valid http/2 config");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Http2Config {
    /// Per-stream initial flow-control window in bytes.
    ///
    /// Default: 2 MiB. Must be >= 65,535 (the RFC 9113 minimum).
    #[serde(default = "default_http2_initial_stream_window_bytes")]
    pub initial_stream_window_bytes: u32,

    /// Per-connection initial flow-control window in bytes.
    ///
    /// Default: 8 MiB. Must be >= `initial_stream_window_bytes` so that at
    /// least one stream can fully utilise its window before the connection
    /// window throttles it.
    #[serde(default = "default_http2_initial_connection_window_bytes")]
    pub initial_connection_window_bytes: u32,

    /// Maximum concurrent HTTP/2 streams per client connection.
    ///
    /// Default: 2048. Must be >= 1.
    #[serde(default = "default_http2_max_concurrent_streams")]
    pub max_concurrent_streams: u32,
}

impl Default for Http2Config {
    fn default() -> Self {
        Self {
            initial_stream_window_bytes: default_http2_initial_stream_window_bytes(),
            initial_connection_window_bytes: default_http2_initial_connection_window_bytes(),
            max_concurrent_streams: default_http2_max_concurrent_streams(),
        }
    }
}

#[bon::bon]
impl Http2Config {
    /// Creates a new HTTP/2 transport configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if:
    /// - `initial_stream_window_bytes` < 65,535
    /// - `initial_connection_window_bytes` < `initial_stream_window_bytes`
    /// - `max_concurrent_streams` is 0
    #[builder]
    pub fn new(
        #[builder(default = default_http2_initial_stream_window_bytes())]
        initial_stream_window_bytes: u32,
        #[builder(default = default_http2_initial_connection_window_bytes())]
        initial_connection_window_bytes: u32,
        #[builder(default = default_http2_max_concurrent_streams())] max_concurrent_streams: u32,
    ) -> Result<Self, ConfigError> {
        let config = Self {
            initial_stream_window_bytes,
            initial_connection_window_bytes,
            max_concurrent_streams,
        };
        config.validate()?;
        Ok(config)
    }
}

impl Http2Config {
    /// Validates the configuration values.
    ///
    /// Call after deserialization to ensure values are within valid ranges.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is out of range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        // RFC 9113 §6.9.2: the initial value of SETTINGS_INITIAL_WINDOW_SIZE
        // is 65,535 octets. Operators may raise — never lower below — this.
        if self.initial_stream_window_bytes < 65_535 {
            return Err(ConfigError::Validation {
                message: format!(
                    "initial_stream_window_bytes must be >= 65535 (RFC 9113), got {}",
                    self.initial_stream_window_bytes
                ),
            });
        }
        if self.initial_connection_window_bytes < self.initial_stream_window_bytes {
            return Err(ConfigError::Validation {
                message: format!(
                    "initial_connection_window_bytes ({}) must be >= initial_stream_window_bytes ({})",
                    self.initial_connection_window_bytes, self.initial_stream_window_bytes
                ),
            });
        }
        if self.max_concurrent_streams == 0 {
            return Err(ConfigError::Validation {
                message: "max_concurrent_streams must be >= 1".to_string(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn default_values_are_production_ready() {
        let config = Http2Config::default();
        assert_eq!(config.initial_stream_window_bytes, 2 * 1024 * 1024);
        assert_eq!(config.initial_connection_window_bytes, 8 * 1024 * 1024);
        assert_eq!(config.max_concurrent_streams, 2048);
    }

    #[test]
    fn defaults_pass_validation() {
        Http2Config::default().validate().unwrap();
    }

    #[test]
    fn builder_with_defaults_validates() {
        let config = Http2Config::builder().build().unwrap();
        assert_eq!(config, Http2Config::default());
    }

    #[test]
    fn builder_accepts_overrides() {
        let config = Http2Config::builder()
            .initial_stream_window_bytes(4 * 1024 * 1024)
            .initial_connection_window_bytes(16 * 1024 * 1024)
            .max_concurrent_streams(4096)
            .build()
            .unwrap();
        assert_eq!(config.initial_stream_window_bytes, 4 * 1024 * 1024);
        assert_eq!(config.initial_connection_window_bytes, 16 * 1024 * 1024);
        assert_eq!(config.max_concurrent_streams, 4096);
    }

    #[test]
    fn stream_window_below_rfc_minimum_fails() {
        let result = Http2Config::builder().initial_stream_window_bytes(65_534).build();
        assert!(result.is_err());
    }

    #[test]
    fn connection_window_smaller_than_stream_window_fails() {
        let result = Http2Config::builder()
            .initial_stream_window_bytes(4 * 1024 * 1024)
            .initial_connection_window_bytes(2 * 1024 * 1024)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn zero_max_concurrent_streams_fails() {
        let result = Http2Config::builder().max_concurrent_streams(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn serde_with_defaults_from_empty() {
        let config: Http2Config = serde_json::from_str("{}").unwrap();
        assert_eq!(config, Http2Config::default());
    }

    #[test]
    fn serde_round_trip() {
        let config = Http2Config::builder()
            .initial_stream_window_bytes(3 * 1024 * 1024)
            .initial_connection_window_bytes(12 * 1024 * 1024)
            .max_concurrent_streams(1024)
            .build()
            .unwrap();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: Http2Config = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }
}
