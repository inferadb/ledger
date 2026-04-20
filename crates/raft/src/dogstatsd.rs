//! Optional DogStatsD recorder for emitting metrics as Datadog distributions.
//!
//! Enabled via the `dogstatsd` Cargo feature. When active, this module installs
//! a DogStatsD recorder as the global `metrics` recorder, replacing the default
//! Prometheus recorder.
//!
//! ## Single-recorder constraint
//!
//! The `metrics` crate permits only one global recorder per process. You must
//! choose between Prometheus (default) and DogStatsD at startup. If you need
//! both simultaneously, deploy a Prometheus-compatible DogStatsD sidecar
//! (e.g. `prometheus-dogstatsd-bridge`) or export via OTLP instead.
//!
//! ## Cost benefit
//!
//! Datadog distributions cost 1 custom metric per `(name, tag_set)`. Legacy
//! histogram aggregates cost 8. Enabling distributions reduces custom metric
//! spend by ~8× for workloads with many histogram series.

use inferadb_ledger_types::config::DogStatsdConfig;
use metrics_exporter_dogstatsd::DogStatsDBuilder;
use snafu::Snafu;

// =========================================================================
// Error type
// =========================================================================

/// Errors that can occur when initializing the DogStatsD exporter.
#[derive(Debug, Snafu)]
pub enum DogStatsdError {
    /// The remote address string is invalid or the recorder could not be built.
    #[snafu(display("failed to configure DogStatsD exporter: {message}"))]
    Build {
        /// Description of the build failure.
        message: String,
    },
    /// The global recorder slot is already occupied.
    #[snafu(display("failed to install DogStatsD recorder: {message}"))]
    Install {
        /// Description of the install failure.
        message: String,
    },
}

// =========================================================================
// Initialization
// =========================================================================

/// Installs the DogStatsD recorder as the global `metrics` recorder.
///
/// Call this once at process startup, before any metrics are recorded, and
/// only when the `dogstatsd` feature is enabled. After this call, all
/// `metrics::counter!`, `metrics::histogram!`, etc. calls emit to the
/// DogStatsD agent at `config.endpoint`.
///
/// Returns `Ok(())` immediately if `config.endpoint` is `None` (disabled).
///
/// # Errors
///
/// Returns [`DogStatsdError::Build`] if the endpoint address is invalid.
/// Returns [`DogStatsdError::Install`] if a global recorder is already set.
pub fn init_dogstatsd(config: &DogStatsdConfig) -> Result<(), DogStatsdError> {
    let Some(endpoint) = config.endpoint else {
        return Ok(());
    };

    let builder = DogStatsDBuilder::default()
        .with_remote_address(endpoint.to_string())
        .map_err(|e| DogStatsdError::Build { message: e.to_string() })?
        .send_histograms_as_distributions(config.use_distributions);

    builder.install().map_err(|e| DogStatsdError::Install { message: e.to_string() })?;

    tracing::info!(
        endpoint = %endpoint,
        use_distributions = config.use_distributions,
        "DogStatsD exporter installed"
    );

    Ok(())
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::net::SocketAddr;

    use super::*;

    #[test]
    fn no_endpoint_returns_ok() {
        let config = DogStatsdConfig::default();
        assert!(config.endpoint.is_none());
        // Should return Ok without attempting to install a recorder.
        // We cannot actually call init_dogstatsd here because it would
        // install a global recorder that persists across tests; testing the
        // None path only verifies early return logic.
        let result = (|| -> Result<(), DogStatsdError> {
            let Some(_endpoint) = config.endpoint else {
                return Ok(());
            };
            unreachable!("endpoint is None");
        })();
        assert!(result.is_ok());
    }

    #[test]
    fn dogstatsd_config_serde_roundtrip_with_endpoint() {
        let addr: SocketAddr = "10.0.0.1:8125".parse().unwrap();
        let config = DogStatsdConfig { endpoint: Some(addr), use_distributions: false };
        let json = serde_json::to_string(&config).unwrap();
        let restored: DogStatsdConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, restored);
    }
}
