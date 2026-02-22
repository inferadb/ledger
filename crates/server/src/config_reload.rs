//! SIGHUP-driven runtime configuration reload.
//!
//! Watches for `SIGHUP` signals and re-reads the runtime config file,
//! applying reconfigurable parameters (rate limits, hot key thresholds,
//! compaction intervals, validation limits) without server restart.

use std::{path::PathBuf, sync::Arc};

use inferadb_ledger_raft::{HotKeyDetector, RateLimiter, RuntimeConfigHandle};
use inferadb_ledger_types::config::RuntimeConfig;
use tracing::{error, info, warn};

/// Spawns a background task that reloads runtime config on SIGHUP.
///
/// The task runs until the process exits. On each SIGHUP:
/// 1. Re-reads and parses the TOML config file
/// 2. Validates the new config
/// 3. Atomically swaps it into the `RuntimeConfigHandle`
///
/// If parsing or validation fails, the current config remains unchanged.
#[cfg(unix)]
pub fn spawn_sighup_handler(
    config_path: PathBuf,
    handle: RuntimeConfigHandle,
    rate_limiter: Option<Arc<RateLimiter>>,
    hot_key_detector: Option<Arc<HotKeyDetector>>,
) {
    tokio::spawn(async move {
        let mut signal = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        {
            Ok(s) => s,
            Err(e) => {
                error!(error = %e, "Failed to install SIGHUP handler for config reload");
                return;
            },
        };

        info!(config_path = %config_path.display(), "SIGHUP config reload handler started");

        loop {
            signal.recv().await;
            info!("Received SIGHUP, reloading runtime config");

            match reload_config(
                &config_path,
                &handle,
                rate_limiter.as_ref(),
                hot_key_detector.as_ref(),
            ) {
                Ok(changed) => {
                    if changed.is_empty() {
                        info!("SIGHUP reload: no configuration changes detected");
                    } else {
                        info!(changed_fields = ?changed, "SIGHUP reload: configuration updated");
                    }
                },
                Err(e) => {
                    warn!(error = %e, "SIGHUP reload failed, keeping current config");
                },
            }
        }
    });
}

/// Reads and applies a runtime config file.
///
/// Returns the list of changed field paths, or an error if the file
/// cannot be read, parsed, or validated.
///
/// # Errors
///
/// Returns an error string if the config file cannot be read, contains
/// invalid TOML, or fails validation.
fn reload_config(
    config_path: &std::path::Path,
    handle: &RuntimeConfigHandle,
    rate_limiter: Option<&Arc<RateLimiter>>,
    hot_key_detector: Option<&Arc<HotKeyDetector>>,
) -> Result<Vec<String>, String> {
    let contents = std::fs::read_to_string(config_path)
        .map_err(|e| format!("Failed to read config file: {e}"))?;

    let new_config: RuntimeConfig =
        toml::from_str(&contents).map_err(|e| format!("Failed to parse config file: {e}"))?;

    handle
        .update(new_config, rate_limiter, hot_key_detector)
        .map_err(|e| format!("Config validation failed: {e}"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::io::Write;

    use inferadb_ledger_types::config::RateLimitConfig;
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn test_reload_config_applies_changes() {
        let handle = RuntimeConfigHandle::new(RuntimeConfig::default());

        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
[rate_limit]
client_burst = 500
client_rate = 50.0
organization_burst = 2000
organization_rate = 200.0
backpressure_threshold = 800
"#
        )
        .unwrap();

        let changed = reload_config(file.path(), &handle, None, None).unwrap();
        assert!(changed.contains(&"rate_limit".to_string()));

        let loaded = handle.load();
        assert!(loaded.rate_limit.is_some());
        let rl = loaded.rate_limit.as_ref().unwrap();
        assert_eq!(rl.client_burst, 500);
    }

    #[test]
    fn test_reload_config_rejects_invalid() {
        let handle = RuntimeConfigHandle::new(RuntimeConfig::default());

        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
[rate_limit]
client_burst = 0
client_rate = 50.0
organization_burst = 2000
organization_rate = 200.0
backpressure_threshold = 800
"#
        )
        .unwrap();

        let result = reload_config(file.path(), &handle, None, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("validation"));
    }

    #[test]
    fn test_reload_config_handles_missing_file() {
        let handle = RuntimeConfigHandle::new(RuntimeConfig::default());
        let result =
            reload_config(std::path::Path::new("/nonexistent/config.toml"), &handle, None, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("read config file"));
    }

    #[test]
    fn test_reload_config_handles_invalid_toml() {
        let handle = RuntimeConfigHandle::new(RuntimeConfig::default());

        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "this is {{ not valid toml").unwrap();

        let result = reload_config(file.path(), &handle, None, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("parse config file"));
    }

    #[test]
    fn test_reload_config_empty_file_is_no_change() {
        let handle = RuntimeConfigHandle::new(RuntimeConfig::default());

        let mut file = NamedTempFile::new().unwrap();
        writeln!(file).unwrap();

        let changed = reload_config(file.path(), &handle, None, None).unwrap();
        assert!(changed.is_empty());
    }

    #[test]
    fn test_reload_config_propagates_to_rate_limiter() {
        let limiter = Arc::new(RateLimiter::new(100, 10.0, 500, 50.0, 200));
        let handle = RuntimeConfigHandle::new(RuntimeConfig::default());

        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
[rate_limit]
client_burst = 999
client_rate = 99.0
organization_burst = 4999
organization_rate = 499.0
backpressure_threshold = 1500
"#
        )
        .unwrap();

        let changed = reload_config(file.path(), &handle, Some(&limiter), None).unwrap();
        assert!(changed.contains(&"rate_limit".to_string()));
    }

    #[test]
    fn test_reload_preserves_existing_on_error() {
        let initial = RuntimeConfig::builder().rate_limit(RateLimitConfig::default()).build();
        let handle = RuntimeConfigHandle::new(initial);

        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
[rate_limit]
client_burst = 0
client_rate = 50.0
organization_burst = 2000
organization_rate = 200.0
backpressure_threshold = 800
"#
        )
        .unwrap();

        let result = reload_config(file.path(), &handle, None, None);
        assert!(result.is_err());

        // Original config preserved
        let loaded = handle.load();
        assert!(loaded.rate_limit.is_some());
    }
}
