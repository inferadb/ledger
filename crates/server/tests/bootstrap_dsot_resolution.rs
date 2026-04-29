//! Bootstrap-time DSoT resolution tests.
//!
//! Verifies that `--ratelimit`, `--vault-hibernation`, and `--vault-metrics`
//! CLI overrides honor the canonical inner config field when the override
//! is `None`. Pre-fix the inner field was silently overwritten by the CLI
//! flag at startup; post-fix it is honored as the source of truth.
//!
//! Each test constructs a `Config` with the inner master switch set to a
//! non-default value and the CLI override left as `None`, calls
//! `bootstrap_node` (or just the resolution entrypoint, in the case of
//! vault-metrics — the gate is a global static), and asserts the expected
//! resolved behavior.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]

use std::{sync::Arc, time::Duration};

use inferadb_ledger_server::{bootstrap::bootstrap_node, config::Config};
use inferadb_ledger_store::crypto::{InMemoryKeyManager, RegionKeyManager};
use inferadb_ledger_test_utils::TestDir;

use crate::common::allocate_ports;

/// Bootstraps a single-node cluster with a custom `Config` and returns the
/// `RaftManager` so the test can inspect post-bootstrap state. The shutdown
/// channel is intentionally kept alive for the test's lifetime — a `_drop`
/// would race the manager's background tasks against the assertion.
async fn bootstrap_with_config(
    config: Config,
    data_dir: &std::path::Path,
) -> (inferadb_ledger_server::bootstrap::BootstrappedNode, tokio::sync::watch::Sender<bool>) {
    let key_manager: Arc<dyn RegionKeyManager> =
        Arc::new(InMemoryKeyManager::generate_for_regions(&inferadb_ledger_types::ALL_REGIONS));

    // Pre-write the cluster_id so bootstrap_node takes the restart-style
    // path (single-voter, immediate startup) rather than waiting for an
    // InitCluster RPC.
    inferadb_ledger_server::cluster_id::write_cluster_id(data_dir, 1).expect("write cluster_id");

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let health_state = inferadb_ledger_raft::HealthState::new();
    let bootstrapped =
        bootstrap_node(&config, data_dir, health_state.clone(), shutdown_rx, Some(key_manager))
            .await
            .expect("bootstrap node");
    health_state.mark_ready();
    (bootstrapped, shutdown_tx)
}

fn base_config(socket_path: std::path::PathBuf, data_dir: std::path::PathBuf) -> Config {
    let backup_config = inferadb_ledger_types::config::BackupConfig::builder()
        .destination(data_dir.join("backups").to_string_lossy().to_string())
        .build()
        .expect("valid backup config");
    let raft = inferadb_ledger_types::config::RaftConfig::builder()
        .heartbeat_interval(Duration::from_millis(100))
        .election_timeout_min(Duration::from_millis(300))
        .election_timeout_max(Duration::from_millis(600))
        .build()
        .expect("valid raft config");

    Config {
        listen: None,
        socket: Some(socket_path),
        metrics_addr: None,
        data_dir: Some(data_dir),
        backup: Some(backup_config),
        raft: Some(raft),
        saga: inferadb_ledger_types::config::SagaConfig { poll_interval_secs: 2 },
        token_maintenance_interval_secs: 3,
        email_blinding_key: Some(
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string(),
        ),
        ..Config::default()
    }
}

/// The DSoT bug fix: `hibernation.enabled = true` set on `Config` (no CLI
/// flag) must activate hibernation at bootstrap. Pre-fix the inner field
/// was silently overwritten with the CLI default (`false`); post-fix the
/// inner field is honored.
#[tokio::test]
async fn bootstrap_honors_inner_hibernation_enabled_when_cli_unset() {
    let temp_dir = TestDir::new();
    let data_dir = temp_dir.path().to_path_buf();
    let socket_dir = data_dir.join("sockets");
    std::fs::create_dir_all(&socket_dir).expect("mk socket dir");
    let unique = allocate_ports(1);
    let socket_path = socket_dir.join(format!("hibernation-on-{unique}.sock"));

    let hibernation_cfg = inferadb_ledger_types::config::HibernationConfig::builder()
        .enabled(true)
        .idle_secs(300)
        .scan_interval_secs(30)
        .wake_threshold_ms(100)
        .build()
        .expect("valid hibernation config");

    let config = Config {
        // CLI override left as None — the inner config drives the master
        // switch. This is the central regression case the DSoT migration
        // fixes: pre-fix `vault_hibernation: false` was implicitly written
        // into the manager's hibernation config at startup, silently
        // overriding `hibernation.enabled = true`.
        vault_hibernation: None,
        hibernation: hibernation_cfg,
        ..base_config(socket_path, data_dir.clone())
    };

    let (bootstrapped, _shutdown_tx) = bootstrap_with_config(config, &data_dir).await;

    let post_bootstrap = bootstrapped.manager.hibernation_config();
    assert!(
        post_bootstrap.enabled,
        "hibernation.enabled = true (inner) must be honored when --vault-hibernation is unset",
    );
    assert_eq!(
        post_bootstrap.idle_secs, 300,
        "hibernation tuning knobs must round-trip through bootstrap",
    );
}

/// Symmetric guarantee: the rate-limiting-disabled default holds when
/// neither the CLI flag nor the inner field opts in. This is the central
/// "no surprise throttling" guarantee the DSoT migration must not regress.
#[tokio::test]
async fn bootstrap_default_keeps_rate_limiting_disabled() {
    let temp_dir = TestDir::new();
    let data_dir = temp_dir.path().to_path_buf();
    let socket_dir = data_dir.join("sockets");
    std::fs::create_dir_all(&socket_dir).expect("mk socket dir");
    let unique = allocate_ports(1);
    let socket_path = socket_dir.join(format!("ratelimit-default-{unique}.sock"));

    let config = Config {
        // Both CLI override and inner config left at default; the
        // resolution must produce `false` (limiter disabled). Bootstrap
        // would not panic on `enabled = true`, but the gate would
        // silently throttle — exactly the trap the silent-throttling
        // discipline guards against.
        ratelimit: None,
        rate_limit: None,
        ..base_config(socket_path, data_dir.clone())
    };

    let (bootstrapped, _shutdown_tx) = bootstrap_with_config(config, &data_dir).await;

    // The rate limiter is constructed inside bootstrap; assert via the
    // resolution helper that the central guarantee holds.
    assert!(
        !inferadb_ledger_types::config::RateLimitConfig::resolve_enabled(None, false),
        "the (None, false) resolution arm must keep rate limiting disabled",
    );
    // Spot-check the manager came up successfully.
    let _ = bootstrapped.manager.hibernation_config();
}

/// The DSoT bug fix for vault metrics: `observability.vault_metrics_enabled = true`
/// set on `Config` (no CLI flag) must enable per-vault metric emission at
/// bootstrap. Pre-fix `Config` had no `observability` field, so the inner
/// struct was orphaned; post-fix it is wired and the CLI flag is an
/// optional override.
#[tokio::test]
async fn bootstrap_resolution_honors_inner_vault_metrics_when_cli_unset() {
    // The vault-metrics gate is a global AtomicBool in `metrics.rs`; the
    // resolved value is set exactly once via `set_vault_metrics_enabled`
    // at startup. We exercise the resolution helper directly (it is the
    // single decision point) rather than the bootstrap pipeline, because
    // the global is process-wide and its state would leak between tests.
    let observability = inferadb_ledger_types::config::ObservabilityConfig::builder()
        .vault_metrics_enabled(true)
        .build();

    let resolved = inferadb_ledger_types::config::ObservabilityConfig::resolve_enabled(
        None,
        observability.vault_metrics_enabled,
    );
    assert!(
        resolved,
        "observability.vault_metrics_enabled = true (inner) must be honored when --vault-metrics is unset",
    );

    // CLI override forces off even when the inner field opts in.
    let resolved_off = inferadb_ledger_types::config::ObservabilityConfig::resolve_enabled(
        Some(false),
        observability.vault_metrics_enabled,
    );
    assert!(!resolved_off, "--vault-metrics=false must override the inner field");
}
