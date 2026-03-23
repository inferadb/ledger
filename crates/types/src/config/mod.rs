//! Configuration types for InferaDB Ledger.
//!
//! Configuration is loaded from CLI arguments and environment variables.
//! All config structs validate their values at construction time via
//! fallible builders. Post-deserialization validation is available via
//! the `validate()` method on each struct.

// The schemars `JsonSchema` derive macro internally uses `.unwrap()` in its
// `json_schema!` and `json_internal!` expansions. Allow `disallowed_methods`
// at the module level since config types are declarative structs with minimal
// procedural code.
#![allow(clippy::disallowed_methods)]

/// Encryption-at-rest configuration.
mod encryption;
/// JWT signing, validation, and token lifetime configuration.
mod jwt;
/// Key management and rotation configuration.
mod key_management;
/// Node identity and peer configuration.
mod node;
/// Hot key detection, metrics cardinality, and OpenTelemetry configuration.
mod observability;
/// Raft consensus and log storage configuration.
mod raft;
/// Rate limiting, shutdown, health check, validation, saga, and cleanup configuration.
mod resilience;
/// Runtime reconfiguration types for hot-reloadable settings.
mod runtime;
/// Storage backend and cache configuration.
mod storage;

pub use encryption::*;
pub use jwt::*;
pub use key_management::*;
pub use node::*;
pub use observability::*;
pub use raft::*;
pub use resilience::*;
pub use runtime::*;
use snafu::Snafu;
pub use storage::*;

/// Configuration validation error.
///
/// Returned when a configuration value is outside its valid range or
/// violates a cross-field constraint.
#[derive(Debug, Snafu)]
pub enum ConfigError {
    /// A configuration value is invalid.
    #[snafu(display("invalid config: {message}"))]
    Validation {
        /// Description of the validation failure.
        message: String,
    },
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::{net::SocketAddr, path::PathBuf, time::Duration};

    use super::*;

    // =========================================================================
    // StorageConfig validation tests
    // =========================================================================

    #[test]
    fn test_storage_config_defaults_are_valid() {
        let config = StorageConfig::builder().build().expect("defaults should be valid");
        assert_eq!(config.cache_size_bytes, 256 * 1024 * 1024);
        assert_eq!(config.hot_cache_snapshots, 3);
        assert_eq!(config.snapshot_interval, Duration::from_secs(300));
        assert_eq!(config.compression_level, 3);
    }

    #[test]
    fn test_storage_config_builder_with_custom_values() {
        let config = StorageConfig::builder()
            .cache_size_bytes(2 * 1024 * 1024)
            .hot_cache_snapshots(5)
            .snapshot_interval(Duration::from_secs(600))
            .compression_level(10)
            .build()
            .expect("valid custom config");
        assert_eq!(config.cache_size_bytes, 2 * 1024 * 1024);
        assert_eq!(config.hot_cache_snapshots, 5);
        assert_eq!(config.snapshot_interval, Duration::from_secs(600));
        assert_eq!(config.compression_level, 10);
    }

    #[test]
    fn test_storage_config_cache_size_minimum() {
        // Exactly 1 MB is valid
        let result = StorageConfig::builder().cache_size_bytes(1024 * 1024).build();
        assert!(result.is_ok());

        // Below 1 MB is invalid
        let result = StorageConfig::builder().cache_size_bytes(1024 * 1024 - 1).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("cache_size_bytes"));
        assert!(err.to_string().contains("1 MB"));
    }

    #[test]
    fn test_storage_config_compression_level_valid_range() {
        // Min boundary
        let result = StorageConfig::builder().compression_level(1).build();
        assert!(result.is_ok());

        // Max boundary
        let result = StorageConfig::builder().compression_level(22).build();
        assert!(result.is_ok());

        // Mid-range
        let result = StorageConfig::builder().compression_level(10).build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_storage_config_compression_level_too_low() {
        let result = StorageConfig::builder().compression_level(0).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("compression_level"));
        assert!(err.to_string().contains("1-22"));
    }

    #[test]
    fn test_storage_config_compression_level_too_high() {
        let result = StorageConfig::builder().compression_level(23).build();
        assert!(result.is_err());

        let result = StorageConfig::builder().compression_level(100).build();
        assert!(result.is_err());
    }

    // =========================================================================
    // RaftConfig validation tests
    // =========================================================================

    #[test]
    fn test_raft_config_defaults_are_valid() {
        let config = RaftConfig::builder().build().expect("defaults should be valid");
        assert_eq!(config.heartbeat_interval, Duration::from_millis(100));
        assert_eq!(config.election_timeout_min, Duration::from_millis(300));
        assert_eq!(config.election_timeout_max, Duration::from_millis(500));
        assert_eq!(config.max_entries_per_rpc, 100);
        assert_eq!(config.snapshot_threshold, 10_000);
    }

    #[test]
    fn test_raft_config_builder_with_custom_values() {
        let config = RaftConfig::builder()
            .heartbeat_interval(Duration::from_millis(200))
            .election_timeout_min(Duration::from_millis(500))
            .election_timeout_max(Duration::from_millis(1000))
            .max_entries_per_rpc(50)
            .snapshot_threshold(5000)
            .build()
            .expect("valid custom config");
        assert_eq!(config.heartbeat_interval, Duration::from_millis(200));
        assert_eq!(config.election_timeout_min, Duration::from_millis(500));
        assert_eq!(config.election_timeout_max, Duration::from_millis(1000));
        assert_eq!(config.max_entries_per_rpc, 50);
        assert_eq!(config.snapshot_threshold, 5000);
    }

    #[test]
    fn test_raft_config_election_timeout_min_must_be_less_than_max() {
        // Equal is invalid
        let result = RaftConfig::builder()
            .election_timeout_min(Duration::from_millis(500))
            .election_timeout_max(Duration::from_millis(500))
            .build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("election_timeout_min"));
        assert!(err.to_string().contains("less than"));

        // Min > max is invalid
        let result = RaftConfig::builder()
            .election_timeout_min(Duration::from_millis(600))
            .election_timeout_max(Duration::from_millis(500))
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_raft_config_heartbeat_must_be_less_than_half_election_min() {
        // heartbeat == election_min / 2 is invalid (must be strictly less)
        let result = RaftConfig::builder()
            .heartbeat_interval(Duration::from_millis(150))
            .election_timeout_min(Duration::from_millis(300))
            .election_timeout_max(Duration::from_millis(500))
            .build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("heartbeat_interval"));
        assert!(err.to_string().contains("election_timeout_min / 2"));

        // heartbeat > election_min / 2 is invalid
        let result = RaftConfig::builder()
            .heartbeat_interval(Duration::from_millis(200))
            .election_timeout_min(Duration::from_millis(300))
            .election_timeout_max(Duration::from_millis(500))
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_raft_config_heartbeat_just_under_half_election_min() {
        // heartbeat < election_min / 2 is valid
        let result = RaftConfig::builder()
            .heartbeat_interval(Duration::from_millis(149))
            .election_timeout_min(Duration::from_millis(300))
            .election_timeout_max(Duration::from_millis(500))
            .build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_raft_config_max_entries_per_rpc_zero() {
        let result = RaftConfig::builder().max_entries_per_rpc(0).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("max_entries_per_rpc"));
    }

    #[test]
    fn test_raft_config_snapshot_threshold_zero() {
        let result = RaftConfig::builder().snapshot_threshold(0).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("snapshot_threshold"));
    }

    #[test]
    fn test_raft_config_max_entries_per_rpc_one() {
        let result = RaftConfig::builder().max_entries_per_rpc(1).build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_raft_config_snapshot_threshold_one() {
        let result = RaftConfig::builder().snapshot_threshold(1).build();
        assert!(result.is_ok());
    }

    // =========================================================================
    // BatchConfig validation tests
    // =========================================================================

    #[test]
    fn test_batch_config_defaults_are_valid() {
        let config = BatchConfig::builder().build().expect("defaults should be valid");
        assert_eq!(config.max_batch_size, 100);
        assert_eq!(config.batch_timeout, Duration::from_millis(5));
        assert!(config.coalesce_enabled);
    }

    #[test]
    fn test_batch_config_builder_with_custom_values() {
        let config = BatchConfig::builder()
            .max_batch_size(50)
            .batch_timeout(Duration::from_millis(10))
            .coalesce_enabled(false)
            .build()
            .expect("valid custom config");
        assert_eq!(config.max_batch_size, 50);
        assert_eq!(config.batch_timeout, Duration::from_millis(10));
        assert!(!config.coalesce_enabled);
    }

    #[test]
    fn test_batch_config_max_batch_size_zero() {
        let result = BatchConfig::builder().max_batch_size(0).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("max_batch_size"));
    }

    #[test]
    fn test_batch_config_max_batch_size_one() {
        let result = BatchConfig::builder().max_batch_size(1).build();
        assert!(result.is_ok());
    }

    // =========================================================================
    // Default impl tests
    // =========================================================================

    // =========================================================================
    // Validate method tests (for post-deserialization)
    // =========================================================================

    #[test]
    fn test_storage_config_validate_method() {
        let mut config = StorageConfig::default();
        assert!(config.validate().is_ok());

        config.compression_level = 100;
        assert!(config.validate().is_err());

        config.compression_level = 3;
        config.cache_size_bytes = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_raft_config_validate_method() {
        let mut config = RaftConfig::default();
        assert!(config.validate().is_ok());

        config.election_timeout_min = Duration::from_millis(600);
        config.election_timeout_max = Duration::from_millis(500);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_batch_config_validate_method() {
        let mut config = BatchConfig::default();
        assert!(config.validate().is_ok());

        config.max_batch_size = 0;
        assert!(config.validate().is_err());
    }

    // =========================================================================
    // PostErasureCompactionConfig validation tests
    // =========================================================================

    #[test]
    fn test_post_erasure_compaction_config_defaults_are_valid() {
        let config =
            PostErasureCompactionConfig::builder().build().expect("defaults should be valid");
        assert_eq!(config.max_log_retention_secs, 3600);
        assert_eq!(config.check_interval_secs, 300);
    }

    #[test]
    fn test_post_erasure_compaction_config_custom_values() {
        let config = PostErasureCompactionConfig::builder()
            .max_log_retention_secs(1800)
            .check_interval_secs(120)
            .build()
            .expect("valid config");
        assert_eq!(config.max_log_retention_secs, 1800);
        assert_eq!(config.check_interval_secs, 120);
    }

    #[test]
    fn test_post_erasure_compaction_config_retention_too_small() {
        let result = PostErasureCompactionConfig::builder().max_log_retention_secs(299).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_post_erasure_compaction_config_interval_too_small() {
        let result = PostErasureCompactionConfig::builder().check_interval_secs(59).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_post_erasure_compaction_config_validate_method() {
        let mut config = PostErasureCompactionConfig::default();
        assert!(config.validate().is_ok());

        config.max_log_retention_secs = 100;
        assert!(config.validate().is_err());
    }

    // =========================================================================
    // Node config and integration tests
    // =========================================================================

    #[test]
    fn test_peer_config_builder() {
        let peer = PeerConfig::builder().node_id("node-2").addr("127.0.0.1:50052").build();
        assert_eq!(peer.node_id, "node-2");
        assert_eq!(peer.addr, "127.0.0.1:50052");
    }

    #[test]
    fn test_node_config_builder_nested() {
        let addr: SocketAddr = "127.0.0.1:50051".parse().unwrap();
        let config = NodeConfig::builder()
            .node_id("node-1")
            .listen_addr(addr)
            .data_dir("/tmp/ledger")
            .peers(vec![PeerConfig::builder().node_id("node-2").addr("127.0.0.1:50052").build()])
            .storage(StorageConfig::builder().cache_size_bytes(1024 * 1024).build().expect("valid"))
            .raft(
                RaftConfig::builder()
                    .heartbeat_interval(Duration::from_millis(200))
                    .election_timeout_min(Duration::from_millis(500))
                    .election_timeout_max(Duration::from_millis(1000))
                    .build()
                    .expect("valid"),
            )
            .batching(BatchConfig::builder().max_batch_size(50).build().expect("valid"))
            .build();

        assert_eq!(config.node_id, "node-1");
        assert_eq!(config.listen_addr, addr);
        assert_eq!(config.data_dir, PathBuf::from("/tmp/ledger"));
        assert_eq!(config.peers.len(), 1);
        assert_eq!(config.peers[0].node_id, "node-2");
        assert_eq!(config.storage.cache_size_bytes, 1024 * 1024);
        assert_eq!(config.raft.heartbeat_interval, Duration::from_millis(200));
        assert_eq!(config.batching.max_batch_size, 50);
    }

    #[test]
    fn test_node_config_builder_with_defaults() {
        let addr: SocketAddr = "127.0.0.1:50051".parse().unwrap();
        let config = NodeConfig::builder()
            .node_id("node-1")
            .listen_addr(addr)
            .data_dir("/tmp/ledger")
            .build();

        assert_eq!(config.node_id, "node-1");
        assert!(config.peers.is_empty());
        assert_eq!(config.storage, StorageConfig::default());
        assert_eq!(config.raft, RaftConfig::default());
        assert_eq!(config.batching, BatchConfig::default());
    }

    #[test]
    fn test_node_config_serde_roundtrip() {
        let addr: SocketAddr = "127.0.0.1:50051".parse().unwrap();
        let config = NodeConfig::builder()
            .node_id("node-1")
            .listen_addr(addr)
            .data_dir("/tmp/ledger")
            .peers(vec![PeerConfig::builder().node_id("node-2").addr("127.0.0.1:50052").build()])
            .storage(
                StorageConfig::builder()
                    .cache_size_bytes(512 * 1024 * 1024)
                    .hot_cache_snapshots(5)
                    .build()
                    .expect("valid"),
            )
            .raft(
                RaftConfig::builder()
                    .heartbeat_interval(Duration::from_millis(150))
                    .election_timeout_min(Duration::from_millis(400))
                    .election_timeout_max(Duration::from_millis(600))
                    .max_entries_per_rpc(200)
                    .build()
                    .expect("valid"),
            )
            .batching(
                BatchConfig::builder()
                    .max_batch_size(200)
                    .coalesce_enabled(false)
                    .build()
                    .expect("valid"),
            )
            .build();

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: NodeConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(config.node_id, deserialized.node_id);
        assert_eq!(config.listen_addr, deserialized.listen_addr);
        assert_eq!(config.data_dir, deserialized.data_dir);
        assert_eq!(config.peers.len(), deserialized.peers.len());
        assert_eq!(config.peers[0].node_id, deserialized.peers[0].node_id);
        assert_eq!(config.storage, deserialized.storage);
        assert_eq!(config.raft, deserialized.raft);
        assert_eq!(config.batching, deserialized.batching);
    }

    // =========================================================================
    // ConfigError display tests
    // =========================================================================

    // =========================================================================
    // RateLimitConfig validation tests
    // =========================================================================

    #[test]
    fn test_rate_limit_config_defaults_are_valid() {
        let config = RateLimitConfig::builder().build().expect("defaults should be valid");
        assert_eq!(config.client_burst, 100);
        assert_eq!(config.client_rate, 50.0);
        assert_eq!(config.organization_burst, 1000);
        assert_eq!(config.organization_rate, 500.0);
        assert_eq!(config.backpressure_threshold, 100);
    }

    #[test]
    fn test_rate_limit_config_builder_with_custom_values() {
        let config = RateLimitConfig::builder()
            .client_burst(200)
            .client_rate(100.0)
            .organization_burst(5000)
            .organization_rate(2000.0)
            .backpressure_threshold(200)
            .build()
            .expect("valid custom config");
        assert_eq!(config.client_burst, 200);
        assert_eq!(config.client_rate, 100.0);
        assert_eq!(config.organization_burst, 5000);
        assert_eq!(config.organization_rate, 2000.0);
        assert_eq!(config.backpressure_threshold, 200);
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn test_rate_limit_config_rejects_invalid_fields() {
        let cases: Vec<(&str, Box<dyn Fn() -> Result<RateLimitConfig, ConfigError>>)> = vec![
            ("client_burst", Box::new(|| RateLimitConfig::builder().client_burst(0).build())),
            ("client_rate", Box::new(|| RateLimitConfig::builder().client_rate(0.0).build())),
            (
                "client_rate (negative)",
                Box::new(|| RateLimitConfig::builder().client_rate(-1.0).build()),
            ),
            (
                "organization_burst",
                Box::new(|| RateLimitConfig::builder().organization_burst(0).build()),
            ),
            (
                "organization_rate",
                Box::new(|| RateLimitConfig::builder().organization_rate(0.0).build()),
            ),
            (
                "backpressure_threshold",
                Box::new(|| RateLimitConfig::builder().backpressure_threshold(0).build()),
            ),
        ];
        for (label, build_fn) in &cases {
            let result = build_fn();
            assert!(result.is_err(), "{label} should be rejected");
        }
    }

    #[test]
    fn test_rate_limit_config_boundary_values() {
        // Minimum valid values
        let result = RateLimitConfig::builder()
            .client_burst(1)
            .client_rate(0.001)
            .organization_burst(1)
            .organization_rate(0.001)
            .backpressure_threshold(1)
            .build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_rate_limit_config_validate_method() {
        let mut config = RateLimitConfig::default();
        assert!(config.validate().is_ok());

        config.client_burst = 0;
        assert!(config.validate().is_err());

        config.client_burst = 100;
        config.organization_rate = 0.0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_rate_limit_config_serde_roundtrip() {
        let config = RateLimitConfig::builder()
            .client_burst(200)
            .client_rate(100.0)
            .organization_burst(3000)
            .organization_rate(1500.0)
            .backpressure_threshold(150)
            .build()
            .expect("valid");

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: RateLimitConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    // =========================================================================
    // BTreeCompactionConfig tests
    // =========================================================================

    #[test]
    fn test_btree_compaction_config_defaults() {
        let config = BTreeCompactionConfig::default();
        assert!((config.min_fill_factor - 0.4).abs() < f64::EPSILON);
        assert_eq!(config.interval_secs, 3600);
    }

    #[test]
    fn test_btree_compaction_config_builder_defaults() {
        let config = BTreeCompactionConfig::builder().build().expect("valid");
        assert!((config.min_fill_factor - 0.4).abs() < f64::EPSILON);
        assert_eq!(config.interval_secs, 3600);
    }

    #[test]
    fn test_btree_compaction_config_builder_custom() {
        let config = BTreeCompactionConfig::builder()
            .min_fill_factor(0.6)
            .interval_secs(1800)
            .build()
            .expect("valid");
        assert!((config.min_fill_factor - 0.6).abs() < f64::EPSILON);
        assert_eq!(config.interval_secs, 1800);
    }

    #[test]
    fn test_btree_compaction_config_fill_factor_zero() {
        let result = BTreeCompactionConfig::builder().min_fill_factor(0.0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_btree_compaction_config_fill_factor_one() {
        let result = BTreeCompactionConfig::builder().min_fill_factor(1.0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_btree_compaction_config_interval_too_short() {
        let result = BTreeCompactionConfig::builder().interval_secs(59).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_btree_compaction_config_interval_minimum() {
        let config =
            BTreeCompactionConfig::builder().interval_secs(60).build().expect("valid at minimum");
        assert_eq!(config.interval_secs, 60);
    }

    #[test]
    fn test_btree_compaction_config_serde_roundtrip() {
        let config = BTreeCompactionConfig::builder()
            .min_fill_factor(0.5)
            .interval_secs(7200)
            .build()
            .expect("valid");
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: BTreeCompactionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_btree_compaction_config_validate_after_deserialize() {
        // Simulate deserializing invalid config
        let config = BTreeCompactionConfig { min_fill_factor: 1.5, interval_secs: 3600 };
        assert!(config.validate().is_err());
    }

    // =========================================================================
    // ShutdownConfig validation tests
    // =========================================================================

    #[test]
    fn test_shutdown_config_defaults_are_valid() {
        let config = ShutdownConfig::default();
        assert_eq!(config.grace_period_secs, 15);
        assert_eq!(config.drain_timeout_secs, 30);
        assert_eq!(config.pre_stop_delay_secs, 5);
        assert_eq!(config.pre_shutdown_timeout_secs, 60);
        assert_eq!(config.watchdog_multiplier, 2);
        config.validate().expect("defaults should be valid");
    }

    #[test]
    fn test_shutdown_config_builder_defaults() {
        let config = ShutdownConfig::builder().build().expect("valid");
        assert_eq!(config.grace_period_secs, 15);
        assert_eq!(config.drain_timeout_secs, 30);
        assert_eq!(config.pre_stop_delay_secs, 5);
        assert_eq!(config.pre_shutdown_timeout_secs, 60);
        assert_eq!(config.watchdog_multiplier, 2);
    }

    #[test]
    fn test_shutdown_config_builder_custom() {
        let config = ShutdownConfig::builder()
            .grace_period_secs(10)
            .drain_timeout_secs(60)
            .pre_stop_delay_secs(3)
            .pre_shutdown_timeout_secs(120)
            .watchdog_multiplier(3)
            .build()
            .expect("valid custom config");
        assert_eq!(config.grace_period_secs, 10);
        assert_eq!(config.drain_timeout_secs, 60);
        assert_eq!(config.pre_stop_delay_secs, 3);
        assert_eq!(config.pre_shutdown_timeout_secs, 120);
        assert_eq!(config.watchdog_multiplier, 3);
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn test_shutdown_config_boundary_values() {
        let cases: Vec<(&str, Box<dyn Fn() -> Result<ShutdownConfig, ConfigError>>, bool)> = vec![
            (
                "grace_period_secs=0 rejected",
                Box::new(|| ShutdownConfig::builder().grace_period_secs(0).build()),
                false,
            ),
            (
                "grace_period_secs=1 accepted",
                Box::new(|| ShutdownConfig::builder().grace_period_secs(1).build()),
                true,
            ),
            (
                "drain_timeout_secs=4 rejected",
                Box::new(|| ShutdownConfig::builder().drain_timeout_secs(4).build()),
                false,
            ),
            (
                "drain_timeout_secs=5 accepted",
                Box::new(|| ShutdownConfig::builder().drain_timeout_secs(5).build()),
                true,
            ),
            (
                "pre_shutdown_timeout_secs=4 rejected",
                Box::new(|| ShutdownConfig::builder().pre_shutdown_timeout_secs(4).build()),
                false,
            ),
            (
                "pre_shutdown_timeout_secs=5 accepted",
                Box::new(|| ShutdownConfig::builder().pre_shutdown_timeout_secs(5).build()),
                true,
            ),
            (
                "watchdog_multiplier=0 rejected",
                Box::new(|| ShutdownConfig::builder().watchdog_multiplier(0).build()),
                false,
            ),
            (
                "pre_stop_delay_secs=0 accepted",
                Box::new(|| ShutdownConfig::builder().pre_stop_delay_secs(0).build()),
                true,
            ),
        ];
        for (label, build_fn, should_succeed) in &cases {
            let result = build_fn();
            assert_eq!(result.is_ok(), *should_succeed, "{label}");
        }
    }

    #[test]
    fn test_shutdown_config_serde_roundtrip() {
        let config = ShutdownConfig::builder()
            .grace_period_secs(20)
            .drain_timeout_secs(45)
            .pre_stop_delay_secs(10)
            .pre_shutdown_timeout_secs(90)
            .watchdog_multiplier(4)
            .build()
            .expect("valid");
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ShutdownConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_shutdown_config_validate_after_deserialize() {
        let config = ShutdownConfig {
            grace_period_secs: 0,
            drain_timeout_secs: 30,
            pre_stop_delay_secs: 5,
            pre_shutdown_timeout_secs: 60,
            watchdog_multiplier: 2,
            leader_transfer_timeout_secs: 10,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_shutdown_config_serde_defaults() {
        let json = "{}";
        let config: ShutdownConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.grace_period_secs, 15);
        assert_eq!(config.drain_timeout_secs, 30);
        assert_eq!(config.pre_stop_delay_secs, 5);
        assert_eq!(config.pre_shutdown_timeout_secs, 60);
        assert_eq!(config.watchdog_multiplier, 2);
    }

    // ─── HotKeyConfig Tests ───────────────────────────────────

    #[test]
    fn test_hot_key_config_defaults_are_valid() {
        let config = HotKeyConfig::default();
        assert_eq!(config.window_secs, 60);
        assert_eq!(config.threshold, 100);
        assert_eq!(config.cms_width, 1024);
        assert_eq!(config.cms_depth, 4);
        assert_eq!(config.top_k, 10);
        config.validate().unwrap();
    }

    #[test]
    fn test_hot_key_config_builder_defaults() {
        let config = HotKeyConfig::builder().build().unwrap();
        assert_eq!(config, HotKeyConfig::default());
    }

    #[test]
    fn test_hot_key_config_builder_custom() {
        let config = HotKeyConfig::builder()
            .window_secs(30)
            .threshold(200)
            .cms_width(2048)
            .cms_depth(6)
            .top_k(20)
            .build()
            .unwrap();
        assert_eq!(config.window_secs, 30);
        assert_eq!(config.threshold, 200);
        assert_eq!(config.cms_width, 2048);
        assert_eq!(config.cms_depth, 6);
        assert_eq!(config.top_k, 20);
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn test_hot_key_config_boundary_values() {
        let cases: Vec<(&str, Box<dyn Fn() -> Result<HotKeyConfig, ConfigError>>, bool)> = vec![
            (
                "window_secs=0 rejected",
                Box::new(|| HotKeyConfig::builder().window_secs(0).build()),
                false,
            ),
            (
                "window_secs=1 accepted",
                Box::new(|| HotKeyConfig::builder().window_secs(1).build()),
                true,
            ),
            (
                "threshold=0 rejected",
                Box::new(|| HotKeyConfig::builder().threshold(0).build()),
                false,
            ),
            (
                "threshold=1 accepted",
                Box::new(|| HotKeyConfig::builder().threshold(1).build()),
                true,
            ),
            (
                "cms_width=63 rejected",
                Box::new(|| HotKeyConfig::builder().cms_width(63).build()),
                false,
            ),
            (
                "cms_width=64 accepted",
                Box::new(|| HotKeyConfig::builder().cms_width(64).build()),
                true,
            ),
            (
                "cms_depth=1 rejected",
                Box::new(|| HotKeyConfig::builder().cms_depth(1).build()),
                false,
            ),
            (
                "cms_depth=2 accepted",
                Box::new(|| HotKeyConfig::builder().cms_depth(2).build()),
                true,
            ),
            ("top_k=0 rejected", Box::new(|| HotKeyConfig::builder().top_k(0).build()), false),
            ("top_k=1 accepted", Box::new(|| HotKeyConfig::builder().top_k(1).build()), true),
        ];
        for (label, build_fn, should_succeed) in &cases {
            let result = build_fn();
            assert_eq!(result.is_ok(), *should_succeed, "{label}");
        }
    }

    #[test]
    fn test_hot_key_config_serde_roundtrip() {
        let config = HotKeyConfig::builder()
            .window_secs(30)
            .threshold(200)
            .cms_width(512)
            .cms_depth(3)
            .top_k(5)
            .build()
            .unwrap();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: HotKeyConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_hot_key_config_serde_defaults() {
        let json = "{}";
        let config: HotKeyConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.window_secs, 60);
        assert_eq!(config.threshold, 100);
        assert_eq!(config.cms_width, 1024);
        assert_eq!(config.cms_depth, 4);
        assert_eq!(config.top_k, 10);
    }

    // =========================================================================
    // ValidationConfig tests
    // =========================================================================

    #[test]
    fn test_validation_config_defaults_are_valid() {
        let config = ValidationConfig::default();
        config.validate().unwrap();
    }

    #[test]
    fn test_validation_config_builder_defaults() {
        let config = ValidationConfig::builder().build().unwrap();
        assert_eq!(config, ValidationConfig::default());
    }

    #[test]
    fn test_validation_config_builder_custom() {
        let config = ValidationConfig::builder()
            .max_key_bytes(2048)
            .max_value_bytes(1024)
            .max_operations_per_write(500)
            .max_batch_payload_bytes(50 * 1024 * 1024)
            .max_organization_name_chars(128)
            .max_relationship_string_bytes(512)
            .build()
            .unwrap();
        assert_eq!(config.max_key_bytes, 2048);
        assert_eq!(config.max_value_bytes, 1024);
        assert_eq!(config.max_operations_per_write, 500);
        assert_eq!(config.max_batch_payload_bytes, 50 * 1024 * 1024);
        assert_eq!(config.max_organization_name_chars, 128);
        assert_eq!(config.max_relationship_string_bytes, 512);
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn test_validation_config_rejects_zero_fields() {
        let cases: Vec<(&str, Box<dyn Fn() -> Result<ValidationConfig, ConfigError>>)> = vec![
            ("max_key_bytes", Box::new(|| ValidationConfig::builder().max_key_bytes(0).build())),
            (
                "max_value_bytes",
                Box::new(|| ValidationConfig::builder().max_value_bytes(0).build()),
            ),
            (
                "max_operations_per_write",
                Box::new(|| ValidationConfig::builder().max_operations_per_write(0).build()),
            ),
            (
                "max_batch_payload_bytes",
                Box::new(|| ValidationConfig::builder().max_batch_payload_bytes(0).build()),
            ),
            (
                "max_organization_name_chars",
                Box::new(|| ValidationConfig::builder().max_organization_name_chars(0).build()),
            ),
            (
                "max_relationship_string_bytes",
                Box::new(|| ValidationConfig::builder().max_relationship_string_bytes(0).build()),
            ),
        ];
        for (field, build_fn) in &cases {
            let result = build_fn();
            assert!(result.is_err(), "{field} = 0 should be rejected");
        }
    }

    #[test]
    fn test_validation_config_serde_roundtrip() {
        let config = ValidationConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ValidationConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_validation_config_serde_defaults() {
        let json = "{}";
        let config: ValidationConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config, ValidationConfig::default());
    }

    #[test]
    fn test_validation_config_validate_method() {
        let config = ValidationConfig { max_key_bytes: 0, ..ValidationConfig::default() };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("max_key_bytes"));
    }

    // =========================================================================
    // IntegrityConfig tests
    // =========================================================================

    #[test]
    fn test_integrity_config_defaults_are_valid() {
        let config = IntegrityConfig::default();
        config.validate().unwrap();
        assert_eq!(config.scrub_interval_secs, 3600);
        assert!((config.pages_per_cycle_percent - 1.0).abs() < f64::EPSILON);
        assert_eq!(config.full_scan_period_secs, 345_600);
    }

    #[test]
    fn test_integrity_config_builder_with_custom_values() {
        let config = IntegrityConfig::builder()
            .scrub_interval_secs(1800)
            .pages_per_cycle_percent(5.0)
            .full_scan_period_secs(86400)
            .build()
            .unwrap();
        assert_eq!(config.scrub_interval_secs, 1800);
        assert!((config.pages_per_cycle_percent - 5.0).abs() < f64::EPSILON);
        assert_eq!(config.full_scan_period_secs, 86400);
    }

    #[test]
    fn test_integrity_config_scrub_interval_too_low() {
        let err = IntegrityConfig::builder().scrub_interval_secs(30).build().unwrap_err();
        assert!(err.to_string().contains("scrub_interval_secs"));
    }

    #[test]
    fn test_integrity_config_pages_percent_zero() {
        let err = IntegrityConfig::builder().pages_per_cycle_percent(0.0).build().unwrap_err();
        assert!(err.to_string().contains("pages_per_cycle_percent"));
    }

    #[test]
    fn test_integrity_config_pages_percent_over_100() {
        let err = IntegrityConfig::builder().pages_per_cycle_percent(101.0).build().unwrap_err();
        assert!(err.to_string().contains("pages_per_cycle_percent"));
    }

    #[test]
    fn test_integrity_config_full_scan_less_than_interval() {
        let err = IntegrityConfig::builder()
            .scrub_interval_secs(7200)
            .full_scan_period_secs(3600)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("full_scan_period_secs"));
    }

    #[test]
    fn test_integrity_config_serde_defaults() {
        let json = "{}";
        let config: IntegrityConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config, IntegrityConfig::default());
    }

    #[test]
    fn test_integrity_config_validate_method() {
        let config = IntegrityConfig { scrub_interval_secs: 10, ..IntegrityConfig::default() };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("scrub_interval_secs"));
    }

    #[test]
    fn test_runtime_config_integrity_validation() {
        let config = RuntimeConfig {
            integrity: Some(IntegrityConfig {
                scrub_interval_secs: 10,
                ..IntegrityConfig::default()
            }),
            ..RuntimeConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_runtime_config_integrity_diff() {
        let a = RuntimeConfig::default();
        let b = RuntimeConfig {
            integrity: Some(IntegrityConfig::default()),
            ..RuntimeConfig::default()
        };
        let changes = a.diff(&b);
        assert!(changes.contains(&"integrity".to_string()));
    }

    // =========================================================================
    // MetricsCardinalityConfig tests
    // =========================================================================

    #[test]
    fn test_metrics_cardinality_config_defaults_are_valid() {
        let config = MetricsCardinalityConfig::builder().build().expect("defaults should be valid");
        assert_eq!(config.warn_cardinality, 5000);
        assert_eq!(config.max_cardinality, 10_000);
    }

    #[test]
    fn test_metrics_cardinality_config_builder_custom() {
        let config = MetricsCardinalityConfig::builder()
            .warn_cardinality(3000)
            .max_cardinality(8000)
            .build()
            .expect("valid config");
        assert_eq!(config.warn_cardinality, 3000);
        assert_eq!(config.max_cardinality, 8000);
    }

    #[test]
    fn test_metrics_cardinality_config_warn_zero() {
        let result = MetricsCardinalityConfig::builder().warn_cardinality(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_metrics_cardinality_config_max_zero() {
        let result = MetricsCardinalityConfig::builder().max_cardinality(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_metrics_cardinality_config_warn_equals_max() {
        let result = MetricsCardinalityConfig::builder()
            .warn_cardinality(5000)
            .max_cardinality(5000)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_metrics_cardinality_config_warn_exceeds_max() {
        let result = MetricsCardinalityConfig::builder()
            .warn_cardinality(10_000)
            .max_cardinality(5000)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_metrics_cardinality_config_serde_roundtrip() {
        let config = MetricsCardinalityConfig::builder().build().expect("defaults should be valid");
        let json = serde_json::to_string(&config).expect("serialize");
        let deserialized: MetricsCardinalityConfig =
            serde_json::from_str(&json).expect("deserialize");
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_metrics_cardinality_config_serde_defaults() {
        let json = "{}";
        let config: MetricsCardinalityConfig = serde_json::from_str(json).expect("deserialize");
        assert_eq!(config.warn_cardinality, 5000);
        assert_eq!(config.max_cardinality, 10_000);
    }

    #[test]
    fn test_runtime_config_metrics_cardinality_validation() {
        let config = RuntimeConfig {
            metrics_cardinality: Some(MetricsCardinalityConfig {
                warn_cardinality: 0,
                max_cardinality: 100,
            }),
            ..RuntimeConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_runtime_config_metrics_cardinality_diff() {
        let a = RuntimeConfig::default();
        let b = RuntimeConfig {
            metrics_cardinality: Some(MetricsCardinalityConfig::default()),
            ..RuntimeConfig::default()
        };
        let changes = a.diff(&b);
        assert!(changes.contains(&"metrics_cardinality".to_string()));
    }

    // =========================================================================
    // DetailedDiff tests
    // =========================================================================

    #[test]
    fn test_detailed_diff_empty_when_identical() {
        let a = RuntimeConfig::default();
        let b = RuntimeConfig::default();
        let changes = a.detailed_diff(&b);
        assert!(changes.is_empty());
    }

    #[test]
    fn test_detailed_diff_detects_nested_field_changes() {
        let a = RuntimeConfig {
            rate_limit: Some(RateLimitConfig::default()),
            ..RuntimeConfig::default()
        };
        let b = RuntimeConfig {
            rate_limit: Some(RateLimitConfig { client_burst: 999, ..RateLimitConfig::default() }),
            ..RuntimeConfig::default()
        };
        let changes = a.detailed_diff(&b);
        assert!(!changes.is_empty());
        let field_names: Vec<&str> = changes.iter().map(|c| c.field.as_str()).collect();
        assert!(
            field_names.contains(&"rate_limit.client_burst"),
            "Expected rate_limit.client_burst in {field_names:?}"
        );
        // Only client_burst changed — other fields should not appear.
        assert_eq!(changes.len(), 1, "Expected exactly 1 change, got {changes:?}");
    }

    #[test]
    fn test_detailed_diff_detects_added_optional_section() {
        let a = RuntimeConfig::default();
        let b = RuntimeConfig {
            rate_limit: Some(RateLimitConfig::default()),
            ..RuntimeConfig::default()
        };
        let changes = a.detailed_diff(&b);
        assert!(!changes.is_empty());
        // Should report the whole section as added (null → object).
        let field_names: Vec<&str> = changes.iter().map(|c| c.field.as_str()).collect();
        assert!(
            field_names.iter().any(|f| f.starts_with("rate_limit")),
            "Expected rate_limit fields in {field_names:?}"
        );
    }

    #[test]
    fn test_detailed_diff_detects_removed_optional_section() {
        let a =
            RuntimeConfig { hot_key: Some(HotKeyConfig::default()), ..RuntimeConfig::default() };
        let b = RuntimeConfig::default();
        let changes = a.detailed_diff(&b);
        assert!(!changes.is_empty());
        let field_names: Vec<&str> = changes.iter().map(|c| c.field.as_str()).collect();
        assert!(
            field_names.iter().any(|f| f.starts_with("hot_key")),
            "Expected hot_key fields in {field_names:?}"
        );
    }

    #[test]
    fn test_detailed_diff_multiple_sections_changed() {
        let a = RuntimeConfig::default();
        let b = RuntimeConfig {
            rate_limit: Some(RateLimitConfig::default()),
            validation: Some(ValidationConfig::default()),
            ..RuntimeConfig::default()
        };
        let changes = a.detailed_diff(&b);
        let field_names: Vec<&str> = changes.iter().map(|c| c.field.as_str()).collect();
        assert!(
            field_names.iter().any(|f| f.starts_with("rate_limit")),
            "Expected rate_limit fields"
        );
        assert!(
            field_names.iter().any(|f| f.starts_with("validation")),
            "Expected validation fields"
        );
    }

    #[test]
    fn test_config_change_display() {
        let change = ConfigChange {
            field: "rate_limit.client_burst".to_string(),
            old: "100".to_string(),
            new: "200".to_string(),
        };
        let display = change.to_string();
        assert_eq!(display, "rate_limit.client_burst: 100 → 200");
    }

    #[test]
    fn test_config_change_serialization_roundtrip() {
        let change = ConfigChange {
            field: "hot_key.threshold".to_string(),
            old: "50".to_string(),
            new: "100".to_string(),
        };
        let json = serde_json::to_string(&change).unwrap();
        let deserialized: ConfigChange = serde_json::from_str(&json).unwrap();
        assert_eq!(change, deserialized);
    }

    // =========================================================================
    // JsonSchema tests
    // =========================================================================

    #[test]
    fn test_runtime_config_json_schema_is_valid() {
        let schema = schemars::schema_for!(RuntimeConfig);
        let json = serde_json::to_string_pretty(&schema).unwrap();
        // Validate it's parseable JSON.
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        // Must have a $schema field pointing to JSON Schema draft.
        assert!(value.get("$schema").is_some(), "Schema missing $schema field");
        // Must have a title matching the type name.
        assert_eq!(value.get("title").and_then(|v| v.as_str()), Some("RuntimeConfig"));
        // Must have properties for all sections.
        let props = value.get("properties").and_then(|v| v.as_object()).unwrap();
        assert!(props.contains_key("rate_limit"), "Missing rate_limit");
        assert!(props.contains_key("hot_key"), "Missing hot_key");
        assert!(props.contains_key("compaction"), "Missing compaction");
        assert!(props.contains_key("validation"), "Missing validation");
        assert!(props.contains_key("integrity"), "Missing integrity");
        assert!(props.contains_key("metrics_cardinality"), "Missing metrics_cardinality");
    }

    #[test]
    fn test_rate_limit_config_json_schema_has_fields() {
        let schema = schemars::schema_for!(RateLimitConfig);
        let json = serde_json::to_string(&schema).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let props = value.get("properties").and_then(|v| v.as_object()).unwrap();
        assert!(props.contains_key("client_burst"));
        assert!(props.contains_key("client_rate"));
        assert!(props.contains_key("organization_burst"));
        assert!(props.contains_key("organization_rate"));
        assert!(props.contains_key("backpressure_threshold"));
    }

    // =========================================================================
    // Region::retention_days tests
    // =========================================================================

    #[test]
    fn test_region_retention_days() {
        use crate::Region;

        let cases: &[(Region, u32)] = &[
            // GDPR regions: 30 days
            (Region::IE_EAST_DUBLIN, 30),
            (Region::FR_NORTH_PARIS, 30),
            (Region::DE_CENTRAL_FRANKFURT, 30),
            (Region::SE_EAST_STOCKHOLM, 30),
            (Region::IT_NORTH_MILAN, 30),
            (Region::UK_SOUTH_LONDON, 30),
            // Non-GDPR regions: 90 days
            (Region::US_EAST_VA, 90),
            (Region::US_WEST_OR, 90),
            (Region::GLOBAL, 90),
            (Region::JP_EAST_TOKYO, 90),
            (Region::AU_EAST_SYDNEY, 90),
            (Region::BR_SOUTHEAST_SP, 90),
            (Region::SG_CENTRAL_SINGAPORE, 90),
            (Region::IN_WEST_MUMBAI, 90),
            (Region::SA_CENTRAL_RIYADH, 90),
        ];

        for (region, expected) in cases {
            assert_eq!(
                region.retention_days(),
                *expected,
                "{region:?} should have {expected}-day retention"
            );
        }
    }

    // =========================================================================
    // JwtConfig validation tests
    // =========================================================================

    #[test]
    fn test_jwt_config_defaults_are_valid() {
        let config = JwtConfig::builder().build().expect("defaults should be valid");
        assert_eq!(config.issuer, "inferadb");
        assert_eq!(config.session_access_ttl_secs, 1800);
        assert_eq!(config.session_refresh_ttl_secs, 1_209_600);
        assert_eq!(config.vault_access_ttl_secs, 900);
        assert_eq!(config.vault_refresh_ttl_secs, 3600);
        assert_eq!(config.clock_skew_secs, 30);
        assert_eq!(config.key_rotation_grace_secs, 14400);
        assert_eq!(config.max_family_lifetime_secs, 2_592_000);
    }

    #[test]
    fn test_jwt_config_default_trait() {
        let config = JwtConfig::default();
        assert_eq!(config.issuer, "inferadb");
        config.validate().expect("default should be valid");
    }

    #[test]
    fn test_jwt_config_builder_custom_values() {
        let config = JwtConfig::builder()
            .issuer("my-cluster")
            .session_access_ttl_secs(600)
            .session_refresh_ttl_secs(86400)
            .vault_access_ttl_secs(300)
            .vault_refresh_ttl_secs(1800)
            .clock_skew_secs(10)
            .key_rotation_grace_secs(7200)
            .build()
            .expect("custom values should be valid");
        assert_eq!(config.issuer, "my-cluster");
        assert_eq!(config.session_access_ttl_secs, 600);
        assert_eq!(config.vault_access_ttl_secs, 300);
    }

    #[test]
    fn test_jwt_config_rejects_empty_issuer() {
        let result = JwtConfig::builder().issuer("").build();
        assert!(result.is_err());
    }

    #[test]
    fn test_jwt_config_rejects_zero_ttls() {
        let fields: Vec<(&str, _)> = vec![
            ("session_access", JwtConfig::builder().session_access_ttl_secs(0).build()),
            ("session_refresh", JwtConfig::builder().session_refresh_ttl_secs(0).build()),
            ("vault_access", JwtConfig::builder().vault_access_ttl_secs(0).build()),
            ("vault_refresh", JwtConfig::builder().vault_refresh_ttl_secs(0).build()),
            ("key_rotation_grace", JwtConfig::builder().key_rotation_grace_secs(0).build()),
            ("max_family_lifetime", JwtConfig::builder().max_family_lifetime_secs(0).build()),
        ];
        for (name, result) in fields {
            assert!(result.is_err(), "{name} should reject zero");
        }
    }

    #[test]
    fn test_jwt_config_refresh_must_exceed_access() {
        // Session: refresh <= access
        let result = JwtConfig::builder()
            .session_access_ttl_secs(1800)
            .session_refresh_ttl_secs(1800)
            .build();
        assert!(result.is_err());

        // Vault: refresh <= access
        let result =
            JwtConfig::builder().vault_access_ttl_secs(900).vault_refresh_ttl_secs(900).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_jwt_config_family_lifetime_must_be_at_least_refresh_ttl() {
        let result = JwtConfig::builder()
            .session_refresh_ttl_secs(1_209_600)
            .max_family_lifetime_secs(86400) // 1 day < 14 day refresh TTL
            .build();
        assert!(result.is_err());

        // Equal is fine
        let result = JwtConfig::builder()
            .session_refresh_ttl_secs(86400)
            .max_family_lifetime_secs(86400)
            .build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_jwt_config_clock_skew_must_be_less_than_min_access_ttl() {
        // clock_skew >= min(session_access, vault_access)
        let result = JwtConfig::builder()
            .vault_access_ttl_secs(30)
            .vault_refresh_ttl_secs(3600)
            .clock_skew_secs(30)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_jwt_config_serde_roundtrip() {
        let config = JwtConfig::builder()
            .issuer("test")
            .session_access_ttl_secs(600)
            .session_refresh_ttl_secs(7200)
            .build()
            .unwrap();
        let json = serde_json::to_string(&config).unwrap();
        let parsed: JwtConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, parsed);
    }

    #[test]
    fn test_jwt_config_serde_defaults() {
        let config: JwtConfig = serde_json::from_str("{}").unwrap();
        config.validate().expect("serde defaults should be valid");
        assert_eq!(config.issuer, "inferadb");
        assert_eq!(config.session_access_ttl_secs, 1800);
    }

    #[test]
    fn test_jwt_config_validate_method() {
        let mut config = JwtConfig::default();
        assert!(config.validate().is_ok());
        config.session_access_ttl_secs = 0;
        assert!(config.validate().is_err());
    }
}
