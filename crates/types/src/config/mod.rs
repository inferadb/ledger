//! Configuration types for InferaDB Ledger.
//!
//! Configuration is loaded from TOML files and environment variables.
//! All config structs validate their values at construction time via
//! fallible builders. Post-deserialization validation is available via
//! the `validate()` method on each struct.

// The schemars `JsonSchema` derive macro internally uses `.unwrap()` in its
// `json_schema!` and `json_internal!` expansions. Allow `disallowed_methods`
// at the module level since config types are declarative structs with minimal
// procedural code.
#![allow(clippy::disallowed_methods)]

mod node;
mod observability;
mod raft;
mod resilience;
mod runtime;
mod storage;

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

/// Duration serialization using humantime format.
mod humantime_serde {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&humantime::format_duration(*duration).to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        humantime::parse_duration(&s).map_err(serde::de::Error::custom)
    }
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
    fn test_storage_config_cache_size_zero() {
        let result = StorageConfig::builder().cache_size_bytes(0).build();
        assert!(result.is_err());
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

    #[test]
    fn test_storage_config_compression_level_negative() {
        let result = StorageConfig::builder().compression_level(-1).build();
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

    #[test]
    fn test_default_configs() {
        let storage = StorageConfig::default();
        assert_eq!(storage.hot_cache_snapshots, 3);
        assert_eq!(storage.compression_level, 3);

        let raft = RaftConfig::default();
        assert_eq!(raft.heartbeat_interval, Duration::from_millis(100));

        let batch = BatchConfig::default();
        assert_eq!(batch.max_batch_size, 100);
        assert!(batch.coalesce_enabled);
    }

    #[test]
    fn test_defaults_pass_validation() {
        assert!(StorageConfig::default().validate().is_ok());
        assert!(RaftConfig::default().validate().is_ok());
        assert!(BatchConfig::default().validate().is_ok());
        assert!(RateLimitConfig::default().validate().is_ok());
    }

    #[test]
    fn test_builder_matches_default() {
        assert_eq!(StorageConfig::builder().build().expect("valid"), StorageConfig::default());
        assert_eq!(RaftConfig::builder().build().expect("valid"), RaftConfig::default());
        assert_eq!(BatchConfig::builder().build().expect("valid"), BatchConfig::default());
        assert_eq!(RateLimitConfig::builder().build().expect("valid"), RateLimitConfig::default());
    }

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

    #[test]
    fn test_config_error_display() {
        let err = ConfigError::Validation { message: "test error".to_string() };
        assert_eq!(err.to_string(), "invalid config: test error");
    }

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
    fn test_rate_limit_config_client_burst_zero() {
        let result = RateLimitConfig::builder().client_burst(0).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("client_burst"));
    }

    #[test]
    fn test_rate_limit_config_client_rate_zero() {
        let result = RateLimitConfig::builder().client_rate(0.0).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("client_rate"));
    }

    #[test]
    fn test_rate_limit_config_client_rate_negative() {
        let result = RateLimitConfig::builder().client_rate(-1.0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_rate_limit_config_organization_burst_zero() {
        let result = RateLimitConfig::builder().organization_burst(0).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("organization_burst"));
    }

    #[test]
    fn test_rate_limit_config_organization_rate_zero() {
        let result = RateLimitConfig::builder().organization_rate(0.0).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("organization_rate"));
    }

    #[test]
    fn test_rate_limit_config_backpressure_threshold_zero() {
        let result = RateLimitConfig::builder().backpressure_threshold(0).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("backpressure_threshold"));
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
    fn test_rate_limit_config_default_impl() {
        let config = RateLimitConfig::default();
        assert_eq!(config.client_burst, 100);
        assert_eq!(config.client_rate, 50.0);
        assert_eq!(config.organization_burst, 1000);
        assert_eq!(config.organization_rate, 500.0);
        assert_eq!(config.backpressure_threshold, 100);
        assert!(config.validate().is_ok());
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

    #[test]
    fn test_rate_limit_config_builder_matches_default() {
        let from_builder = RateLimitConfig::builder().build().expect("valid");
        let from_default = RateLimitConfig::default();
        assert_eq!(from_builder, from_default);
    }

    // =========================================================================
    // AuditConfig validation tests
    // =========================================================================

    #[test]
    fn test_audit_config_builder_with_valid_values() {
        let config = AuditConfig::builder()
            .path("/var/log/audit.jsonl")
            .max_file_size_bytes(50 * 1024 * 1024)
            .max_rotated_files(20)
            .build()
            .expect("valid config");
        assert_eq!(config.path, "/var/log/audit.jsonl");
        assert_eq!(config.max_file_size_bytes, 50 * 1024 * 1024);
        assert_eq!(config.max_rotated_files, 20);
    }

    #[test]
    fn test_audit_config_builder_with_defaults() {
        let config = AuditConfig::builder()
            .path("/var/log/audit.jsonl")
            .build()
            .expect("valid config with defaults");
        assert_eq!(config.max_file_size_bytes, 100 * 1024 * 1024);
        assert_eq!(config.max_rotated_files, 10);
    }

    #[test]
    fn test_audit_config_empty_path() {
        let result = AuditConfig::builder().path("").build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("path"));
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_audit_config_file_size_too_small() {
        let result = AuditConfig::builder()
            .path("/var/log/audit.jsonl")
            .max_file_size_bytes(MIN_AUDIT_FILE_SIZE - 1)
            .build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("max_file_size_bytes"));
        assert!(err.to_string().contains("1 MB"));
    }

    #[test]
    fn test_audit_config_file_size_minimum() {
        let result = AuditConfig::builder()
            .path("/var/log/audit.jsonl")
            .max_file_size_bytes(MIN_AUDIT_FILE_SIZE)
            .build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_audit_config_max_rotated_files_zero() {
        let result =
            AuditConfig::builder().path("/var/log/audit.jsonl").max_rotated_files(0).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("max_rotated_files"));
    }

    #[test]
    fn test_audit_config_max_rotated_files_one() {
        let result =
            AuditConfig::builder().path("/var/log/audit.jsonl").max_rotated_files(1).build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_audit_config_validate_method() {
        let mut config = AuditConfig {
            path: "/var/log/audit.jsonl".to_string(),
            max_file_size_bytes: 100 * 1024 * 1024,
            max_rotated_files: 10,
        };
        assert!(config.validate().is_ok());

        config.path = String::new();
        assert!(config.validate().is_err());

        config.path = "/var/log/audit.jsonl".to_string();
        config.max_file_size_bytes = 0;
        assert!(config.validate().is_err());

        config.max_file_size_bytes = 100 * 1024 * 1024;
        config.max_rotated_files = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_audit_config_serde_roundtrip() {
        let config = AuditConfig::builder()
            .path("/var/log/audit.jsonl")
            .max_file_size_bytes(50 * 1024 * 1024)
            .max_rotated_files(5)
            .build()
            .expect("valid");

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: AuditConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_audit_config_into_string() {
        // Test that #[builder(into)] works for path
        let config = AuditConfig::builder()
            .path("/var/log/audit.jsonl") // &str should work via Into<String>
            .build()
            .expect("valid");
        assert_eq!(config.path, "/var/log/audit.jsonl");
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
    fn test_btree_compaction_config_fill_factor_negative() {
        let result = BTreeCompactionConfig::builder().min_fill_factor(-0.1).build();
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
    fn test_shutdown_config_grace_period_zero() {
        let result = ShutdownConfig::builder().grace_period_secs(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_shutdown_config_grace_period_minimum() {
        let config =
            ShutdownConfig::builder().grace_period_secs(1).build().expect("valid at minimum");
        assert_eq!(config.grace_period_secs, 1);
    }

    #[test]
    fn test_shutdown_config_drain_timeout_too_short() {
        let result = ShutdownConfig::builder().drain_timeout_secs(4).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_shutdown_config_drain_timeout_minimum() {
        let config =
            ShutdownConfig::builder().drain_timeout_secs(5).build().expect("valid at minimum");
        assert_eq!(config.drain_timeout_secs, 5);
    }

    #[test]
    fn test_shutdown_config_pre_shutdown_timeout_too_short() {
        let result = ShutdownConfig::builder().pre_shutdown_timeout_secs(4).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_shutdown_config_pre_shutdown_timeout_minimum() {
        let config = ShutdownConfig::builder()
            .pre_shutdown_timeout_secs(5)
            .build()
            .expect("valid at minimum");
        assert_eq!(config.pre_shutdown_timeout_secs, 5);
    }

    #[test]
    fn test_shutdown_config_watchdog_multiplier_zero() {
        let result = ShutdownConfig::builder().watchdog_multiplier(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_shutdown_config_pre_stop_delay_zero_allowed() {
        let config =
            ShutdownConfig::builder().pre_stop_delay_secs(0).build().expect("zero is valid");
        assert_eq!(config.pre_stop_delay_secs, 0);
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
    fn test_hot_key_config_window_secs_zero() {
        assert!(HotKeyConfig::builder().window_secs(0).build().is_err());
    }

    #[test]
    fn test_hot_key_config_window_secs_minimum() {
        let config = HotKeyConfig::builder().window_secs(1).build().unwrap();
        assert_eq!(config.window_secs, 1);
    }

    #[test]
    fn test_hot_key_config_threshold_zero() {
        assert!(HotKeyConfig::builder().threshold(0).build().is_err());
    }

    #[test]
    fn test_hot_key_config_threshold_minimum() {
        let config = HotKeyConfig::builder().threshold(1).build().unwrap();
        assert_eq!(config.threshold, 1);
    }

    #[test]
    fn test_hot_key_config_cms_width_too_small() {
        assert!(HotKeyConfig::builder().cms_width(63).build().is_err());
    }

    #[test]
    fn test_hot_key_config_cms_width_minimum() {
        let config = HotKeyConfig::builder().cms_width(64).build().unwrap();
        assert_eq!(config.cms_width, 64);
    }

    #[test]
    fn test_hot_key_config_cms_depth_too_small() {
        assert!(HotKeyConfig::builder().cms_depth(1).build().is_err());
    }

    #[test]
    fn test_hot_key_config_cms_depth_minimum() {
        let config = HotKeyConfig::builder().cms_depth(2).build().unwrap();
        assert_eq!(config.cms_depth, 2);
    }

    #[test]
    fn test_hot_key_config_top_k_zero() {
        assert!(HotKeyConfig::builder().top_k(0).build().is_err());
    }

    #[test]
    fn test_hot_key_config_top_k_minimum() {
        let config = HotKeyConfig::builder().top_k(1).build().unwrap();
        assert_eq!(config.top_k, 1);
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
    fn test_hot_key_config_validate_after_deserialize() {
        let config = HotKeyConfig::default();
        config.validate().unwrap();
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
    fn test_validation_config_max_key_bytes_zero() {
        let result = ValidationConfig::builder().max_key_bytes(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_config_max_value_bytes_zero() {
        let result = ValidationConfig::builder().max_value_bytes(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_config_max_operations_per_write_zero() {
        let result = ValidationConfig::builder().max_operations_per_write(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_config_max_batch_payload_bytes_zero() {
        let result = ValidationConfig::builder().max_batch_payload_bytes(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_config_max_organization_name_chars_zero() {
        let result = ValidationConfig::builder().max_organization_name_chars(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_config_max_relationship_string_bytes_zero() {
        let result = ValidationConfig::builder().max_relationship_string_bytes(0).build();
        assert!(result.is_err());
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
    fn test_integrity_config_pages_percent_negative() {
        let err = IntegrityConfig::builder().pages_per_cycle_percent(-1.0).build().unwrap_err();
        assert!(err.to_string().contains("pages_per_cycle_percent"));
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
    fn test_metrics_cardinality_config_validate_method() {
        let config = MetricsCardinalityConfig { warn_cardinality: 100, max_cardinality: 200 };
        assert!(config.validate().is_ok());
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
        assert!(props.contains_key("default_quota"), "Missing default_quota");
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
}
