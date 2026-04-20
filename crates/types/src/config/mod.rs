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
    use std::{net::SocketAddr, time::Duration};

    use super::*;

    // =========================================================================
    // Node config and integration tests
    // (StorageConfig, RaftConfig, BatchConfig, PostErasureCompactionConfig
    //  are tested in their respective sub-module files.)
    // =========================================================================

    #[test]
    fn node_config_builder_uses_sub_config_defaults() {
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
    fn node_config_serde_roundtrip() {
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
            .batching(BatchConfig::builder().max_batch_size(200).build().expect("valid"))
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
    // RuntimeConfig cross-module integration tests
    // (Individual config type tests live in their respective sub-module files.)
    // =========================================================================

    #[test]
    fn runtime_config_rejects_invalid_integrity() {
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
    fn runtime_config_integrity_diff_detects_addition() {
        let a = RuntimeConfig::default();
        let b = RuntimeConfig {
            integrity: Some(IntegrityConfig::default()),
            ..RuntimeConfig::default()
        };
        let changes = a.diff(&b);
        assert!(changes.contains(&"integrity".to_string()));
    }

    #[test]
    fn runtime_config_rejects_invalid_metrics_cardinality() {
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
    fn runtime_config_metrics_cardinality_diff_detects_addition() {
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
    fn detailed_diff_empty_when_identical() {
        let a = RuntimeConfig::default();
        let b = RuntimeConfig::default();
        let changes = a.detailed_diff(&b);
        assert!(changes.is_empty());
    }

    #[test]
    fn detailed_diff_detects_nested_field_changes() {
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
    fn detailed_diff_detects_added_optional_section() {
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
    fn detailed_diff_detects_removed_optional_section() {
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
    fn detailed_diff_multiple_sections_changed() {
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
    fn config_change_display() {
        let change = ConfigChange {
            field: "rate_limit.client_burst".to_string(),
            old: "100".to_string(),
            new: "200".to_string(),
        };
        let display = change.to_string();
        assert_eq!(display, "rate_limit.client_burst: 100 → 200");
    }

    // =========================================================================
    // JsonSchema tests
    // =========================================================================

    #[test]
    fn runtime_config_json_schema_has_all_sections() {
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
    fn rate_limit_config_json_schema_has_fields() {
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
    fn region_retention_days_gdpr_vs_non_gdpr() {
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
    fn jwt_config_rejects_empty_issuer() {
        let result = JwtConfig::builder().issuer("").build();
        assert!(result.is_err());
    }

    #[test]
    fn jwt_config_rejects_zero_ttls() {
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
    fn jwt_config_refresh_must_exceed_access() {
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
    fn jwt_config_family_lifetime_must_be_at_least_refresh_ttl() {
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
    fn jwt_config_clock_skew_must_be_less_than_min_access_ttl() {
        // clock_skew >= min(session_access, vault_access)
        let result = JwtConfig::builder()
            .vault_access_ttl_secs(30)
            .vault_refresh_ttl_secs(3600)
            .clock_skew_secs(30)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn jwt_config_serde_defaults() {
        let config: JwtConfig = serde_json::from_str("{}").unwrap();
        config.validate().expect("serde defaults should be valid");
        assert_eq!(config.issuer, "inferadb");
        assert_eq!(config.session_access_ttl_secs, 1800);
    }

}
