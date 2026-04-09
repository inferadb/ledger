//! Configuration for shards and the engine.

use std::time::Duration;

/// Per-shard configuration.
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// Minimum election timeout.
    pub election_timeout_min: Duration,
    /// Maximum election timeout.
    pub election_timeout_max: Duration,
    /// Heartbeat interval.
    pub heartbeat_interval: Duration,
    /// Entries since last snapshot before triggering.
    pub snapshot_threshold: u64,
    /// Auto-promote learners within this many entries of leader.
    pub auto_promote_threshold: u64,
    /// Maximum entries per AppendEntries RPC.
    pub max_entries_per_rpc: u64,
    /// Max unacknowledged batches per peer.
    pub pipeline_depth: u32,
    /// Whether the shard auto-promotes caught-up learners internally.
    /// Set to false when an external scheduler manages membership.
    pub auto_promote: bool,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: Duration::from_millis(300),
            election_timeout_max: Duration::from_millis(600),
            heartbeat_interval: Duration::from_millis(100),
            snapshot_threshold: 10_000,
            auto_promote_threshold: 100,
            max_entries_per_rpc: 1_000,
            pipeline_depth: 4,
            auto_promote: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_timing_invariants() {
        let c = ShardConfig::default();
        assert!(
            c.heartbeat_interval < c.election_timeout_min,
            "heartbeat must be shorter than min election timeout"
        );
        assert!(
            c.election_timeout_min < c.election_timeout_max,
            "min election timeout must be less than max"
        );
    }

    #[test]
    fn default_config_specific_values() {
        let c = ShardConfig::default();
        assert_eq!(c.election_timeout_min, Duration::from_millis(300));
        assert_eq!(c.election_timeout_max, Duration::from_millis(600));
        assert_eq!(c.heartbeat_interval, Duration::from_millis(100));
        assert_eq!(c.snapshot_threshold, 10_000);
        assert_eq!(c.auto_promote_threshold, 100);
        assert_eq!(c.max_entries_per_rpc, 1_000);
        assert_eq!(c.pipeline_depth, 4);
    }

    #[test]
    fn custom_config_overrides_defaults() {
        let c = ShardConfig {
            election_timeout_min: Duration::from_millis(500),
            election_timeout_max: Duration::from_secs(1),
            heartbeat_interval: Duration::from_millis(200),
            snapshot_threshold: 5_000,
            auto_promote_threshold: 50,
            max_entries_per_rpc: 500,
            pipeline_depth: 8,
            auto_promote: true,
        };
        assert_eq!(c.snapshot_threshold, 5_000);
        assert_eq!(c.pipeline_depth, 8);
        assert_eq!(c.max_entries_per_rpc, 500);
    }
}
