//! Configuration types for InferaDB Ledger.
//!
//! Configuration is loaded from TOML files and environment variables.

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use serde::{Deserialize, Serialize};

/// Main configuration for a ledger node.
#[derive(Debug, Clone, bon::Builder, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Unique identifier for this node.
    #[builder(into)]
    pub node_id: String,
    /// Address to listen for gRPC connections.
    pub listen_addr: SocketAddr,
    /// Directory for persistent data storage.
    #[builder(into)]
    pub data_dir: PathBuf,
    /// Peer nodes in the cluster.
    #[serde(default)]
    #[builder(default)]
    pub peers: Vec<PeerConfig>,
    /// Storage configuration.
    #[serde(default)]
    #[builder(default)]
    pub storage: StorageConfig,
    /// Raft consensus configuration.
    #[serde(default)]
    #[builder(default)]
    pub raft: RaftConfig,
    /// Batching configuration.
    #[serde(default)]
    #[builder(default)]
    pub batching: BatchConfig,
}

/// Configuration for a peer node.
#[derive(Debug, Clone, PartialEq, Eq, bon::Builder, Serialize, Deserialize)]
pub struct PeerConfig {
    /// Peer node identifier.
    #[builder(into)]
    pub node_id: String,
    /// Peer gRPC address.
    #[builder(into)]
    pub addr: String,
}

/// Storage layer configuration.
#[derive(Debug, Clone, PartialEq, Eq, bon::Builder, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Maximum size of the inferadb-ledger-store cache in bytes.
    #[serde(default = "default_cache_size")]
    #[builder(default = default_cache_size())]
    pub cache_size_bytes: usize,
    /// Number of snapshots to keep in hot cache.
    #[serde(default = "default_hot_cache_size")]
    #[builder(default = default_hot_cache_size())]
    pub hot_cache_snapshots: usize,
    /// Interval between automatic snapshots.
    #[serde(default = "default_snapshot_interval")]
    #[serde(with = "humantime_serde")]
    #[builder(default = default_snapshot_interval())]
    pub snapshot_interval: Duration,
    /// Zstd compression level for snapshots (1-22, 3 recommended).
    #[serde(default = "default_compression_level")]
    #[builder(default = default_compression_level())]
    pub compression_level: i32,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            cache_size_bytes: default_cache_size(),
            hot_cache_snapshots: default_hot_cache_size(),
            snapshot_interval: default_snapshot_interval(),
            compression_level: default_compression_level(),
        }
    }
}

/// Raft consensus configuration.
#[derive(Debug, Clone, PartialEq, Eq, bon::Builder, Serialize, Deserialize)]
pub struct RaftConfig {
    /// Heartbeat interval.
    #[serde(default = "default_heartbeat_interval")]
    #[serde(with = "humantime_serde")]
    #[builder(default = default_heartbeat_interval())]
    pub heartbeat_interval: Duration,
    /// Election timeout range (min).
    #[serde(default = "default_election_timeout_min")]
    #[serde(with = "humantime_serde")]
    #[builder(default = default_election_timeout_min())]
    pub election_timeout_min: Duration,
    /// Election timeout range (max).
    #[serde(default = "default_election_timeout_max")]
    #[serde(with = "humantime_serde")]
    #[builder(default = default_election_timeout_max())]
    pub election_timeout_max: Duration,
    /// Maximum entries per append_entries RPC.
    #[serde(default = "default_max_entries_per_rpc")]
    #[builder(default = default_max_entries_per_rpc())]
    pub max_entries_per_rpc: u64,
    /// Snapshot threshold (entries since last snapshot).
    #[serde(default = "default_snapshot_threshold")]
    #[builder(default = default_snapshot_threshold())]
    pub snapshot_threshold: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: default_heartbeat_interval(),
            election_timeout_min: default_election_timeout_min(),
            election_timeout_max: default_election_timeout_max(),
            max_entries_per_rpc: default_max_entries_per_rpc(),
            snapshot_threshold: default_snapshot_threshold(),
        }
    }
}

/// Transaction batching configuration.
#[derive(Debug, Clone, PartialEq, Eq, bon::Builder, Serialize, Deserialize)]
pub struct BatchConfig {
    /// Maximum transactions per batch.
    #[serde(default = "default_max_batch_size")]
    #[builder(default = default_max_batch_size())]
    pub max_batch_size: usize,
    /// Maximum wait time before flushing a partial batch.
    #[serde(default = "default_batch_timeout")]
    #[serde(with = "humantime_serde")]
    #[builder(default = default_batch_timeout())]
    pub batch_timeout: Duration,
    /// Enable batch coalescing for higher throughput.
    #[serde(default = "default_coalesce_enabled")]
    #[builder(default = default_coalesce_enabled())]
    pub coalesce_enabled: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: default_max_batch_size(),
            batch_timeout: default_batch_timeout(),
            coalesce_enabled: default_coalesce_enabled(),
        }
    }
}

// Default value functions
fn default_cache_size() -> usize {
    256 * 1024 * 1024 // 256 MB
}

fn default_hot_cache_size() -> usize {
    3 // Last 3 snapshots
}

fn default_snapshot_interval() -> Duration {
    Duration::from_secs(300) // 5 minutes
}

fn default_compression_level() -> i32 {
    3 // Good balance of speed/ratio
}

fn default_heartbeat_interval() -> Duration {
    Duration::from_millis(100)
}

fn default_election_timeout_min() -> Duration {
    Duration::from_millis(300)
}

fn default_election_timeout_max() -> Duration {
    Duration::from_millis(500)
}

fn default_max_entries_per_rpc() -> u64 {
    100
}

fn default_snapshot_threshold() -> u64 {
    10_000
}

fn default_max_batch_size() -> usize {
    100
}

fn default_batch_timeout() -> Duration {
    Duration::from_millis(5)
}

fn default_coalesce_enabled() -> bool {
    true
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
    use super::*;

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
    fn test_storage_config_builder() {
        // Builder with all defaults
        let storage = StorageConfig::builder().build();
        assert_eq!(storage.cache_size_bytes, default_cache_size());
        assert_eq!(storage.hot_cache_snapshots, default_hot_cache_size());
        assert_eq!(storage.snapshot_interval, default_snapshot_interval());
        assert_eq!(storage.compression_level, default_compression_level());

        // Builder with custom values
        let custom_storage = StorageConfig::builder()
            .cache_size_bytes(1024)
            .hot_cache_snapshots(5)
            .snapshot_interval(Duration::from_secs(600))
            .compression_level(10)
            .build();
        assert_eq!(custom_storage.cache_size_bytes, 1024);
        assert_eq!(custom_storage.hot_cache_snapshots, 5);
        assert_eq!(custom_storage.snapshot_interval, Duration::from_secs(600));
        assert_eq!(custom_storage.compression_level, 10);
    }

    #[test]
    fn test_raft_config_builder() {
        // Builder with all defaults
        let raft = RaftConfig::builder().build();
        assert_eq!(raft.heartbeat_interval, default_heartbeat_interval());
        assert_eq!(raft.election_timeout_min, default_election_timeout_min());
        assert_eq!(raft.election_timeout_max, default_election_timeout_max());
        assert_eq!(raft.max_entries_per_rpc, default_max_entries_per_rpc());
        assert_eq!(raft.snapshot_threshold, default_snapshot_threshold());

        // Builder with custom values
        let custom_raft = RaftConfig::builder()
            .heartbeat_interval(Duration::from_millis(200))
            .election_timeout_min(Duration::from_millis(500))
            .election_timeout_max(Duration::from_millis(1000))
            .max_entries_per_rpc(50)
            .snapshot_threshold(5000)
            .build();
        assert_eq!(custom_raft.heartbeat_interval, Duration::from_millis(200));
        assert_eq!(custom_raft.election_timeout_min, Duration::from_millis(500));
        assert_eq!(custom_raft.election_timeout_max, Duration::from_millis(1000));
        assert_eq!(custom_raft.max_entries_per_rpc, 50);
        assert_eq!(custom_raft.snapshot_threshold, 5000);
    }

    #[test]
    fn test_batch_config_builder() {
        // Builder with all defaults
        let batch = BatchConfig::builder().build();
        assert_eq!(batch.max_batch_size, default_max_batch_size());
        assert_eq!(batch.batch_timeout, default_batch_timeout());
        assert_eq!(batch.coalesce_enabled, default_coalesce_enabled());

        // Builder with custom values
        let custom_batch = BatchConfig::builder()
            .max_batch_size(50)
            .batch_timeout(Duration::from_millis(10))
            .coalesce_enabled(false)
            .build();
        assert_eq!(custom_batch.max_batch_size, 50);
        assert_eq!(custom_batch.batch_timeout, Duration::from_millis(10));
        assert!(!custom_batch.coalesce_enabled);
    }

    #[test]
    fn test_peer_config_builder() {
        let peer = PeerConfig::builder().node_id("node-2").addr("127.0.0.1:50052").build();
        assert_eq!(peer.node_id, "node-2");
        assert_eq!(peer.addr, "127.0.0.1:50052");
    }

    #[test]
    fn test_node_config_builder_nested() {
        // Test nested builder composition
        let addr: SocketAddr = "127.0.0.1:50051".parse().unwrap();
        let config = NodeConfig::builder()
            .node_id("node-1")
            .listen_addr(addr)
            .data_dir("/tmp/ledger")
            .peers(vec![PeerConfig::builder().node_id("node-2").addr("127.0.0.1:50052").build()])
            .storage(StorageConfig::builder().cache_size_bytes(1024 * 1024).build())
            .raft(RaftConfig::builder().heartbeat_interval(Duration::from_millis(200)).build())
            .batching(BatchConfig::builder().max_batch_size(50).build())
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
        // Test that optional nested configs default properly
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
    fn test_builder_matches_default() {
        // Verify builder().build() produces same result as Default::default()
        assert_eq!(StorageConfig::builder().build(), StorageConfig::default());
        assert_eq!(RaftConfig::builder().build(), RaftConfig::default());
        assert_eq!(BatchConfig::builder().build(), BatchConfig::default());
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
                    .build(),
            )
            .raft(
                RaftConfig::builder()
                    .heartbeat_interval(Duration::from_millis(150))
                    .max_entries_per_rpc(200)
                    .build(),
            )
            .batching(BatchConfig::builder().max_batch_size(200).coalesce_enabled(false).build())
            .build();

        // Serialize to JSON and back
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
}
