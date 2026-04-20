//! Node and peer configuration for ledger cluster nodes.

use std::{net::SocketAddr, path::PathBuf};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    raft::{BatchConfig, RaftConfig},
    storage::StorageConfig,
};

/// Main configuration for a ledger node.
#[derive(Debug, Clone, bon::Builder, Serialize, Deserialize, JsonSchema)]
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
    /// Number of independent Raft shards to run per region.
    ///
    /// Each shard is its own Raft group with its own WAL, state DBs,
    /// apply worker, and Merkle chain. Orgs route to shards via
    /// `ShardManager::resolve_shard(region, org_id)`; multiple orgs can
    /// share a shard. Per-node throughput scales roughly linearly with
    /// shard count until ApplyWorker CPU saturates. Must be in `[1, 256]`.
    ///
    /// Default is 16 — matches the target configuration for typical
    /// multi-core production hosts. Tune down for constrained test
    /// fixtures or small deployments; tune up on higher-core hosts to
    /// keep apply-pipeline CPU utilization near full.
    #[serde(default = "default_shards_per_region")]
    #[builder(default = default_shards_per_region())]
    pub shards_per_region: usize,
}

fn default_shards_per_region() -> usize {
    16
}

/// Configuration for a peer node.
#[derive(Debug, Clone, PartialEq, Eq, bon::Builder, Serialize, Deserialize, JsonSchema)]
pub struct PeerConfig {
    /// Peer node identifier.
    #[builder(into)]
    pub node_id: String,
    /// Peer gRPC address.
    #[builder(into)]
    pub addr: String,
}
