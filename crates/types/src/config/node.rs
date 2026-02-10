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
