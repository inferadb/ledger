//! Cluster bootstrap and initialization.
//!
//! Provides functions to:
//! - Bootstrap a new cluster with this node as the initial leader
//! - Join an existing cluster by contacting the leader

use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::storage::Adaptor;
use openraft::{BasicNode, Raft};
use parking_lot::RwLock;
use redb::Database;

use ledger_raft::{
    GrpcRaftNetworkFactory, LedgerNodeId, LedgerServer, LedgerTypeConfig, RaftLogStore,
};
use ledger_storage::StateLayer;

use crate::config::Config;

/// Error type for bootstrap operations.
#[derive(Debug)]
pub enum BootstrapError {
    /// Failed to open database.
    Database(String),
    /// Failed to create Raft storage.
    Storage(String),
    /// Failed to create Raft instance.
    Raft(String),
    /// Failed to initialize cluster.
    Initialize(String),
}

impl std::fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BootstrapError::Database(msg) => write!(f, "database error: {}", msg),
            BootstrapError::Storage(msg) => write!(f, "storage error: {}", msg),
            BootstrapError::Raft(msg) => write!(f, "raft error: {}", msg),
            BootstrapError::Initialize(msg) => write!(f, "initialization error: {}", msg),
        }
    }
}

impl std::error::Error for BootstrapError {}

/// Bootstrapped node components.
pub struct BootstrappedNode {
    /// The Raft instance.
    #[allow(dead_code)]
    pub raft: Arc<Raft<LedgerTypeConfig>>,
    /// The shared state layer.
    #[allow(dead_code)]
    pub state: Arc<RwLock<StateLayer>>,
    /// The configured Ledger server.
    pub server: LedgerServer,
}

/// Bootstrap a new cluster or join an existing one based on configuration.
///
/// If `config.bootstrap` is true, initializes a new single-node cluster.
/// Otherwise, the node starts without initialization (ready to join).
pub async fn bootstrap_node(config: &Config) -> Result<BootstrappedNode, BootstrapError> {
    // Create data directories
    std::fs::create_dir_all(&config.data_dir)
        .map_err(|e| BootstrapError::Database(format!("failed to create data dir: {}", e)))?;

    // Open state database
    let state_db_path = config.data_dir.join("state.redb");
    let state_db = Database::create(&state_db_path)
        .map_err(|e| BootstrapError::Database(format!("failed to create state db: {}", e)))?;
    let state_db = Arc::new(state_db);
    let state = Arc::new(RwLock::new(StateLayer::new(state_db)));

    // Open Raft log store
    let log_path = config.data_dir.join("raft.redb");
    let log_store = RaftLogStore::open(&log_path)
        .map_err(|e| BootstrapError::Storage(format!("failed to open log store: {}", e)))?;

    // Get accessor before log_store is consumed by Adaptor
    let applied_state_accessor = log_store.accessor();

    // Create Raft network factory
    let network = GrpcRaftNetworkFactory::new();

    // Build Raft configuration
    let raft_config = openraft::Config {
        cluster_name: "ledger".to_string(),
        heartbeat_interval: 150,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    };

    // Create adaptor to split RaftStorage into log storage and state machine
    // The Adaptor provides RaftLogStorage and RaftStateMachine from our RaftStorage impl
    // Note: Adaptor takes ownership of the store
    let (log_storage, state_machine) = Adaptor::new(log_store);

    // Create Raft instance
    let raft = Raft::<LedgerTypeConfig>::new(
        config.node_id,
        Arc::new(raft_config),
        network,
        log_storage,
        state_machine,
    )
    .await
    .map_err(|e| BootstrapError::Raft(format!("failed to create raft: {}", e)))?;

    let raft = Arc::new(raft);

    // Bootstrap cluster if this is the initial node
    if config.bootstrap {
        bootstrap_cluster(&raft, config).await?;
    }

    // Create server
    let server = LedgerServer::new(
        raft.clone(),
        state.clone(),
        applied_state_accessor,
        config.listen_addr,
    );

    Ok(BootstrappedNode {
        raft,
        state,
        server,
    })
}

/// Bootstrap a new cluster with this node as the initial member.
///
/// This should only be called once for the first node in a new cluster.
async fn bootstrap_cluster(
    raft: &Raft<LedgerTypeConfig>,
    config: &Config,
) -> Result<(), BootstrapError> {
    // Build initial membership with this node and any configured peers
    let mut members: BTreeMap<LedgerNodeId, BasicNode> = BTreeMap::new();

    // Add self
    members.insert(
        config.node_id,
        BasicNode {
            addr: config.listen_addr.to_string(),
        },
    );

    // Add configured peers
    for peer in &config.peers {
        members.insert(
            peer.node_id,
            BasicNode {
                addr: peer.addr.clone(),
            },
        );
    }

    // Initialize the cluster
    raft.initialize(members)
        .await
        .map_err(|e| BootstrapError::Initialize(format!("failed to initialize: {}", e)))?;

    tracing::info!(
        node_id = config.node_id,
        "Bootstrapped new cluster as initial leader"
    );

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_bootstrap_single_node() {
        let temp_dir = tempdir().expect("create temp dir");
        let config = Config::for_test(1, 50051, temp_dir.path().to_path_buf());

        let result = bootstrap_node(&config).await;
        assert!(
            result.is_ok(),
            "bootstrap should succeed: {:?}",
            result.err()
        );
    }
}
