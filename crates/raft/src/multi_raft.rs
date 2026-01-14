//! Multi-Raft Manager for coordinating multiple independent shard groups.
//!
//! Per DESIGN.md §4.6: InferaDB uses a shard-per-Raft-group architecture where
//! each shard is an independent Raft consensus group. The `_system` shard handles
//! global coordination, while data shards handle namespace workloads.
//!
//! ## Architecture
//!
//! ```text
//! MultiRaftManager
//! ├── _system shard (ShardGroup 0)
//! │   ├── Raft instance
//! │   ├── RaftLogStore + StateLayer
//! │   ├── BlockArchive
//! │   └── Background jobs
//! ├── data shard 1 (ShardGroup 1)
//! │   └── ... (same structure)
//! └── data shard N (ShardGroup N)
//!     └── ...
//! ```
//!
//! ## Shard Isolation
//!
//! Each shard group is fully isolated:
//! - Separate Raft consensus (independent elections, log replication)
//! - Separate storage files (state.inkwell, blocks.inkwell, raft.inkwell per shard)
//! - Separate background jobs (GC, compaction, recovery)
//!
//! ## Usage
//!
//! ```ignore
//! let manager = MultiRaftManager::new(config);
//!
//! // Start the _system shard (always required)
//! manager.start_system_shard().await?;
//!
//! // Start data shards as needed
//! manager.start_shard(1, shard_config).await?;
//!
//! // Route requests
//! let shard = manager.get_shard(shard_id)?;
//! shard.raft().client_write(request).await?;
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::storage::Adaptor;
use openraft::{BasicNode, Raft};
use parking_lot::RwLock;
use snafu::Snafu;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use inkwell::{Database, FileBackend};
use ledger_storage::system::SystemNamespaceService;
use ledger_storage::{BlockArchive, StateLayer};
use ledger_types::{NamespaceId, ShardId};

use crate::auto_recovery::AutoRecoveryJob;
use crate::block_compaction::BlockCompactor;
use crate::log_storage::{AppliedStateAccessor, RaftLogStore};
use crate::raft_network::GrpcRaftNetworkFactory;
use crate::shard_router::ShardRouter;
use crate::ttl_gc::TtlGarbageCollector;
use crate::types::{LedgerNodeId, LedgerTypeConfig};

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur during multi-raft operations.
#[allow(missing_docs)]
#[derive(Debug, Snafu)]
pub enum MultiRaftError {
    /// Shard already exists.
    #[snafu(display("Shard {shard_id} already exists"))]
    ShardExists { shard_id: ShardId },

    /// Shard not found.
    #[snafu(display("Shard {shard_id} not found"))]
    ShardNotFound { shard_id: ShardId },

    /// Failed to open storage.
    #[snafu(display("Storage error for shard {shard_id}: {message}"))]
    Storage { shard_id: ShardId, message: String },

    /// Failed to create Raft instance.
    #[snafu(display("Raft error for shard {shard_id}: {message}"))]
    Raft { shard_id: ShardId, message: String },

    /// System shard not initialized.
    #[snafu(display("System shard (_system) must be started first"))]
    SystemShardRequired,
}

/// Result type for multi-raft operations.
pub type Result<T> = std::result::Result<T, MultiRaftError>;

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the Multi-Raft Manager.
#[derive(Debug, Clone)]
pub struct MultiRaftConfig {
    /// Base data directory (shards stored in subdirectories).
    pub data_dir: PathBuf,
    /// This node's ID.
    pub node_id: LedgerNodeId,
    /// Raft heartbeat interval in milliseconds.
    pub heartbeat_interval_ms: u64,
    /// Minimum election timeout in milliseconds.
    pub election_timeout_min_ms: u64,
    /// Maximum election timeout in milliseconds.
    pub election_timeout_max_ms: u64,
}

impl MultiRaftConfig {
    /// Create a new configuration.
    pub fn new(data_dir: PathBuf, node_id: LedgerNodeId) -> Self {
        Self {
            data_dir,
            node_id,
            heartbeat_interval_ms: 150,
            election_timeout_min_ms: 300,
            election_timeout_max_ms: 600,
        }
    }

    /// Get the data directory for a specific shard.
    pub fn shard_dir(&self, shard_id: ShardId) -> PathBuf {
        if shard_id == 0 {
            self.data_dir.join("shards").join("_system")
        } else {
            self.data_dir
                .join("shards")
                .join(format!("shard_{:04}", shard_id))
        }
    }
}

/// Configuration for a single shard.
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// Shard identifier.
    pub shard_id: ShardId,
    /// Initial cluster members (node_id -> address).
    pub initial_members: Vec<(LedgerNodeId, String)>,
    /// Whether to bootstrap this shard as a new cluster.
    pub bootstrap: bool,
    /// Whether to start background jobs (GC, compactor).
    pub enable_background_jobs: bool,
}

impl ShardConfig {
    /// Create configuration for the system shard.
    pub fn system(node_id: LedgerNodeId, address: String) -> Self {
        Self {
            shard_id: 0,
            initial_members: vec![(node_id, address)],
            bootstrap: true,
            enable_background_jobs: true,
        }
    }

    /// Create configuration for a data shard.
    pub fn data(shard_id: ShardId, initial_members: Vec<(LedgerNodeId, String)>) -> Self {
        Self {
            shard_id,
            initial_members,
            bootstrap: true,
            enable_background_jobs: true,
        }
    }

    /// Disable background jobs (useful for testing).
    pub fn without_background_jobs(mut self) -> Self {
        self.enable_background_jobs = false;
        self
    }
}

// ============================================================================
// Shard Group
// ============================================================================

/// Background job handles for a shard.
pub struct ShardBackgroundJobs {
    /// TTL garbage collector handle.
    gc_handle: Option<JoinHandle<()>>,
    /// Block compactor handle.
    compactor_handle: Option<JoinHandle<()>>,
    /// Auto-recovery job handle.
    recovery_handle: Option<JoinHandle<()>>,
}

impl ShardBackgroundJobs {
    /// Create with no jobs (used when background_jobs disabled).
    fn none() -> Self {
        Self {
            gc_handle: None,
            compactor_handle: None,
            recovery_handle: None,
        }
    }

    /// Abort all running background jobs.
    fn abort(&mut self) {
        if let Some(handle) = self.gc_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.compactor_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.recovery_handle.take() {
            handle.abort();
        }
    }
}

/// A single shard group with its own Raft instance and storage.
///
/// Each ShardGroup is a complete, isolated Raft cluster member for one shard.
/// Uses FileBackend for production storage - this is intentional as Raft requires
/// durable storage for safety.
pub struct ShardGroup {
    /// Shard identifier.
    shard_id: ShardId,
    /// The Raft consensus instance.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// Shared state layer for this shard.
    state: Arc<StateLayer<FileBackend>>,
    /// Block archive for historical blocks.
    block_archive: Arc<BlockArchive<FileBackend>>,
    /// Accessor for applied state.
    applied_state: AppliedStateAccessor,
    /// Background job handles (wrapped in Mutex for mutable access through Arc).
    background_jobs: parking_lot::Mutex<ShardBackgroundJobs>,
}

impl ShardGroup {
    /// Get the shard ID.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Get the Raft instance.
    pub fn raft(&self) -> &Arc<Raft<LedgerTypeConfig>> {
        &self.raft
    }

    /// Get the state layer.
    pub fn state(&self) -> &Arc<StateLayer<FileBackend>> {
        &self.state
    }

    /// Get the block archive.
    pub fn block_archive(&self) -> &Arc<BlockArchive<FileBackend>> {
        &self.block_archive
    }

    /// Get the applied state accessor.
    pub fn applied_state(&self) -> &AppliedStateAccessor {
        &self.applied_state
    }

    /// Check if this node is the leader for this shard.
    pub fn is_leader(&self, node_id: LedgerNodeId) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(node_id)
    }

    /// Get the current leader node ID, if known.
    pub fn current_leader(&self) -> Option<LedgerNodeId> {
        self.raft.metrics().borrow().current_leader
    }
}

// ============================================================================
// Multi-Raft Manager
// ============================================================================

/// Manager for multiple Raft shard groups.
///
/// Coordinates the lifecycle of multiple independent Raft consensus groups,
/// each handling a subset of namespaces. Uses FileBackend for production storage.
pub struct MultiRaftManager {
    /// Configuration.
    config: MultiRaftConfig,
    /// Active shard groups indexed by shard ID.
    shards: RwLock<HashMap<ShardId, Arc<ShardGroup>>>,
    /// Router for namespace-to-shard resolution.
    router: RwLock<Option<Arc<ShardRouter<FileBackend>>>>,
}

impl MultiRaftManager {
    /// Create a new Multi-Raft Manager.
    pub fn new(config: MultiRaftConfig) -> Self {
        Self {
            config,
            shards: RwLock::new(HashMap::new()),
            router: RwLock::new(None),
        }
    }

    /// Get the configuration.
    pub fn config(&self) -> &MultiRaftConfig {
        &self.config
    }

    /// Get a shard group by ID.
    pub fn get_shard(&self, shard_id: ShardId) -> Result<Arc<ShardGroup>> {
        self.shards
            .read()
            .get(&shard_id)
            .cloned()
            .ok_or(MultiRaftError::ShardNotFound { shard_id })
    }

    /// Get the system shard (_system).
    pub fn system_shard(&self) -> Result<Arc<ShardGroup>> {
        self.get_shard(0)
    }

    /// List all active shard IDs.
    pub fn list_shards(&self) -> Vec<ShardId> {
        self.shards.read().keys().copied().collect()
    }

    /// Check if a shard is active.
    pub fn has_shard(&self, shard_id: ShardId) -> bool {
        self.shards.read().contains_key(&shard_id)
    }

    /// Get the shard router (if initialized).
    pub fn router(&self) -> Option<Arc<ShardRouter<FileBackend>>> {
        self.router.read().clone()
    }

    /// Route a namespace to its shard group.
    ///
    /// Uses the ShardRouter to look up the namespace's shard assignment,
    /// then returns the local ShardGroup if available.
    ///
    /// Returns `None` if:
    /// - Router not initialized (system shard not started)
    /// - Namespace not found in routing table
    /// - Shard is on a different node (requires forwarding)
    pub fn route_namespace(&self, namespace_id: NamespaceId) -> Option<Arc<ShardGroup>> {
        let router = self.router.read().clone()?;

        // Look up shard assignment
        let routing = router.get_routing(namespace_id).ok()?;

        // Get local shard group (if we host this shard)
        self.shards.read().get(&routing.shard_id).cloned()
    }

    /// Get the shard ID for a namespace.
    ///
    /// Looks up the namespace's shard assignment without checking
    /// if the shard is locally available.
    pub fn get_namespace_shard(&self, namespace_id: NamespaceId) -> Option<ShardId> {
        let router = self.router.read().clone()?;
        router.get_routing(namespace_id).ok().map(|r| r.shard_id)
    }

    /// Start the system shard (_system).
    ///
    /// The system shard must be started before any data shards.
    /// It stores the namespace routing table and cluster metadata.
    /// Also initializes the ShardRouter for namespace-to-shard routing.
    pub async fn start_system_shard(&self, shard_config: ShardConfig) -> Result<Arc<ShardGroup>> {
        if shard_config.shard_id != 0 {
            return Err(MultiRaftError::Raft {
                shard_id: shard_config.shard_id,
                message: "System shard must have shard_id=0".to_string(),
            });
        }

        let shard = self.start_shard(shard_config).await?;

        // Initialize the ShardRouter with access to _system's state
        let system_service = Arc::new(SystemNamespaceService::new(shard.state.clone()));
        let router = Arc::new(ShardRouter::new(system_service));
        *self.router.write() = Some(router);

        info!("ShardRouter initialized with _system namespace");

        Ok(shard)
    }

    /// Start a data shard.
    ///
    /// Requires the system shard to be started first.
    pub async fn start_data_shard(&self, shard_config: ShardConfig) -> Result<Arc<ShardGroup>> {
        // Verify system shard is running
        if !self.has_shard(0) {
            return Err(MultiRaftError::SystemShardRequired);
        }

        if shard_config.shard_id == 0 {
            return Err(MultiRaftError::Raft {
                shard_id: 0,
                message: "Use start_system_shard for shard_id=0".to_string(),
            });
        }

        self.start_shard(shard_config).await
    }

    /// Start a shard group.
    async fn start_shard(&self, shard_config: ShardConfig) -> Result<Arc<ShardGroup>> {
        let shard_id = shard_config.shard_id;

        // Check if shard already exists
        if self.has_shard(shard_id) {
            return Err(MultiRaftError::ShardExists { shard_id });
        }

        info!(shard_id, "Starting shard group");

        // Create shard directory
        let shard_dir = self.config.shard_dir(shard_id);
        std::fs::create_dir_all(&shard_dir).map_err(|e| MultiRaftError::Storage {
            shard_id,
            message: format!("Failed to create shard directory: {}", e),
        })?;

        // Open storage
        let (state, block_archive, log_store) = self.open_shard_storage(shard_id, &shard_dir)?;

        // Get accessor before log_store is consumed
        let applied_state = log_store.accessor();

        // Create network factory
        let network = GrpcRaftNetworkFactory::new();

        // Build Raft config
        let raft_config = openraft::Config {
            cluster_name: format!("ledger-shard-{}", shard_id),
            heartbeat_interval: self.config.heartbeat_interval_ms,
            election_timeout_min: self.config.election_timeout_min_ms,
            election_timeout_max: self.config.election_timeout_max_ms,
            ..Default::default()
        };

        // Create adaptor
        let (log_storage, state_machine) = Adaptor::new(log_store);

        // Create Raft instance
        let raft = Raft::<LedgerTypeConfig>::new(
            self.config.node_id,
            Arc::new(raft_config),
            network,
            log_storage,
            state_machine,
        )
        .await
        .map_err(|e| MultiRaftError::Raft {
            shard_id,
            message: format!("Failed to create Raft instance: {}", e),
        })?;

        let raft = Arc::new(raft);

        // Bootstrap if configured
        if shard_config.bootstrap && !shard_config.initial_members.is_empty() {
            self.bootstrap_shard(&raft, &shard_config).await?;
        }

        // Start background jobs if enabled
        let background_jobs = if shard_config.enable_background_jobs {
            self.start_background_jobs(
                shard_id,
                raft.clone(),
                state.clone(),
                block_archive.clone(),
                applied_state.clone(),
            )
        } else {
            ShardBackgroundJobs::none()
        };

        // Create shard group
        let shard_group = Arc::new(ShardGroup {
            shard_id,
            raft,
            state,
            block_archive,
            applied_state,
            background_jobs: parking_lot::Mutex::new(background_jobs),
        });

        // Register shard
        {
            let mut shards = self.shards.write();
            shards.insert(shard_id, shard_group.clone());
        }

        info!(shard_id, "Shard group started successfully");

        Ok(shard_group)
    }

    /// Start background jobs for a shard.
    fn start_background_jobs(
        &self,
        shard_id: ShardId,
        raft: Arc<Raft<LedgerTypeConfig>>,
        state: Arc<StateLayer<FileBackend>>,
        block_archive: Arc<BlockArchive<FileBackend>>,
        applied_state: AppliedStateAccessor,
    ) -> ShardBackgroundJobs {
        info!(shard_id, "Starting background jobs for shard");

        // TTL Garbage Collector
        let gc = TtlGarbageCollector::new(
            raft.clone(),
            self.config.node_id,
            state.clone(),
            applied_state.clone(),
        );
        let gc_handle = gc.start();
        info!(shard_id, "Started TTL garbage collector");

        // Block Compactor
        let compactor = BlockCompactor::new(
            raft.clone(),
            self.config.node_id,
            block_archive.clone(),
            applied_state.clone(),
        );
        let compactor_handle = compactor.start();
        info!(shard_id, "Started block compactor");

        // Auto Recovery Job
        let recovery = AutoRecoveryJob::new(raft, self.config.node_id, applied_state, state)
            .with_block_archive(block_archive);
        let recovery_handle = recovery.start();
        info!(shard_id, "Started auto recovery job");

        ShardBackgroundJobs {
            gc_handle: Some(gc_handle),
            compactor_handle: Some(compactor_handle),
            recovery_handle: Some(recovery_handle),
        }
    }

    /// Open storage for a shard.
    fn open_shard_storage(
        &self,
        shard_id: ShardId,
        shard_dir: &PathBuf,
    ) -> Result<(
        Arc<StateLayer<FileBackend>>,
        Arc<BlockArchive<FileBackend>>,
        RaftLogStore<FileBackend>,
    )> {
        // Open or create state database using inkwell
        let state_db_path = shard_dir.join("state.inkwell");
        let state_db = if state_db_path.exists() {
            Database::<FileBackend>::open(&state_db_path)
        } else {
            Database::<FileBackend>::create(&state_db_path)
        }
        .map_err(|e| MultiRaftError::Storage {
            shard_id,
            message: format!("Failed to open state db: {}", e),
        })?;
        let state = Arc::new(StateLayer::new(Arc::new(state_db)));

        // Open or create block archive database using inkwell
        let blocks_db_path = shard_dir.join("blocks.inkwell");
        let blocks_db = if blocks_db_path.exists() {
            Database::<FileBackend>::open(&blocks_db_path)
        } else {
            Database::<FileBackend>::create(&blocks_db_path)
        }
        .map_err(|e| MultiRaftError::Storage {
            shard_id,
            message: format!("Failed to open blocks db: {}", e),
        })?;
        let block_archive = Arc::new(BlockArchive::new(Arc::new(blocks_db)));

        // Open Raft log store (uses inkwell storage - handles open/create internally)
        let log_path = shard_dir.join("raft.inkwell");
        let log_store = RaftLogStore::<FileBackend>::open(&log_path)
            .map_err(|e| MultiRaftError::Storage {
                shard_id,
                message: format!("Failed to open log store: {}", e),
            })?
            .with_state_layer(state.clone())
            .with_block_archive(block_archive.clone())
            .with_shard_config(shard_id, self.config.node_id.to_string());

        Ok((state, block_archive, log_store))
    }

    /// Bootstrap a shard as a new cluster.
    async fn bootstrap_shard(
        &self,
        raft: &Raft<LedgerTypeConfig>,
        config: &ShardConfig,
    ) -> Result<()> {
        use std::collections::BTreeMap;

        let mut members: BTreeMap<LedgerNodeId, BasicNode> = BTreeMap::new();
        for (node_id, addr) in &config.initial_members {
            members.insert(*node_id, BasicNode { addr: addr.clone() });
        }

        raft.initialize(members)
            .await
            .map_err(|e| MultiRaftError::Raft {
                shard_id: config.shard_id,
                message: format!("Failed to initialize: {}", e),
            })?;

        info!(
            shard_id = config.shard_id,
            members = config.initial_members.len(),
            "Bootstrapped shard cluster"
        );

        Ok(())
    }

    /// Stop a shard group.
    ///
    /// This gracefully shuts down the shard, stopping background jobs
    /// and removing it from the manager.
    pub async fn stop_shard(&self, shard_id: ShardId) -> Result<()> {
        let shard = {
            let mut shards = self.shards.write();
            shards
                .remove(&shard_id)
                .ok_or(MultiRaftError::ShardNotFound { shard_id })?
        };

        // Abort background jobs first
        {
            let mut jobs = shard.background_jobs.lock();
            jobs.abort();
            debug!(shard_id, "Aborted background jobs");
        }

        // Trigger Raft shutdown
        if let Err(e) = shard.raft.shutdown().await {
            warn!(shard_id, error = ?e, "Error during Raft shutdown");
        }

        info!(shard_id, "Shard group stopped");
        Ok(())
    }

    /// Stop all shard groups.
    pub async fn shutdown(&self) {
        let shard_ids: Vec<ShardId> = self.list_shards();

        for shard_id in shard_ids {
            if let Err(e) = self.stop_shard(shard_id).await {
                warn!(shard_id, error = %e, "Error stopping shard during shutdown");
            }
        }

        info!("Multi-Raft Manager shutdown complete");
    }

    /// Get statistics about the manager.
    pub fn stats(&self) -> MultiRaftStats {
        let shards = self.shards.read();
        let mut leader_count = 0;

        for shard in shards.values() {
            if shard.is_leader(self.config.node_id) {
                leader_count += 1;
            }
        }

        MultiRaftStats {
            total_shards: shards.len(),
            leader_shards: leader_count,
            node_id: self.config.node_id,
        }
    }
}

/// Statistics about the Multi-Raft Manager.
#[derive(Debug, Clone)]
pub struct MultiRaftStats {
    /// Total number of active shards.
    pub total_shards: usize,
    /// Number of shards where this node is leader.
    pub leader_shards: usize,
    /// This node's ID.
    pub node_id: LedgerNodeId,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_config(temp_dir: &TempDir) -> MultiRaftConfig {
        MultiRaftConfig::new(temp_dir.path().to_path_buf(), 1)
    }

    #[test]
    fn test_config_shard_dir() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_config(&temp);

        // System shard directory
        let system_dir = config.shard_dir(0);
        assert!(system_dir.ends_with("shards/_system"));

        // Data shard directory
        let data_dir = config.shard_dir(1);
        assert!(data_dir.ends_with("shards/shard_0001"));

        let data_dir = config.shard_dir(42);
        assert!(data_dir.ends_with("shards/shard_0042"));
    }

    #[test]
    fn test_shard_config_system() {
        let config = ShardConfig::system(1, "127.0.0.1:50051".to_string());
        assert_eq!(config.shard_id, 0);
        assert!(config.bootstrap);
        assert_eq!(config.initial_members.len(), 1);
    }

    #[test]
    fn test_shard_config_data() {
        let members = vec![
            (1, "127.0.0.1:50051".to_string()),
            (2, "127.0.0.1:50052".to_string()),
        ];
        let config = ShardConfig::data(1, members);
        assert_eq!(config.shard_id, 1);
        assert!(config.bootstrap);
        assert_eq!(config.initial_members.len(), 2);
    }

    #[test]
    fn test_manager_creation() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        assert_eq!(manager.list_shards().len(), 0);
        assert!(!manager.has_shard(0));
    }

    #[test]
    fn test_manager_stats_empty() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        let stats = manager.stats();
        assert_eq!(stats.total_shards, 0);
        assert_eq!(stats.leader_shards, 0);
        assert_eq!(stats.node_id, 1);
    }

    #[tokio::test]
    async fn test_system_shard_required() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Try to start data shard without system shard
        let shard_config = ShardConfig::data(1, vec![(1, "127.0.0.1:50051".to_string())]);
        let result = manager.start_data_shard(shard_config).await;

        assert!(matches!(result, Err(MultiRaftError::SystemShardRequired)));
    }

    #[tokio::test]
    async fn test_start_system_shard() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        let shard_config = ShardConfig::system(1, "127.0.0.1:50051".to_string());
        let result = manager.start_system_shard(shard_config).await;

        assert!(
            result.is_ok(),
            "start_system_shard failed: {:?}",
            result.err()
        );

        let shard = result.unwrap();
        assert_eq!(shard.shard_id(), 0);
        assert!(manager.has_shard(0));
        assert_eq!(manager.list_shards(), vec![0]);
    }

    #[tokio::test]
    async fn test_start_multiple_shards() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Start system shard
        let system_config = ShardConfig::system(1, "127.0.0.1:50051".to_string());
        manager
            .start_system_shard(system_config)
            .await
            .expect("start system");

        // Start data shard
        let data_config = ShardConfig::data(1, vec![(1, "127.0.0.1:50051".to_string())]);
        manager
            .start_data_shard(data_config)
            .await
            .expect("start data shard");

        assert_eq!(manager.list_shards().len(), 2);
        assert!(manager.has_shard(0));
        assert!(manager.has_shard(1));
    }

    #[tokio::test]
    async fn test_duplicate_shard_error() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Start system shard
        let shard_config = ShardConfig::system(1, "127.0.0.1:50051".to_string());
        manager
            .start_system_shard(shard_config.clone())
            .await
            .expect("start system");

        // Try to start again
        let result = manager.start_system_shard(shard_config).await;
        assert!(matches!(
            result,
            Err(MultiRaftError::ShardExists { shard_id: 0 })
        ));
    }

    #[tokio::test]
    async fn test_stop_shard() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Start system shard
        let shard_config = ShardConfig::system(1, "127.0.0.1:50051".to_string());
        manager
            .start_system_shard(shard_config)
            .await
            .expect("start system");

        assert!(manager.has_shard(0));

        // Stop shard
        manager.stop_shard(0).await.expect("stop shard");

        assert!(!manager.has_shard(0));
    }

    #[tokio::test]
    async fn test_get_shard() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Try to get non-existent shard
        let result = manager.get_shard(0);
        assert!(matches!(
            result,
            Err(MultiRaftError::ShardNotFound { shard_id: 0 })
        ));

        // Start and get
        let shard_config = ShardConfig::system(1, "127.0.0.1:50051".to_string());
        manager
            .start_system_shard(shard_config)
            .await
            .expect("start system");

        let shard = manager.get_shard(0).expect("get shard");
        assert_eq!(shard.shard_id(), 0);

        // system_shard() should work too
        let system = manager.system_shard().expect("system shard");
        assert_eq!(system.shard_id(), 0);
    }

    #[test]
    fn test_background_jobs_none() {
        let jobs = ShardBackgroundJobs::none();
        // All handles should be None
        assert!(jobs.gc_handle.is_none());
        assert!(jobs.compactor_handle.is_none());
        assert!(jobs.recovery_handle.is_none());
    }

    #[test]
    fn test_background_jobs_abort_empty() {
        let mut jobs = ShardBackgroundJobs::none();
        // Aborting empty jobs shouldn't panic
        jobs.abort();
        assert!(jobs.gc_handle.is_none());
        assert!(jobs.compactor_handle.is_none());
        assert!(jobs.recovery_handle.is_none());
    }

    #[tokio::test]
    async fn test_shard_with_background_jobs_disabled() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Create config with background jobs disabled
        let mut shard_config = ShardConfig::system(1, "127.0.0.1:50051".to_string());
        shard_config.enable_background_jobs = false;

        manager
            .start_system_shard(shard_config)
            .await
            .expect("start system");

        let shard = manager.get_shard(0).expect("get shard");

        // Background jobs should be None when disabled
        let jobs = shard.background_jobs.lock();
        assert!(jobs.gc_handle.is_none());
        assert!(jobs.compactor_handle.is_none());
        assert!(jobs.recovery_handle.is_none());
    }

    #[tokio::test]
    async fn test_shard_with_background_jobs_enabled() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Create config with background jobs enabled (default)
        let shard_config = ShardConfig::system(1, "127.0.0.1:50051".to_string());
        assert!(shard_config.enable_background_jobs); // Verify default is true

        manager
            .start_system_shard(shard_config)
            .await
            .expect("start system");

        let shard = manager.get_shard(0).expect("get shard");

        // Background jobs should be Some when enabled
        let jobs = shard.background_jobs.lock();
        assert!(jobs.gc_handle.is_some(), "GC job should be started");
        assert!(
            jobs.compactor_handle.is_some(),
            "Compactor job should be started"
        );
        assert!(
            jobs.recovery_handle.is_some(),
            "Recovery job should be started"
        );
    }

    #[tokio::test]
    async fn test_stop_shard_aborts_background_jobs() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Start with background jobs enabled
        let shard_config = ShardConfig::system(1, "127.0.0.1:50051".to_string());
        manager
            .start_system_shard(shard_config)
            .await
            .expect("start system");

        // Verify jobs are running before stop
        {
            let shard = manager.get_shard(0).expect("get shard");
            let jobs = shard.background_jobs.lock();
            assert!(jobs.gc_handle.is_some());
            assert!(jobs.compactor_handle.is_some());
            assert!(jobs.recovery_handle.is_some());
        }

        // Stop shard - should abort jobs
        manager.stop_shard(0).await.expect("stop shard");

        // Shard should be removed
        assert!(!manager.has_shard(0));
    }
}
