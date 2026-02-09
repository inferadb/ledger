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
//! - Separate storage files (state.db, blocks.db, raft.db per shard)
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

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use inferadb_ledger_state::{BlockArchive, StateLayer, system::SystemNamespaceService};
use inferadb_ledger_store::{Database, DatabaseConfig, FileBackend};
use inferadb_ledger_types::{NamespaceId, ShardId};
use openraft::{BasicNode, Raft, storage::Adaptor};
use parking_lot::RwLock;
use snafu::Snafu;
use tokio::{sync::broadcast, task::JoinHandle};
use tracing::{debug, info, warn};

use crate::{
    auto_recovery::AutoRecoveryJob,
    block_compaction::BlockCompactor,
    btree_compaction::BTreeCompactor,
    integrity_scrubber::IntegrityScrubberJob,
    log_storage::{AppliedStateAccessor, RaftLogStore},
    proto::BlockAnnouncement,
    raft_network::GrpcRaftNetworkFactory,
    shard_router::ShardRouter,
    ttl_gc::TtlGarbageCollector,
    types::{LedgerNodeId, LedgerTypeConfig},
};

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

/// Storage components for a shard (state layer, block archive, raft log store, block
/// announcements).
type ShardStorage = (
    Arc<StateLayer<FileBackend>>,
    Arc<BlockArchive<FileBackend>>,
    RaftLogStore<FileBackend>,
    broadcast::Sender<BlockAnnouncement>,
);

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the Multi-Raft Manager.
#[derive(Debug, Clone, bon::Builder)]
pub struct MultiRaftConfig {
    /// Base data directory (shards stored in subdirectories).
    pub data_dir: PathBuf,
    /// This node's ID.
    pub node_id: LedgerNodeId,
    /// Raft heartbeat interval in milliseconds.
    #[builder(default = 150)]
    pub heartbeat_interval_ms: u64,
    /// Minimum election timeout in milliseconds.
    #[builder(default = 300)]
    pub election_timeout_min_ms: u64,
    /// Maximum election timeout in milliseconds.
    #[builder(default = 600)]
    pub election_timeout_max_ms: u64,
    /// Whether to inject trace context into Raft RPCs.
    #[builder(default = true)]
    pub trace_raft_rpcs: bool,
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
            trace_raft_rpcs: true,
        }
    }

    /// Get the data directory for a specific shard.
    pub fn shard_dir(&self, shard_id: ShardId) -> PathBuf {
        if shard_id == ShardId::new(0) {
            self.data_dir.join("shards").join("_system")
        } else {
            self.data_dir.join("shards").join(format!("shard_{:04}", shard_id.value()))
        }
    }
}

/// Configuration for a single shard.
#[derive(Debug, Clone, bon::Builder)]
pub struct ShardConfig {
    /// Shard identifier.
    pub shard_id: ShardId,
    /// Initial cluster members (node_id -> address).
    #[builder(default)]
    pub initial_members: Vec<(LedgerNodeId, String)>,
    /// Whether to bootstrap this shard as a new cluster.
    #[builder(default = true)]
    pub bootstrap: bool,
    /// Whether to start background jobs (GC, compactor).
    #[builder(default = true)]
    pub enable_background_jobs: bool,
}

impl ShardConfig {
    /// Create configuration for the system shard.
    pub fn system(node_id: LedgerNodeId, address: String) -> Self {
        Self {
            shard_id: ShardId::new(0),
            initial_members: vec![(node_id, address)],
            bootstrap: true,
            enable_background_jobs: true,
        }
    }

    /// Create configuration for a data shard.
    pub fn data(shard_id: ShardId, initial_members: Vec<(LedgerNodeId, String)>) -> Self {
        Self { shard_id, initial_members, bootstrap: true, enable_background_jobs: true }
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
    /// B+ tree compactor handle.
    btree_compactor_handle: Option<JoinHandle<()>>,
    /// Integrity scrubber handle.
    integrity_scrubber_handle: Option<JoinHandle<()>>,
}

impl ShardBackgroundJobs {
    /// Create with no jobs (used when background_jobs disabled).
    fn none() -> Self {
        Self {
            gc_handle: None,
            compactor_handle: None,
            recovery_handle: None,
            btree_compactor_handle: None,
            integrity_scrubber_handle: None,
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
        if let Some(handle) = self.btree_compactor_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.integrity_scrubber_handle.take() {
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
    /// Block announcement broadcast channel for real-time block notifications.
    block_announcements: broadcast::Sender<BlockAnnouncement>,
    /// Background job handles (wrapped in Mutex for mutable access through Arc).
    background_jobs: parking_lot::Mutex<ShardBackgroundJobs>,
}

impl ShardGroup {
    /// Get the shard ID.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Get the Raft instance.
    #[must_use]
    pub fn raft(&self) -> &Arc<Raft<LedgerTypeConfig>> {
        &self.raft
    }

    /// Get the state layer.
    #[must_use]
    pub fn state(&self) -> &Arc<StateLayer<FileBackend>> {
        &self.state
    }

    /// Get the block archive.
    #[must_use]
    pub fn block_archive(&self) -> &Arc<BlockArchive<FileBackend>> {
        &self.block_archive
    }

    /// Get the applied state accessor.
    #[must_use]
    pub fn applied_state(&self) -> &AppliedStateAccessor {
        &self.applied_state
    }

    /// Get the block announcements broadcast channel.
    #[must_use]
    pub fn block_announcements(&self) -> &broadcast::Sender<BlockAnnouncement> {
        &self.block_announcements
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
        Self { config, shards: RwLock::new(HashMap::new()), router: RwLock::new(None) }
    }

    /// Get the configuration.
    #[must_use]
    pub fn config(&self) -> &MultiRaftConfig {
        &self.config
    }

    /// Get a shard group by ID.
    pub fn get_shard(&self, shard_id: ShardId) -> Result<Arc<ShardGroup>> {
        self.shards.read().get(&shard_id).cloned().ok_or(MultiRaftError::ShardNotFound { shard_id })
    }

    /// Get the system shard (_system).
    pub fn system_shard(&self) -> Result<Arc<ShardGroup>> {
        self.get_shard(ShardId::new(0))
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
        if shard_config.shard_id != ShardId::new(0) {
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
        if !self.has_shard(ShardId::new(0)) {
            return Err(MultiRaftError::SystemShardRequired);
        }

        if shard_config.shard_id == ShardId::new(0) {
            return Err(MultiRaftError::Raft {
                shard_id: ShardId::new(0),
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

        info!(shard_id = shard_id.value(), "Starting shard group");

        // Create shard directory
        let shard_dir = self.config.shard_dir(shard_id);
        std::fs::create_dir_all(&shard_dir).map_err(|e| MultiRaftError::Storage {
            shard_id,
            message: format!("Failed to create shard directory: {}", e),
        })?;

        // Open storage (includes block announcements channel wired to RaftLogStore)
        let (state, block_archive, log_store, block_announcements) =
            self.open_shard_storage(shard_id, &shard_dir)?;

        // Get accessor before log_store is consumed
        let applied_state = log_store.accessor();

        let network = GrpcRaftNetworkFactory::with_trace_config(self.config.trace_raft_rpcs);

        // Build Raft config
        let raft_config = openraft::Config {
            cluster_name: format!("ledger-shard-{}", shard_id.value()),
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
            block_announcements,
            background_jobs: parking_lot::Mutex::new(background_jobs),
        });

        // Register shard
        {
            let mut shards = self.shards.write();
            shards.insert(shard_id, shard_group.clone());
        }

        info!(shard_id = shard_id.value(), "Shard group started successfully");

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
        info!(shard_id = shard_id.value(), "Starting background jobs for shard");

        // TTL Garbage Collector
        let gc = TtlGarbageCollector::builder()
            .raft(raft.clone())
            .node_id(self.config.node_id)
            .state(state.clone())
            .applied_state(applied_state.clone())
            .build();
        let gc_handle = gc.start();
        info!(shard_id = shard_id.value(), "Started TTL garbage collector");

        // Block Compactor
        let compactor = BlockCompactor::builder()
            .raft(raft.clone())
            .node_id(self.config.node_id)
            .block_archive(block_archive.clone())
            .applied_state(applied_state.clone())
            .build();
        let compactor_handle = compactor.start();
        info!(shard_id = shard_id.value(), "Started block compactor");

        // Auto Recovery Job
        let recovery = AutoRecoveryJob::builder()
            .raft(raft.clone())
            .node_id(self.config.node_id)
            .applied_state(applied_state)
            .state(state.clone())
            .block_archive(Some(block_archive))
            .build();
        let recovery_handle = recovery.start();
        info!(shard_id = shard_id.value(), "Started auto recovery job");

        // B+ Tree Compactor
        let btree_compactor = BTreeCompactor::builder()
            .raft(raft.clone())
            .node_id(self.config.node_id)
            .state(state.clone())
            .build();
        let btree_compactor_handle = btree_compactor.start();
        info!(shard_id = shard_id.value(), "Started B+ tree compactor");

        // Integrity Scrubber
        let integrity_scrubber = IntegrityScrubberJob::builder()
            .raft(raft)
            .node_id(self.config.node_id)
            .state(state)
            .build();
        let integrity_scrubber_handle = integrity_scrubber.start();
        info!(shard_id = shard_id.value(), "Started integrity scrubber");

        ShardBackgroundJobs {
            gc_handle: Some(gc_handle),
            compactor_handle: Some(compactor_handle),
            recovery_handle: Some(recovery_handle),
            btree_compactor_handle: Some(btree_compactor_handle),
            integrity_scrubber_handle: Some(integrity_scrubber_handle),
        }
    }

    /// Open storage for a shard.
    fn open_shard_storage(&self, shard_id: ShardId, shard_dir: &Path) -> Result<ShardStorage> {
        // Use larger pages for all databases to support larger batch sizes
        // This matches RAFT_PAGE_SIZE in RaftLogStore
        const SHARD_PAGE_SIZE: usize = 16 * 1024; // 16KB

        // Open or create state database using inferadb-ledger-store with 16KB pages
        let state_db_path = shard_dir.join("state.db");
        let state_db = if state_db_path.exists() {
            Database::<FileBackend>::open(&state_db_path)
        } else {
            let config = DatabaseConfig { page_size: SHARD_PAGE_SIZE, ..Default::default() };
            Database::<FileBackend>::create_with_config(&state_db_path, config)
        }
        .map_err(|e| MultiRaftError::Storage {
            shard_id,
            message: format!("Failed to open state db: {}", e),
        })?;
        let state = Arc::new(StateLayer::new(Arc::new(state_db)));

        // Open or create block archive database using inferadb-ledger-store with 16KB pages
        let blocks_db_path = shard_dir.join("blocks.db");
        let blocks_db = if blocks_db_path.exists() {
            Database::<FileBackend>::open(&blocks_db_path)
        } else {
            let config = DatabaseConfig { page_size: SHARD_PAGE_SIZE, ..Default::default() };
            Database::<FileBackend>::create_with_config(&blocks_db_path, config)
        }
        .map_err(|e| MultiRaftError::Storage {
            shard_id,
            message: format!("Failed to open blocks db: {}", e),
        })?;
        let block_archive = Arc::new(BlockArchive::new(Arc::new(blocks_db)));

        // Create block announcements broadcast channel for real-time notifications.
        // Buffer size of 1024 allows for burst handling during high commit rates.
        let (block_announcements, _) = broadcast::channel(1024);

        // Open Raft log store (uses inferadb-ledger-store storage - handles open/create internally)
        let log_path = shard_dir.join("raft.db");
        let log_store = RaftLogStore::<FileBackend>::open(&log_path)
            .map_err(|e| MultiRaftError::Storage {
                shard_id,
                message: format!("Failed to open log store: {}", e),
            })?
            .with_state_layer(state.clone())
            .with_block_archive(block_archive.clone())
            .with_shard_config(shard_id, self.config.node_id.to_string())
            .with_block_announcements(block_announcements.clone());

        Ok((state, block_archive, log_store, block_announcements))
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

        raft.initialize(members).await.map_err(|e| MultiRaftError::Raft {
            shard_id: config.shard_id,
            message: format!("Failed to initialize: {}", e),
        })?;

        info!(
            shard_id = config.shard_id.value(),
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
            shards.remove(&shard_id).ok_or(MultiRaftError::ShardNotFound { shard_id })?
        };

        // Abort background jobs first
        {
            let mut jobs = shard.background_jobs.lock();
            jobs.abort();
            debug!(shard_id = shard_id.value(), "Aborted background jobs");
        }

        // Trigger Raft shutdown
        if let Err(e) = shard.raft.shutdown().await {
            warn!(shard_id = shard_id.value(), error = ?e, "Error during Raft shutdown");
        }

        info!(shard_id = shard_id.value(), "Shard group stopped");
        Ok(())
    }

    /// Stop all shard groups.
    pub async fn shutdown(&self) {
        let shard_ids: Vec<ShardId> = self.list_shards();

        for shard_id in shard_ids {
            if let Err(e) = self.stop_shard(shard_id).await {
                warn!(shard_id = shard_id.value(), error = %e, "Error stopping shard during shutdown");
            }
        }

        info!("Multi-Raft Manager shutdown complete");
    }

    /// Gracefully shut down all shard groups with leadership handoff.
    ///
    /// For each shard where this node is the leader, triggers a final
    /// snapshot before shutdown so the new leader has up-to-date state.
    /// Then performs the normal shutdown sequence.
    ///
    /// openraft 0.9 does not provide explicit leadership transfer, so
    /// shutting down the leader triggers re-election among remaining nodes.
    pub async fn graceful_shutdown(&self) {
        let node_id = self.config.node_id;
        let shard_ids = self.list_shards();

        // Trigger final snapshots for leader shards
        for shard_id in &shard_ids {
            // Clone the shard Arc so we can drop the lock before awaiting
            let shard = {
                let shards = self.shards.read();
                shards.get(shard_id).cloned()
            };

            if let Some(shard) = shard
                && shard.is_leader(node_id)
            {
                info!(
                    shard_id = shard_id.value(),
                    "Triggering final snapshot before leadership handoff"
                );
                if let Err(e) = shard.raft.trigger().snapshot().await {
                    warn!(
                        shard_id = shard_id.value(),
                        error = %e,
                        "Failed to trigger final snapshot"
                    );
                }
            }
        }

        // Proceed with normal shutdown
        self.shutdown().await;
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
    use inferadb_ledger_test_utils::TestDir;

    use super::*;

    fn create_test_config(temp_dir: &TestDir) -> MultiRaftConfig {
        MultiRaftConfig::new(temp_dir.path().to_path_buf(), 1)
    }

    #[test]
    fn test_config_shard_dir() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);

        // System shard directory
        let system_dir = config.shard_dir(ShardId::new(0));
        assert!(system_dir.ends_with("shards/_system"));

        // Data shard directory
        let data_dir = config.shard_dir(ShardId::new(1));
        assert!(data_dir.ends_with("shards/shard_0001"));

        let data_dir = config.shard_dir(ShardId::new(42));
        assert!(data_dir.ends_with("shards/shard_0042"));
    }

    #[test]
    fn test_shard_config_system() {
        let config = ShardConfig::system(1, "127.0.0.1:50051".to_string());
        assert_eq!(config.shard_id, ShardId::new(0));
        assert!(config.bootstrap);
        assert_eq!(config.initial_members.len(), 1);
    }

    #[test]
    fn test_shard_config_data() {
        let members = vec![(1, "127.0.0.1:50051".to_string()), (2, "127.0.0.1:50052".to_string())];
        let config = ShardConfig::data(ShardId::new(1), members);
        assert_eq!(config.shard_id, ShardId::new(1));
        assert!(config.bootstrap);
        assert_eq!(config.initial_members.len(), 2);
    }

    #[test]
    fn test_multi_raft_config_builder() {
        let temp = TestDir::new();
        let config =
            MultiRaftConfig::builder().data_dir(temp.path().to_path_buf()).node_id(42).build();
        assert_eq!(config.node_id, 42);
        assert_eq!(config.heartbeat_interval_ms, 150);
        assert_eq!(config.election_timeout_min_ms, 300);
        assert_eq!(config.election_timeout_max_ms, 600);
    }

    #[test]
    fn test_multi_raft_config_builder_custom_timeouts() {
        let temp = TestDir::new();
        let config = MultiRaftConfig::builder()
            .data_dir(temp.path().to_path_buf())
            .node_id(1)
            .heartbeat_interval_ms(200)
            .election_timeout_min_ms(500)
            .election_timeout_max_ms(1000)
            .build();
        assert_eq!(config.heartbeat_interval_ms, 200);
        assert_eq!(config.election_timeout_min_ms, 500);
        assert_eq!(config.election_timeout_max_ms, 1000);
    }

    #[test]
    fn test_multi_raft_config_builder_matches_new() {
        let temp = TestDir::new();
        let from_builder =
            MultiRaftConfig::builder().data_dir(temp.path().to_path_buf()).node_id(1).build();
        let from_new = MultiRaftConfig::new(temp.path().to_path_buf(), 1);
        assert_eq!(from_builder.node_id, from_new.node_id);
        assert_eq!(from_builder.heartbeat_interval_ms, from_new.heartbeat_interval_ms);
        assert_eq!(from_builder.election_timeout_min_ms, from_new.election_timeout_min_ms);
        assert_eq!(from_builder.election_timeout_max_ms, from_new.election_timeout_max_ms);
    }

    #[test]
    fn test_shard_config_builder() {
        let config = ShardConfig::builder().shard_id(ShardId::new(5)).build();
        assert_eq!(config.shard_id, ShardId::new(5));
        assert!(config.initial_members.is_empty());
        assert!(config.bootstrap);
        assert!(config.enable_background_jobs);
    }

    #[test]
    fn test_shard_config_builder_with_all_fields() {
        let members = vec![(1, "127.0.0.1:50051".to_string())];
        let config = ShardConfig::builder()
            .shard_id(ShardId::new(3))
            .initial_members(members.clone())
            .bootstrap(false)
            .enable_background_jobs(false)
            .build();
        assert_eq!(config.shard_id, ShardId::new(3));
        assert_eq!(config.initial_members, members);
        assert!(!config.bootstrap);
        assert!(!config.enable_background_jobs);
    }

    #[test]
    fn test_shard_config_without_background_jobs_method() {
        let config =
            ShardConfig::system(1, "127.0.0.1:50051".to_string()).without_background_jobs();
        assert!(!config.enable_background_jobs);
    }

    #[test]
    fn test_manager_creation() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        assert_eq!(manager.list_shards().len(), 0);
        assert!(!manager.has_shard(ShardId::new(0)));
    }

    #[test]
    fn test_manager_stats_empty() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        let stats = manager.stats();
        assert_eq!(stats.total_shards, 0);
        assert_eq!(stats.leader_shards, 0);
        assert_eq!(stats.node_id, 1);
    }

    #[tokio::test]
    async fn test_system_shard_required() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Try to start data shard without system shard
        let shard_config =
            ShardConfig::data(ShardId::new(1), vec![(1, "127.0.0.1:50051".to_string())]);
        let result = manager.start_data_shard(shard_config).await;

        assert!(matches!(result, Err(MultiRaftError::SystemShardRequired)));
    }

    #[tokio::test]
    async fn test_start_system_shard() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        let shard_config = ShardConfig::system(1, "127.0.0.1:50051".to_string());
        let result = manager.start_system_shard(shard_config).await;

        assert!(result.is_ok(), "start_system_shard failed: {:?}", result.err());

        let shard = result.unwrap();
        assert_eq!(shard.shard_id(), ShardId::new(0));
        assert!(manager.has_shard(ShardId::new(0)));
        assert_eq!(manager.list_shards(), vec![ShardId::new(0)]);
    }

    #[tokio::test]
    async fn test_start_multiple_shards() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Start system shard
        let system_config = ShardConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_shard(system_config).await.expect("start system");

        // Start data shard
        let data_config =
            ShardConfig::data(ShardId::new(1), vec![(1, "127.0.0.1:50051".to_string())]);
        manager.start_data_shard(data_config).await.expect("start data shard");

        assert_eq!(manager.list_shards().len(), 2);
        assert!(manager.has_shard(ShardId::new(0)));
        assert!(manager.has_shard(ShardId::new(1)));
    }

    #[tokio::test]
    async fn test_duplicate_shard_error() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Start system shard
        let shard_config = ShardConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_shard(shard_config.clone()).await.expect("start system");

        // Try to start again
        let result = manager.start_system_shard(shard_config).await;
        assert!(
            matches!(result, Err(MultiRaftError::ShardExists { shard_id }) if shard_id == ShardId::new(0))
        );
    }

    #[tokio::test]
    async fn test_stop_shard() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Start system shard
        let shard_config = ShardConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_shard(shard_config).await.expect("start system");

        assert!(manager.has_shard(ShardId::new(0)));

        // Stop shard
        manager.stop_shard(ShardId::new(0)).await.expect("stop shard");

        assert!(!manager.has_shard(ShardId::new(0)));
    }

    #[tokio::test]
    async fn test_get_shard() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Try to get non-existent shard
        let result = manager.get_shard(ShardId::new(0));
        assert!(
            matches!(result, Err(MultiRaftError::ShardNotFound { shard_id }) if shard_id == ShardId::new(0))
        );

        // Start and get
        let shard_config = ShardConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_shard(shard_config).await.expect("start system");

        let shard = manager.get_shard(ShardId::new(0)).expect("get shard");
        assert_eq!(shard.shard_id(), ShardId::new(0));

        // system_shard() should work too
        let system = manager.system_shard().expect("system shard");
        assert_eq!(system.shard_id(), ShardId::new(0));
    }

    #[test]
    fn test_background_jobs_none() {
        let jobs = ShardBackgroundJobs::none();
        // All handles should be None
        assert!(jobs.gc_handle.is_none());
        assert!(jobs.compactor_handle.is_none());
        assert!(jobs.recovery_handle.is_none());
        assert!(jobs.btree_compactor_handle.is_none());
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
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Create config with background jobs disabled
        let mut shard_config = ShardConfig::system(1, "127.0.0.1:50051".to_string());
        shard_config.enable_background_jobs = false;

        manager.start_system_shard(shard_config).await.expect("start system");

        let shard = manager.get_shard(ShardId::new(0)).expect("get shard");

        // Background jobs should be None when disabled
        let jobs = shard.background_jobs.lock();
        assert!(jobs.gc_handle.is_none());
        assert!(jobs.compactor_handle.is_none());
        assert!(jobs.recovery_handle.is_none());
    }

    #[tokio::test]
    async fn test_shard_with_background_jobs_enabled() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Create config with background jobs enabled (default)
        let shard_config = ShardConfig::system(1, "127.0.0.1:50051".to_string());
        assert!(shard_config.enable_background_jobs); // Verify default is true

        manager.start_system_shard(shard_config).await.expect("start system");

        let shard = manager.get_shard(ShardId::new(0)).expect("get shard");

        // Background jobs should be Some when enabled
        let jobs = shard.background_jobs.lock();
        assert!(jobs.gc_handle.is_some(), "GC job should be started");
        assert!(jobs.compactor_handle.is_some(), "Compactor job should be started");
        assert!(jobs.recovery_handle.is_some(), "Recovery job should be started");
    }

    #[tokio::test]
    async fn test_stop_shard_aborts_background_jobs() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Start with background jobs enabled
        let shard_config = ShardConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_shard(shard_config).await.expect("start system");

        // Verify jobs are running before stop
        {
            let shard = manager.get_shard(ShardId::new(0)).expect("get shard");
            let jobs = shard.background_jobs.lock();
            assert!(jobs.gc_handle.is_some());
            assert!(jobs.compactor_handle.is_some());
            assert!(jobs.recovery_handle.is_some());
        }

        // Stop shard - should abort jobs
        manager.stop_shard(ShardId::new(0)).await.expect("stop shard");

        // Shard should be removed
        assert!(!manager.has_shard(ShardId::new(0)));
    }
}
