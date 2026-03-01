//! Multi-Raft Manager for coordinating multiple independent region groups.
//!
//! InferaDB uses a region-per-Raft-group architecture where
//! each region is an independent Raft consensus group. The `_system` region handles
//! global coordination, while data regions handle organization workloads.
//!
//! ## Architecture
//!
//! ```text
//! MultiRaftManager
//! ├── _system region (RegionGroup 0)
//! │   ├── Raft instance
//! │   ├── RaftLogStore + StateLayer
//! │   ├── BlockArchive
//! │   └── Background jobs
//! ├── data region 1 (RegionGroup 1)
//! │   └── ... (same structure)
//! └── data region N (RegionGroup N)
//!     └── ...
//! ```
//!
//! ## Region Isolation
//!
//! Each region group is fully isolated:
//! - Separate Raft consensus (independent elections, log replication)
//! - Separate storage files (state.db, blocks.db, raft.db per region)
//! - Separate background jobs (GC, compaction, recovery)
//!
//! ## Usage
//!
//! ```no_run
//! # use std::path::PathBuf;
//! # use inferadb_ledger_raft::multi_raft::{MultiRaftConfig, MultiRaftManager, RegionConfig};
//! # use inferadb_ledger_types::Region;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = MultiRaftConfig::builder()
//!     .data_dir(PathBuf::from("/data"))
//!     .node_id(1u64)
//!     .local_region(Region::GLOBAL)
//!     .build();
//! let manager = MultiRaftManager::new(config);
//!
//! // Start the _system region (always required)
//! let system_config = RegionConfig::system(1u64, "127.0.0.1:50051".to_string());
//! manager.start_system_region(system_config).await?;
//!
//! // Route requests to a region
//! let region = manager.get_region_group(Region::GLOBAL)?;
//! # Ok(())
//! # }
//! ```

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use inferadb_ledger_proto::proto::BlockAnnouncement;
use inferadb_ledger_state::{
    BlockArchive, StateLayer,
    system::{MIN_NODES_PER_PROTECTED_REGION, SystemOrganizationService},
};
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{OrganizationId, Region};
use openraft::{BasicNode, Raft, storage::Adaptor};
use parking_lot::RwLock;
use snafu::Snafu;
use tokio::{sync::broadcast, task::JoinHandle};
use tracing::{debug, info, warn};

use crate::{
    auto_recovery::AutoRecoveryJob,
    block_compaction::BlockCompactor,
    btree_compaction::BTreeCompactor,
    dek_rewrap::{DekRewrapJob, RewrapProgress},
    integrity_scrubber::IntegrityScrubberJob,
    log_storage::{AppliedStateAccessor, RaftLogStore},
    metrics::record_region_node_count,
    raft_network::GrpcRaftNetworkFactory,
    region_router::RegionRouter,
    region_storage::RegionStorageManager,
    ttl_gc::TtlGarbageCollector,
    types::{LedgerNodeId, LedgerTypeConfig},
};

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur during multi-raft operations.
#[derive(Debug, Snafu)]
pub enum MultiRaftError {
    /// Region already exists.
    #[snafu(display("Region {region} already exists"))]
    RegionExists { region: Region },

    /// Region not found.
    #[snafu(display("Region {region} not found"))]
    RegionNotFound { region: Region },

    /// Failed to open storage.
    #[snafu(display("Storage error for region {region}: {message}"))]
    Storage { region: Region, message: String },

    /// Failed to create Raft instance.
    #[snafu(display("Raft error for region {region}: {message}"))]
    Raft { region: Region, message: String },

    /// System region not initialized.
    #[snafu(display("System region (_system) must be started first"))]
    SystemRegionRequired,

    /// Insufficient initial members for a protected region.
    ///
    /// The caller is responsible for filtering to in-region nodes before
    /// constructing `RegionConfig`; this check enforces the minimum count.
    #[snafu(display(
        "Protected region {region} requires at least {required} initial members, found {found}"
    ))]
    InsufficientNodes { region: Region, required: usize, found: usize },
}

/// Result type for multi-raft operations.
pub type Result<T> = std::result::Result<T, MultiRaftError>;

/// Storage components returned from region opening (state, block archive, raft log store,
/// block announcements).
type OpenedRegionStorage = (
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
    /// Base data directory (regions stored in subdirectories).
    pub data_dir: PathBuf,
    /// This node's ID.
    pub node_id: LedgerNodeId,
    /// This node's region tag (set at startup from node config).
    pub local_region: Region,
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
    /// Creates a new configuration.
    pub fn new(data_dir: PathBuf, node_id: LedgerNodeId, local_region: Region) -> Self {
        Self {
            data_dir,
            node_id,
            local_region,
            heartbeat_interval_ms: 150,
            election_timeout_min_ms: 300,
            election_timeout_max_ms: 600,
            trace_raft_rpcs: true,
        }
    }
}

/// Configuration for a single region.
#[derive(Debug, Clone, bon::Builder)]
pub struct RegionConfig {
    /// Region identifier.
    pub region: Region,
    /// Initial cluster members (node_id -> address).
    #[builder(default)]
    pub initial_members: Vec<(LedgerNodeId, String)>,
    /// Whether to bootstrap this region as a new cluster.
    #[builder(default = true)]
    pub bootstrap: bool,
    /// Whether to start background jobs (GC, compactor).
    #[builder(default = true)]
    pub enable_background_jobs: bool,
}

impl RegionConfig {
    /// Creates configuration for the system region.
    pub fn system(node_id: LedgerNodeId, address: String) -> Self {
        Self {
            region: Region::GLOBAL,
            initial_members: vec![(node_id, address)],
            bootstrap: true,
            enable_background_jobs: true,
        }
    }

    /// Creates configuration for a data region.
    pub fn data(region: Region, initial_members: Vec<(LedgerNodeId, String)>) -> Self {
        Self { region, initial_members, bootstrap: true, enable_background_jobs: true }
    }

    /// Disables background jobs (useful for testing).
    pub fn without_background_jobs(mut self) -> Self {
        self.enable_background_jobs = false;
        self
    }
}

// ============================================================================
// Region Group
// ============================================================================

/// Background job handles for a region.
pub struct RegionBackgroundJobs {
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
    /// DEK re-wrapping job handle.
    dek_rewrap_handle: Option<JoinHandle<()>>,
    /// Shared re-wrapping progress (read by admin service).
    rewrap_progress: Arc<RewrapProgress>,
}

impl RegionBackgroundJobs {
    /// Creates with no jobs (used when background_jobs disabled).
    fn none() -> Self {
        Self {
            gc_handle: None,
            compactor_handle: None,
            recovery_handle: None,
            btree_compactor_handle: None,
            integrity_scrubber_handle: None,
            dek_rewrap_handle: None,
            rewrap_progress: Arc::new(RewrapProgress::new()),
        }
    }

    /// Returns the shared re-wrapping progress tracker.
    pub fn rewrap_progress(&self) -> Arc<RewrapProgress> {
        Arc::clone(&self.rewrap_progress)
    }

    /// Aborts all running background jobs.
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
        if let Some(handle) = self.dek_rewrap_handle.take() {
            handle.abort();
        }
    }
}

/// A single region group with its own Raft instance and storage.
///
/// Each RegionGroup is a complete, isolated Raft cluster member for one region.
/// Uses FileBackend for production storage - this is intentional as Raft requires
/// durable storage for safety.
pub struct RegionGroup {
    /// Region identifier.
    region: Region,
    /// The Raft consensus instance.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// Shared state layer for this region.
    state: Arc<StateLayer<FileBackend>>,
    /// Block archive for historical blocks.
    block_archive: Arc<BlockArchive<FileBackend>>,
    /// Accessor for applied state.
    applied_state: AppliedStateAccessor,
    /// Block announcement broadcast channel for real-time block notifications.
    block_announcements: broadcast::Sender<BlockAnnouncement>,
    /// Background job handles (wrapped in Mutex for mutable access through Arc).
    background_jobs: parking_lot::Mutex<RegionBackgroundJobs>,
}

impl RegionGroup {
    /// Returns the region ID.
    pub fn region(&self) -> Region {
        self.region
    }

    /// Returns the Raft instance.
    #[must_use]
    pub fn raft(&self) -> &Arc<Raft<LedgerTypeConfig>> {
        &self.raft
    }

    /// Returns the state layer.
    #[must_use]
    pub fn state(&self) -> &Arc<StateLayer<FileBackend>> {
        &self.state
    }

    /// Returns the block archive.
    #[must_use]
    pub fn block_archive(&self) -> &Arc<BlockArchive<FileBackend>> {
        &self.block_archive
    }

    /// Returns the applied state accessor.
    #[must_use]
    pub fn applied_state(&self) -> &AppliedStateAccessor {
        &self.applied_state
    }

    /// Returns the block announcements broadcast channel.
    #[must_use]
    pub fn block_announcements(&self) -> &broadcast::Sender<BlockAnnouncement> {
        &self.block_announcements
    }

    /// Checks if this node is the leader for this region.
    pub fn is_leader(&self, node_id: LedgerNodeId) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(node_id)
    }

    /// Returns the current leader node ID, if known.
    pub fn current_leader(&self) -> Option<LedgerNodeId> {
        self.raft.metrics().borrow().current_leader
    }
}

// ============================================================================
// Multi-Raft Manager
// ============================================================================

/// Manager for multiple Raft region groups.
///
/// Coordinates the lifecycle of multiple independent Raft consensus groups,
/// each handling a subset of organizations. Uses FileBackend for production storage.
/// Delegates per-region database lifecycle to [`RegionStorageManager`].
pub struct MultiRaftManager {
    /// Configuration.
    config: MultiRaftConfig,
    /// Per-region database storage manager.
    storage_manager: RegionStorageManager,
    /// Active region groups indexed by region ID.
    regions: RwLock<HashMap<Region, Arc<RegionGroup>>>,
    /// Router for organization-to-region resolution.
    router: RwLock<Option<Arc<RegionRouter<FileBackend>>>>,
}

impl MultiRaftManager {
    /// Creates a new Multi-Raft Manager.
    pub fn new(config: MultiRaftConfig) -> Self {
        let storage_manager = RegionStorageManager::new(config.data_dir.clone());
        Self {
            config,
            storage_manager,
            regions: RwLock::new(HashMap::new()),
            router: RwLock::new(None),
        }
    }

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &MultiRaftConfig {
        &self.config
    }

    /// Returns this node's configured region.
    pub fn local_region(&self) -> Region {
        self.config.local_region
    }

    /// Returns the region storage manager.
    #[must_use]
    pub fn storage_manager(&self) -> &RegionStorageManager {
        &self.storage_manager
    }

    /// Returns a region group by ID.
    ///
    /// # Errors
    ///
    /// Returns [`MultiRaftError::RegionNotFound`] if no region with the given ID
    /// is currently active.
    pub fn get_region_group(&self, region: Region) -> Result<Arc<RegionGroup>> {
        self.regions.read().get(&region).cloned().ok_or(MultiRaftError::RegionNotFound { region })
    }

    /// Returns the system region (`_system`).
    ///
    /// # Errors
    ///
    /// Returns [`MultiRaftError::RegionNotFound`] if the system region (ID 0)
    /// has not been started.
    pub fn system_region(&self) -> Result<Arc<RegionGroup>> {
        self.get_region_group(Region::GLOBAL)
    }

    /// Lists all active region IDs.
    pub fn list_regions(&self) -> Vec<Region> {
        self.regions.read().keys().copied().collect()
    }

    /// Checks if a region is active.
    pub fn has_region(&self, region: Region) -> bool {
        self.regions.read().contains_key(&region)
    }

    /// Returns the region router (if initialized).
    pub fn router(&self) -> Option<Arc<RegionRouter<FileBackend>>> {
        self.router.read().clone()
    }

    /// Routes an organization to its region group.
    ///
    /// Uses the RegionRouter to look up the organization's region assignment,
    /// then returns the local RegionGroup if available.
    ///
    /// Returns `None` if:
    /// - Router not initialized (system region not started)
    /// - Organization not found in routing table
    /// - Region is on a different node (requires forwarding)
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    pub fn route_organization(&self, organization: OrganizationId) -> Option<Arc<RegionGroup>> {
        let router = self.router.read().clone()?;

        // Look up region assignment
        let routing = router.get_routing(organization).ok()?;

        // Get local region group (if we host this region)
        self.regions.read().get(&routing.region).cloned()
    }

    /// Returns the region ID for an organization.
    ///
    /// Looks up the organization's region assignment without checking
    /// if the region is locally available.
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    pub fn get_organization_region(&self, organization: OrganizationId) -> Option<Region> {
        let router = self.router.read().clone()?;
        router.get_routing(organization).ok().map(|r| r.region)
    }

    /// Starts the system region (`_system`).
    ///
    /// The system region must be started before any data regions.
    /// It stores the organization routing table and cluster metadata.
    /// Also initializes the `RegionRouter` for organization-to-region routing.
    ///
    /// # Errors
    ///
    /// Returns [`MultiRaftError::Raft`] if `region` is not 0,
    /// [`MultiRaftError::RegionExists`] if the region is already running,
    /// or a storage/Raft error if initialization fails.
    pub async fn start_system_region(
        &self,
        region_config: RegionConfig,
    ) -> Result<Arc<RegionGroup>> {
        if region_config.region != Region::GLOBAL {
            return Err(MultiRaftError::Raft {
                region: region_config.region,
                message: "System region must have region=0".to_string(),
            });
        }

        let region = self.start_region(region_config).await?;

        // Initialize the RegionRouter with access to _system's state
        let system_service = Arc::new(SystemOrganizationService::new(region.state.clone()));
        let router = Arc::new(RegionRouter::new(system_service, self.config.local_region));
        *self.router.write() = Some(router);

        info!("RegionRouter initialized with _system organization");

        Ok(region)
    }

    /// Starts a data region.
    ///
    /// Requires the system region to be started first.
    ///
    /// # Errors
    ///
    /// Returns [`MultiRaftError::SystemRegionRequired`] if the system region has
    /// not been started, [`MultiRaftError::Raft`] if `region` is 0,
    /// [`MultiRaftError::RegionExists`] if the region is already running,
    /// or a storage/Raft error if initialization fails.
    pub async fn start_data_region(&self, region_config: RegionConfig) -> Result<Arc<RegionGroup>> {
        // Verify system region is running
        if !self.has_region(Region::GLOBAL) {
            return Err(MultiRaftError::SystemRegionRequired);
        }

        if region_config.region == Region::GLOBAL {
            return Err(MultiRaftError::Raft {
                region: Region::GLOBAL,
                message: "Use start_system_region for region=0".to_string(),
            });
        }

        self.start_region(region_config).await
    }

    /// Starts a region group.
    async fn start_region(&self, region_config: RegionConfig) -> Result<Arc<RegionGroup>> {
        let region = region_config.region;

        // Check if region already exists
        if self.has_region(region) {
            return Err(MultiRaftError::RegionExists { region });
        }

        let is_protected = region.requires_residency();

        // Protected regions enforce minimum in-region node count
        if is_protected && region_config.initial_members.len() < MIN_NODES_PER_PROTECTED_REGION {
            return Err(MultiRaftError::InsufficientNodes {
                region,
                required: MIN_NODES_PER_PROTECTED_REGION,
                found: region_config.initial_members.len(),
            });
        }

        // Emit region node count metric
        record_region_node_count(
            region.as_str(),
            region_config.initial_members.len(),
            is_protected,
        );

        info!(region = region.as_str(), "Starting region group");

        // Open storage via RegionStorageManager (creates directory + databases + RaftLogStore)
        let (state, block_archive, log_store, block_announcements) =
            self.open_region_storage(region)?;

        // Get accessor before log_store is consumed
        let applied_state = log_store.accessor();

        let network = GrpcRaftNetworkFactory::with_trace_config(self.config.trace_raft_rpcs);

        // Build Raft config
        let raft_config = openraft::Config {
            cluster_name: format!("ledger-region-{}", region.as_str()),
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
            region,
            message: format!("Failed to create Raft instance: {}", e),
        })?;

        let raft = Arc::new(raft);

        // Bootstrap if configured
        if region_config.bootstrap && !region_config.initial_members.is_empty() {
            self.bootstrap_region(&raft, &region_config).await?;
        }

        // Start background jobs if enabled
        let background_jobs = if region_config.enable_background_jobs {
            self.start_background_jobs(
                region,
                raft.clone(),
                state.clone(),
                block_archive.clone(),
                applied_state.clone(),
            )
        } else {
            RegionBackgroundJobs::none()
        };

        // Create region group
        let region_group = Arc::new(RegionGroup {
            region,
            raft,
            state,
            block_archive,
            applied_state,
            block_announcements,
            background_jobs: parking_lot::Mutex::new(background_jobs),
        });

        // Register region
        {
            let mut regions = self.regions.write();
            regions.insert(region, region_group.clone());
        }

        info!(region = region.as_str(), "Region group started successfully");

        Ok(region_group)
    }

    /// Starts background jobs for a region.
    fn start_background_jobs(
        &self,
        region: Region,
        raft: Arc<Raft<LedgerTypeConfig>>,
        state: Arc<StateLayer<FileBackend>>,
        block_archive: Arc<BlockArchive<FileBackend>>,
        applied_state: AppliedStateAccessor,
    ) -> RegionBackgroundJobs {
        info!(region = region.as_str(), "Starting background jobs for region");

        // TTL Garbage Collector
        let gc = TtlGarbageCollector::builder()
            .raft(raft.clone())
            .node_id(self.config.node_id)
            .state(state.clone())
            .applied_state(applied_state.clone())
            .build();
        let gc_handle = gc.start();
        info!(region = region.as_str(), "Started TTL garbage collector");

        // Block Compactor
        let compactor = BlockCompactor::builder()
            .raft(raft.clone())
            .node_id(self.config.node_id)
            .block_archive(block_archive.clone())
            .applied_state(applied_state.clone())
            .build();
        let compactor_handle = compactor.start();
        info!(region = region.as_str(), "Started block compactor");

        // Auto Recovery Job
        let recovery = AutoRecoveryJob::builder()
            .raft(raft.clone())
            .node_id(self.config.node_id)
            .applied_state(applied_state)
            .state(state.clone())
            .block_archive(Some(block_archive))
            .build();
        let recovery_handle = recovery.start();
        info!(region = region.as_str(), "Started auto recovery job");

        // B+ Tree Compactor
        let btree_compactor = BTreeCompactor::builder()
            .raft(raft.clone())
            .node_id(self.config.node_id)
            .state(state.clone())
            .build();
        let btree_compactor_handle = btree_compactor.start();
        info!(region = region.as_str(), "Started B+ tree compactor");

        // Integrity Scrubber
        let integrity_scrubber = IntegrityScrubberJob::builder().state(state.clone()).build();
        let integrity_scrubber_handle = integrity_scrubber.start();
        info!(region = region.as_str(), "Started integrity scrubber");

        // DEK Re-Wrapping Job
        let rewrap_progress = Arc::new(RewrapProgress::new());
        let dek_rewrap = DekRewrapJob::builder()
            .raft(raft)
            .node_id(self.config.node_id)
            .state(state)
            .progress(rewrap_progress.clone())
            .build();
        let dek_rewrap_handle = dek_rewrap.start();
        info!(region = region.as_str(), "Started DEK re-wrapping job");

        RegionBackgroundJobs {
            gc_handle: Some(gc_handle),
            compactor_handle: Some(compactor_handle),
            recovery_handle: Some(recovery_handle),
            btree_compactor_handle: Some(btree_compactor_handle),
            integrity_scrubber_handle: Some(integrity_scrubber_handle),
            dek_rewrap_handle: Some(dek_rewrap_handle),
            rewrap_progress,
        }
    }

    /// Opens storage for a region.
    ///
    /// Delegates database opening to the [`RegionStorageManager`], then creates
    /// higher-level wrappers (`StateLayer`, `BlockArchive`) and the `RaftLogStore`.
    fn open_region_storage(&self, region: Region) -> Result<OpenedRegionStorage> {
        // Open databases via storage manager (creates directory + state.db, blocks.db, events.db)
        let storage = self
            .storage_manager
            .open_region(region)
            .map_err(|e| MultiRaftError::Storage { region, message: format!("{e}") })?;

        // Wrap raw databases in domain-specific types
        let state = Arc::new(StateLayer::new(storage.state_db().clone()));
        let block_archive = Arc::new(BlockArchive::new(storage.blocks_db().clone()));

        // Create block announcements broadcast channel for real-time notifications.
        // Buffer size of 1024 allows for burst handling during high commit rates.
        let (block_announcements, _) = broadcast::channel(1024);

        // Open Raft log store (uses inferadb-ledger-store storage - handles open/create internally)
        let log_path = self.storage_manager.raft_db_path(region);
        let log_store = RaftLogStore::<FileBackend>::open(&log_path)
            .map_err(|e| MultiRaftError::Storage {
                region,
                message: format!("Failed to open log store: {e}"),
            })?
            .with_state_layer(state.clone())
            .with_block_archive(block_archive.clone())
            .with_region_config(region, self.config.node_id.to_string())
            .with_block_announcements(block_announcements.clone());

        Ok((state, block_archive, log_store, block_announcements))
    }

    /// Bootstraps a region as a new cluster.
    async fn bootstrap_region(
        &self,
        raft: &Raft<LedgerTypeConfig>,
        config: &RegionConfig,
    ) -> Result<()> {
        use std::collections::BTreeMap;

        let mut members: BTreeMap<LedgerNodeId, BasicNode> = BTreeMap::new();
        for (node_id, addr) in &config.initial_members {
            members.insert(*node_id, BasicNode { addr: addr.clone() });
        }

        raft.initialize(members).await.map_err(|e| MultiRaftError::Raft {
            region: config.region,
            message: format!("Failed to initialize: {}", e),
        })?;

        info!(
            region = config.region.as_str(),
            members = config.initial_members.len(),
            "Bootstrapped region cluster"
        );

        Ok(())
    }

    /// Stops a region group.
    ///
    /// This gracefully shuts down the region, stopping background jobs
    /// and removing it from the manager.
    ///
    /// # Errors
    ///
    /// Returns [`MultiRaftError::RegionNotFound`] if no region with the given ID
    /// is currently active.
    pub async fn stop_region(&self, region: Region) -> Result<()> {
        let region_group = {
            let mut regions = self.regions.write();
            regions.remove(&region).ok_or(MultiRaftError::RegionNotFound { region })?
        };

        // Abort background jobs first
        {
            let mut jobs = region_group.background_jobs.lock();
            jobs.abort();
            debug!(region = region.as_str(), "Aborted background jobs");
        }

        // Trigger Raft shutdown
        if let Err(e) = region_group.raft.shutdown().await {
            warn!(region = region.as_str(), error = ?e, "Error during Raft shutdown");
        }

        // Close region storage (removes from storage manager tracking)
        if let Err(e) = self.storage_manager.close_region(region) {
            warn!(region = region.as_str(), error = %e, "Error closing region storage");
        }

        info!(region = region.as_str(), "Region group stopped");
        Ok(())
    }

    /// Stops all region groups.
    pub async fn shutdown(&self) {
        let regions: Vec<Region> = self.list_regions();

        for region in regions {
            if let Err(e) = self.stop_region(region).await {
                warn!(region = region.as_str(), error = %e, "Error stopping region during shutdown");
            }
        }

        info!("Multi-Raft Manager shutdown complete");
    }

    /// Gracefully shut down all region groups with leadership handoff.
    ///
    /// For each region where this node is the leader, triggers a final
    /// snapshot before shutdown so the new leader has up-to-date state.
    /// Then performs the normal shutdown sequence.
    ///
    /// openraft 0.9 does not provide explicit leadership transfer, so
    /// shutting down the leader triggers re-election among remaining nodes.
    pub async fn graceful_shutdown(&self) {
        let node_id = self.config.node_id;
        let regions = self.list_regions();

        // Trigger final snapshots for leader regions
        for region in &regions {
            // Clone the region Arc so we can drop the lock before awaiting
            let region_group = {
                let regions = self.regions.read();
                regions.get(region).cloned()
            };

            if let Some(region_group) = region_group
                && region_group.is_leader(node_id)
            {
                info!(
                    region = region.as_str(),
                    "Triggering final snapshot before leadership handoff"
                );
                if let Err(e) = region_group.raft.trigger().snapshot().await {
                    warn!(
                        region = region.as_str(),
                        error = %e,
                        "Failed to trigger final snapshot"
                    );
                }
            }
        }

        // Proceed with normal shutdown
        self.shutdown().await;
    }

    /// Returns statistics about the manager.
    pub fn stats(&self) -> MultiRaftStats {
        let regions = self.regions.read();
        let mut leader_count = 0;

        for region in regions.values() {
            if region.is_leader(self.config.node_id) {
                leader_count += 1;
            }
        }

        MultiRaftStats {
            total_regions: regions.len(),
            leader_regions: leader_count,
            node_id: self.config.node_id,
        }
    }
}

/// Statistics about the Multi-Raft Manager.
#[derive(Debug, Clone)]
pub struct MultiRaftStats {
    /// Total number of active regions.
    pub total_regions: usize,
    /// Number of regions where this node is leader.
    pub leader_regions: usize,
    /// This node's ID.
    pub node_id: LedgerNodeId,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use inferadb_ledger_test_utils::TestDir;

    use super::*;

    fn create_test_config(temp_dir: &TestDir) -> MultiRaftConfig {
        MultiRaftConfig::new(temp_dir.path().to_path_buf(), 1, Region::GLOBAL)
    }

    #[test]
    fn test_storage_manager_region_dir() {
        let temp = TestDir::new();
        let manager = RegionStorageManager::new(temp.path().to_path_buf());

        // Global region directory
        let global_dir = manager.region_dir(Region::GLOBAL);
        assert!(global_dir.ends_with("global"));
        assert!(!global_dir.to_string_lossy().contains("regions"));

        // Data region directories
        let data_dir = manager.region_dir(Region::US_EAST_VA);
        assert!(data_dir.ends_with("regions/us-east-va"));

        let data_dir = manager.region_dir(Region::JP_EAST_TOKYO);
        assert!(data_dir.ends_with("regions/jp-east-tokyo"));
    }

    #[test]
    fn test_region_config_system() {
        let config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        assert_eq!(config.region, Region::GLOBAL);
        assert!(config.bootstrap);
        assert_eq!(config.initial_members.len(), 1);
    }

    #[test]
    fn test_region_config_data() {
        let members = vec![(1, "127.0.0.1:50051".to_string()), (2, "127.0.0.1:50052".to_string())];
        let config = RegionConfig::data(Region::US_EAST_VA, members);
        assert_eq!(config.region, Region::US_EAST_VA);
        assert!(config.bootstrap);
        assert_eq!(config.initial_members.len(), 2);
    }

    #[test]
    fn test_multi_raft_config_builder() {
        let temp = TestDir::new();
        let config = MultiRaftConfig::builder()
            .data_dir(temp.path().to_path_buf())
            .node_id(42)
            .local_region(Region::GLOBAL)
            .build();
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
            .local_region(Region::GLOBAL)
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
        let from_builder = MultiRaftConfig::builder()
            .data_dir(temp.path().to_path_buf())
            .node_id(1)
            .local_region(Region::GLOBAL)
            .build();
        let from_new = MultiRaftConfig::new(temp.path().to_path_buf(), 1, Region::GLOBAL);
        assert_eq!(from_builder.node_id, from_new.node_id);
        assert_eq!(from_builder.heartbeat_interval_ms, from_new.heartbeat_interval_ms);
        assert_eq!(from_builder.election_timeout_min_ms, from_new.election_timeout_min_ms);
        assert_eq!(from_builder.election_timeout_max_ms, from_new.election_timeout_max_ms);
    }

    #[test]
    fn test_region_config_builder() {
        let config = RegionConfig::builder().region(Region::IE_EAST_DUBLIN).build();
        assert_eq!(config.region, Region::IE_EAST_DUBLIN);
        assert!(config.initial_members.is_empty());
        assert!(config.bootstrap);
        assert!(config.enable_background_jobs);
    }

    #[test]
    fn test_region_config_builder_with_all_fields() {
        let members = vec![(1, "127.0.0.1:50051".to_string())];
        let config = RegionConfig::builder()
            .region(Region::CA_CENTRAL_QC)
            .initial_members(members.clone())
            .bootstrap(false)
            .enable_background_jobs(false)
            .build();
        assert_eq!(config.region, Region::CA_CENTRAL_QC);
        assert_eq!(config.initial_members, members);
        assert!(!config.bootstrap);
        assert!(!config.enable_background_jobs);
    }

    #[test]
    fn test_region_config_without_background_jobs_method() {
        let config =
            RegionConfig::system(1, "127.0.0.1:50051".to_string()).without_background_jobs();
        assert!(!config.enable_background_jobs);
    }

    #[test]
    fn test_manager_creation() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        assert_eq!(manager.list_regions().len(), 0);
        assert!(!manager.has_region(Region::GLOBAL));
    }

    #[test]
    fn test_manager_stats_empty() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        let stats = manager.stats();
        assert_eq!(stats.total_regions, 0);
        assert_eq!(stats.leader_regions, 0);
        assert_eq!(stats.node_id, 1);
    }

    #[test]
    fn test_manager_local_region() {
        let temp = TestDir::new();
        let config =
            MultiRaftConfig::new(temp.path().to_path_buf(), 1, Region::DE_CENTRAL_FRANKFURT);
        let manager = MultiRaftManager::new(config);
        assert_eq!(manager.local_region(), Region::DE_CENTRAL_FRANKFURT);
    }

    #[test]
    fn test_multi_raft_config_local_region_default_global() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        assert_eq!(config.local_region, Region::GLOBAL);
    }

    #[tokio::test]
    async fn test_system_region_required() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Try to start data region without system region
        let region_config =
            RegionConfig::data(Region::US_EAST_VA, vec![(1, "127.0.0.1:50051".to_string())]);
        let result = manager.start_data_region(region_config).await;

        assert!(matches!(result, Err(MultiRaftError::SystemRegionRequired)));
    }

    #[tokio::test]
    async fn test_start_system_region() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        let region_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        let result = manager.start_system_region(region_config).await;

        assert!(result.is_ok(), "start_system_region failed: {:?}", result.err());

        let region = result.unwrap();
        assert_eq!(region.region(), Region::GLOBAL);
        assert!(manager.has_region(Region::GLOBAL));
        assert_eq!(manager.list_regions(), vec![Region::GLOBAL]);
    }

    #[tokio::test]
    async fn test_start_multiple_regions() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Start system region
        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        // Start data region
        let data_config =
            RegionConfig::data(Region::US_EAST_VA, vec![(1, "127.0.0.1:50051".to_string())]);
        manager.start_data_region(data_config).await.expect("start data region");

        assert_eq!(manager.list_regions().len(), 2);
        assert!(manager.has_region(Region::GLOBAL));
        assert!(manager.has_region(Region::US_EAST_VA));
    }

    #[tokio::test]
    async fn test_duplicate_region_error() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Start system region
        let region_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(region_config.clone()).await.expect("start system");

        // Try to start again
        let result = manager.start_system_region(region_config).await;
        assert!(
            matches!(result, Err(MultiRaftError::RegionExists { region }) if region == Region::GLOBAL)
        );
    }

    #[tokio::test]
    async fn test_stop_region() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Start system region
        let region_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(region_config).await.expect("start system");

        assert!(manager.has_region(Region::GLOBAL));

        // Stop region
        manager.stop_region(Region::GLOBAL).await.expect("stop region");

        assert!(!manager.has_region(Region::GLOBAL));
    }

    #[tokio::test]
    async fn test_get_region_group() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Try to get non-existent region
        let result = manager.get_region_group(Region::GLOBAL);
        assert!(
            matches!(result, Err(MultiRaftError::RegionNotFound { region }) if region == Region::GLOBAL)
        );

        // Start and get
        let region_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(region_config).await.expect("start system");

        let region = manager.get_region_group(Region::GLOBAL).expect("get region");
        assert_eq!(region.region(), Region::GLOBAL);

        // system_region() should work too
        let system = manager.system_region().expect("system region");
        assert_eq!(system.region(), Region::GLOBAL);
    }

    #[test]
    fn test_background_jobs_none() {
        let jobs = RegionBackgroundJobs::none();
        // All handles should be None
        assert!(jobs.gc_handle.is_none());
        assert!(jobs.compactor_handle.is_none());
        assert!(jobs.recovery_handle.is_none());
        assert!(jobs.btree_compactor_handle.is_none());
        assert!(jobs.dek_rewrap_handle.is_none());
    }

    #[test]
    fn test_background_jobs_abort_empty() {
        let mut jobs = RegionBackgroundJobs::none();
        // Aborting empty jobs shouldn't panic
        jobs.abort();
        assert!(jobs.gc_handle.is_none());
        assert!(jobs.compactor_handle.is_none());
        assert!(jobs.recovery_handle.is_none());
        assert!(jobs.dek_rewrap_handle.is_none());
    }

    #[tokio::test]
    async fn test_region_with_background_jobs_disabled() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Create config with background jobs disabled
        let mut region_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        region_config.enable_background_jobs = false;

        manager.start_system_region(region_config).await.expect("start system");

        let region = manager.get_region_group(Region::GLOBAL).expect("get region");

        // Background jobs should be None when disabled
        let jobs = region.background_jobs.lock();
        assert!(jobs.gc_handle.is_none());
        assert!(jobs.compactor_handle.is_none());
        assert!(jobs.recovery_handle.is_none());
    }

    #[tokio::test]
    async fn test_region_with_background_jobs_enabled() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Create config with background jobs enabled (default)
        let region_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        assert!(region_config.enable_background_jobs); // Verify default is true

        manager.start_system_region(region_config).await.expect("start system");

        let region = manager.get_region_group(Region::GLOBAL).expect("get region");

        // Background jobs should be Some when enabled
        let jobs = region.background_jobs.lock();
        assert!(jobs.gc_handle.is_some(), "GC job should be started");
        assert!(jobs.compactor_handle.is_some(), "Compactor job should be started");
        assert!(jobs.recovery_handle.is_some(), "Recovery job should be started");
    }

    #[tokio::test]
    async fn test_stop_region_aborts_background_jobs() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Start with background jobs enabled
        let region_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(region_config).await.expect("start system");

        // Verify jobs are running before stop
        {
            let region = manager.get_region_group(Region::GLOBAL).expect("get region");
            let jobs = region.background_jobs.lock();
            assert!(jobs.gc_handle.is_some());
            assert!(jobs.compactor_handle.is_some());
            assert!(jobs.recovery_handle.is_some());
        }

        // Stop region - should abort jobs
        manager.stop_region(Region::GLOBAL).await.expect("stop region");

        // Region should be removed
        assert!(!manager.has_region(Region::GLOBAL));
    }

    #[tokio::test]
    async fn test_protected_region_rejects_insufficient_nodes() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Start system region first (required for data regions)
        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        // IE_EAST_DUBLIN is protected (requires_residency = true)
        // Only 2 members — should fail (minimum is 3)
        let data_config = RegionConfig::data(
            Region::IE_EAST_DUBLIN,
            vec![(1, "127.0.0.1:50051".to_string()), (2, "127.0.0.1:50052".to_string())],
        );
        match manager.start_data_region(data_config).await {
            Err(MultiRaftError::InsufficientNodes { region, required, found }) => {
                assert_eq!(region, Region::IE_EAST_DUBLIN);
                assert_eq!(required, 3);
                assert_eq!(found, 2);
            },
            Err(other) => panic!("Expected InsufficientNodes, got: {other}"),
            Ok(_) => panic!("Expected error, got Ok"),
        }
    }

    #[tokio::test]
    async fn test_protected_region_accepts_sufficient_nodes() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Start system region first
        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        // IE_EAST_DUBLIN with 3 members — should pass validation
        let data_config = RegionConfig::data(
            Region::IE_EAST_DUBLIN,
            vec![
                (1, "127.0.0.1:50051".to_string()),
                (2, "127.0.0.1:50052".to_string()),
                (3, "127.0.0.1:50053".to_string()),
            ],
        );
        // Should pass membership validation (may fail later on Raft bootstrap, which is fine)
        if let Err(MultiRaftError::InsufficientNodes { .. }) =
            manager.start_data_region(data_config).await
        {
            panic!("Should not reject sufficient nodes with InsufficientNodes");
        }
    }

    #[tokio::test]
    async fn test_non_protected_region_accepts_any_member_count() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = MultiRaftManager::new(config);

        // Start system region first
        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        // US_EAST_VA is non-protected (requires_residency = false)
        // Only 1 member — should pass (no minimum for non-protected)
        let data_config =
            RegionConfig::data(Region::US_EAST_VA, vec![(1, "127.0.0.1:50051".to_string())]);
        if let Err(MultiRaftError::InsufficientNodes { .. }) =
            manager.start_data_region(data_config).await
        {
            panic!("Non-protected region should accept any member count");
        }
    }

    #[test]
    fn test_protected_region_empty_members_rejected() {
        // Verify requires_residency correctly identifies protected regions
        assert!(Region::IE_EAST_DUBLIN.requires_residency());
        assert!(Region::DE_CENTRAL_FRANKFURT.requires_residency());
        assert!(Region::JP_EAST_TOKYO.requires_residency());

        // Non-protected
        assert!(!Region::GLOBAL.requires_residency());
        assert!(!Region::US_EAST_VA.requires_residency());
        assert!(!Region::US_WEST_OR.requires_residency());
    }
}
