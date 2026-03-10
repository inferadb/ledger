//! Raft Manager for coordinating multiple independent region groups.
//!
//! InferaDB uses a region-per-Raft-group architecture where
//! each region is an independent Raft consensus group. The `_system` region handles
//! global coordination, while data regions handle organization workloads.
//!
//! ## Architecture
//!
//! ```text
//! RaftManager
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
//! # use inferadb_ledger_raft::raft_manager::{RaftManagerConfig, RaftManager, RegionConfig};
//! # use inferadb_ledger_types::Region;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = RaftManagerConfig::builder()
//!     .data_dir(PathBuf::from("/data"))
//!     .node_id(1u64)
//!     .local_region(Region::GLOBAL)
//!     .build();
//! let manager = RaftManager::new(config);
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
use inferadb_ledger_types::{NodeId, OrganizationId, Region};
use openraft::{BasicNode, Raft, storage::Adaptor};
use parking_lot::RwLock;
use snafu::Snafu;
use tokio::{sync::broadcast, task::JoinHandle};
use tracing::{debug, info, warn};

use crate::{
    auto_recovery::AutoRecoveryJob,
    batching::{BatchWriter, BatchWriterConfig, BatchWriterHandle},
    block_compaction::BlockCompactor,
    btree_compaction::BTreeCompactor,
    dek_rewrap::{DekRewrapJob, RewrapProgress},
    event_writer::EventWriter,
    integrity_scrubber::IntegrityScrubberJob,
    log_storage::{AppliedStateAccessor, RaftLogStore},
    metrics::record_region_node_count,
    raft_network::GrpcRaftNetworkFactory,
    region_router::RegionRouter,
    region_storage::RegionStorageManager,
    ttl_gc::TtlGarbageCollector,
    types::{LedgerNodeId, LedgerRequest, LedgerResponse, LedgerTypeConfig, RaftPayload},
};

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur during multi-raft operations.
#[derive(Debug, Snafu)]
pub enum RaftManagerError {
    /// Region already exists.
    #[snafu(display("Region {region} already exists"))]
    RegionExists { region: Region },

    /// Region not found.
    #[snafu(display("Region {region} not found"))]
    RegionNotFound { region: Region },

    /// Region storage already open (concurrent creation).
    #[snafu(display("Region {region} storage already open"))]
    RegionAlreadyOpen { region: Region },

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
pub type Result<T> = std::result::Result<T, RaftManagerError>;

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

/// Configuration for the Raft Manager.
#[derive(Debug, Clone, bon::Builder)]
pub struct RaftManagerConfig {
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

impl RaftManagerConfig {
    /// Creates a new configuration with default timing parameters.
    pub fn new(data_dir: PathBuf, node_id: LedgerNodeId, local_region: Region) -> Self {
        Self::builder().data_dir(data_dir).node_id(node_id).local_region(local_region).build()
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
    /// Batch writer configuration. When set, the region starts its own
    /// batch writer and exposes a `BatchWriterHandle` on `RegionGroup`.
    pub batch_writer_config: Option<BatchWriterConfig>,
    /// Event writer for apply-phase audit event persistence.
    /// When set, events are recorded into the region's `events.db`.
    pub event_writer: Option<EventWriter<FileBackend>>,
}

impl RegionConfig {
    /// Creates configuration for the system region.
    pub fn system(node_id: LedgerNodeId, address: String) -> Self {
        Self::builder().region(Region::GLOBAL).initial_members(vec![(node_id, address)]).build()
    }

    /// Creates configuration for a data region.
    pub fn data(region: Region, initial_members: Vec<(LedgerNodeId, String)>) -> Self {
        Self::builder().region(region).initial_members(initial_members).build()
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

impl Drop for RegionBackgroundJobs {
    fn drop(&mut self) {
        self.abort();
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
    /// Batch writer handle for coalescing writes (if batch writing is enabled).
    batch_handle: Option<BatchWriterHandle>,
    /// Shared state root commitment buffer.
    ///
    /// Populated by `apply_to_state_machine` after each block, drained by the
    /// leader when constructing the next `RaftPayload` for piggybacked verification.
    commitment_buffer: std::sync::Arc<std::sync::Mutex<Vec<crate::types::StateRootCommitment>>>,
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

    /// Returns the batch writer handle, if batch writing is enabled for this region.
    #[must_use]
    pub fn batch_handle(&self) -> Option<&BatchWriterHandle> {
        self.batch_handle.as_ref()
    }

    /// Drains all buffered state root commitments.
    ///
    /// Called by the leader when constructing `RaftPayload` to piggyback
    /// commitments from the previous apply batch onto the next entry.
    pub fn drain_state_root_commitments(&self) -> Vec<crate::types::StateRootCommitment> {
        std::mem::take(&mut *self.commitment_buffer.lock().unwrap_or_else(|e| e.into_inner()))
    }

    /// Returns the shared commitment buffer handle.
    pub fn commitment_buffer(
        &self,
    ) -> std::sync::Arc<std::sync::Mutex<Vec<crate::types::StateRootCommitment>>> {
        std::sync::Arc::clone(&self.commitment_buffer)
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
// Raft Manager
// ============================================================================

/// Manager for multiple Raft region groups.
///
/// Coordinates the lifecycle of multiple independent Raft consensus groups,
/// each handling a subset of organizations. Uses FileBackend for production storage.
/// Delegates per-region database lifecycle to [`RegionStorageManager`].
pub struct RaftManager {
    /// Configuration.
    config: RaftManagerConfig,
    /// Per-region database storage manager.
    storage_manager: RegionStorageManager,
    /// Active region groups indexed by region ID.
    regions: RwLock<HashMap<Region, Arc<RegionGroup>>>,
    /// Router for organization-to-region resolution.
    router: RwLock<Option<Arc<RegionRouter<FileBackend>>>>,
}

impl RaftManager {
    /// Creates a new Raft Manager.
    pub fn new(config: RaftManagerConfig) -> Self {
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
    pub fn config(&self) -> &RaftManagerConfig {
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
    /// Returns [`RaftManagerError::RegionNotFound`] if no region with the given ID
    /// is currently active.
    pub fn get_region_group(&self, region: Region) -> Result<Arc<RegionGroup>> {
        self.regions.read().get(&region).cloned().ok_or(RaftManagerError::RegionNotFound { region })
    }

    /// Returns the system region (`_system`).
    ///
    /// # Errors
    ///
    /// Returns [`RaftManagerError::RegionNotFound`] if the system region (ID 0)
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

    /// Registers an externally created region group.
    ///
    /// Used by bootstrap code that creates Raft/state resources manually
    /// (before `start_system_region` can be used). The region group is inserted
    /// into the manager's registry and the router is initialized if this is
    /// the system region.
    ///
    /// The `commitment_buffer` must be extracted from the [`RaftLogStore`] via
    /// [`RaftLogStore::commitment_buffer()`] before `Adaptor::new()` consumes
    /// the store. This ensures state root commitments flow from the apply path
    /// to the proposal path.
    ///
    /// # Errors
    ///
    /// Returns [`RaftManagerError::RegionExists`] if the region is already active.
    pub fn register_external_region(
        &self,
        region: Region,
        raft: Arc<Raft<LedgerTypeConfig>>,
        state: Arc<StateLayer<FileBackend>>,
        block_archive: Arc<BlockArchive<FileBackend>>,
        applied_state: AppliedStateAccessor,
        block_announcements: broadcast::Sender<BlockAnnouncement>,
        commitment_buffer: std::sync::Arc<std::sync::Mutex<Vec<crate::types::StateRootCommitment>>>,
    ) -> Result<Arc<RegionGroup>> {
        if self.has_region(region) {
            return Err(RaftManagerError::RegionExists { region });
        }

        let region_group = Arc::new(RegionGroup {
            region,
            raft,
            state: state.clone(),
            block_archive,
            applied_state,
            block_announcements,
            background_jobs: parking_lot::Mutex::new(RegionBackgroundJobs::none()),
            batch_handle: None,
            commitment_buffer,
        });

        {
            let mut regions = self.regions.write();
            regions.insert(region, region_group.clone());
        }

        // Initialize the RegionRouter if this is the system region.
        if region == Region::GLOBAL {
            let system_service = Arc::new(SystemOrganizationService::new(state));
            let router = Arc::new(RegionRouter::new(system_service, self.config.local_region));
            *self.router.write() = Some(router);
            info!("RegionRouter initialized with _system organization (external registration)");
        }

        Ok(region_group)
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
    /// Returns [`RaftManagerError::Raft`] if `region` is not 0,
    /// [`RaftManagerError::RegionExists`] if the region is already running,
    /// or a storage/Raft error if initialization fails.
    pub async fn start_system_region(
        &self,
        region_config: RegionConfig,
    ) -> Result<Arc<RegionGroup>> {
        if region_config.region != Region::GLOBAL {
            return Err(RaftManagerError::Raft {
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
    /// Returns [`RaftManagerError::SystemRegionRequired`] if the system region has
    /// not been started, [`RaftManagerError::Raft`] if `region` is 0,
    /// [`RaftManagerError::RegionExists`] if the region is already running,
    /// or a storage/Raft error if initialization fails.
    pub async fn start_data_region(&self, region_config: RegionConfig) -> Result<Arc<RegionGroup>> {
        // Verify system region is running
        if !self.has_region(Region::GLOBAL) {
            return Err(RaftManagerError::SystemRegionRequired);
        }

        if region_config.region == Region::GLOBAL {
            return Err(RaftManagerError::Raft {
                region: Region::GLOBAL,
                message: "Use start_system_region for region=0".to_string(),
            });
        }

        self.start_region(region_config).await
    }

    /// Ensures a data region is active, creating it lazily if needed.
    ///
    /// Returns the existing `RegionGroup` if the region is already running.
    /// Otherwise creates the region with the provided config. This is the
    /// entry point for lazy Raft group creation: the first organization or
    /// user assigned to a region triggers group creation.
    ///
    /// Thread-safe: concurrent calls for the same region are handled via
    /// fallback — if `start_region` fails because the region already exists
    /// (either via `RegionExists` or a storage `AlreadyOpen` error from a
    /// concurrent opener), we fall through to `get_region_group`.
    ///
    /// # Errors
    ///
    /// Returns [`RaftManagerError::SystemRegionRequired`] if the system region
    /// has not been started, or a storage/Raft error if initialization fails.
    /// Returns `(group, created)` where `created` is `true` if this call
    /// actually created the region, `false` if it already existed.
    pub async fn ensure_data_region(
        &self,
        region_config: RegionConfig,
    ) -> Result<(Arc<RegionGroup>, bool)> {
        let region = region_config.region;

        // GLOBAL is the control plane — always created eagerly via start_system_region
        if region == Region::GLOBAL {
            return Ok((self.get_region_group(Region::GLOBAL)?, false));
        }

        // Fast path: region already running
        if let Ok(group) = self.get_region_group(region) {
            return Ok((group, false));
        }

        // Verify system region is running
        if !self.has_region(Region::GLOBAL) {
            return Err(RaftManagerError::SystemRegionRequired);
        }

        // Attempt to start — if a concurrent caller beat us, fall through.
        // Two error variants can indicate concurrency:
        // - RegionExists: start_region's has_region check saw it after map insert
        // - RegionAlreadyOpen: RegionStorageManager rejected a second opener
        match self.start_region(region_config).await {
            Ok(group) => {
                info!(region = region.as_str(), "Lazily created region group");
                Ok((group, true))
            },
            Err(
                RaftManagerError::RegionExists { .. } | RaftManagerError::RegionAlreadyOpen { .. },
            ) => {
                // A concurrent caller is creating this region. RegionAlreadyOpen fires
                // from the storage layer *before* the winner finishes initialization and
                // inserts into the regions map. Poll briefly for the winner to complete.
                for _ in 0..50 {
                    if let Ok(group) = self.get_region_group(region) {
                        return Ok((group, false));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                // Winner still hasn't finished — report the original error rather than
                // masking a potential initialization failure.
                Err(RaftManagerError::RegionExists { region })
            },
            Err(e) => Err(e),
        }
    }

    /// Starts a region group.
    async fn start_region(&self, region_config: RegionConfig) -> Result<Arc<RegionGroup>> {
        // Destructure config upfront to avoid partial-move issues
        let RegionConfig {
            region,
            initial_members,
            bootstrap,
            enable_background_jobs,
            batch_writer_config,
            event_writer,
        } = region_config;

        // Check if region already exists (early exit for the common case).
        // Note: this check-then-insert has a TOCTOU window — concurrent callers
        // may both pass this check. RegionStorageManager::open_region provides the
        // true guard (returns AlreadyOpen), and ensure_data_region handles both
        // RegionExists and RegionAlreadyOpen gracefully.
        if self.has_region(region) {
            return Err(RaftManagerError::RegionExists { region });
        }

        let is_protected = region.requires_residency();

        // Protected regions enforce minimum in-region node count
        if is_protected && initial_members.len() < MIN_NODES_PER_PROTECTED_REGION {
            return Err(RaftManagerError::InsufficientNodes {
                region,
                required: MIN_NODES_PER_PROTECTED_REGION,
                found: initial_members.len(),
            });
        }

        // Emit region node count metric
        record_region_node_count(region.as_str(), initial_members.len(), is_protected);

        info!(region = region.as_str(), "Starting region group");

        // Create divergence channel for state root verification.
        // The sender is passed into RaftLogStore; the receiver drives the handler task.
        let (divergence_sender, divergence_receiver) = tokio::sync::mpsc::unbounded_channel();

        // Open storage via RegionStorageManager (creates directory + databases + RaftLogStore)
        let (state, block_archive, log_store, block_announcements) =
            self.open_region_storage(region, event_writer, divergence_sender)?;

        // Get accessor and commitment buffer before log_store is consumed by Adaptor
        let applied_state = log_store.accessor();
        let commitment_buffer = log_store.commitment_buffer();

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
        .map_err(|e| RaftManagerError::Raft {
            region,
            message: format!("Failed to create Raft instance: {}", e),
        })?;

        let raft = Arc::new(raft);

        // Bootstrap if configured
        if bootstrap && !initial_members.is_empty() {
            self.bootstrap_region(&raft, region, &initial_members).await?;
        }

        // Create batch writer if configured
        let batch_handle = if let Some(batch_config) = batch_writer_config {
            let raft_clone = raft.clone();
            let buffer_clone = commitment_buffer.clone();
            let submit_fn = move |requests: Vec<LedgerRequest>| {
                let raft = raft_clone.clone();
                let buffer = buffer_clone.clone();
                Box::pin(async move {
                    let batch_request = LedgerRequest::BatchWrite { requests };
                    let commitments =
                        std::mem::take(&mut *buffer.lock().unwrap_or_else(|e| e.into_inner()));
                    let result = raft
                        .client_write(RaftPayload::with_commitments(batch_request, commitments))
                        .await;
                    match result {
                        Ok(response) => match response.data {
                            LedgerResponse::BatchWrite { responses } => Ok(responses),
                            other => Ok(vec![other]),
                        },
                        Err(e) => Err(format!("Raft error: {}", e)),
                    }
                })
                    as futures::future::BoxFuture<
                        'static,
                        std::result::Result<Vec<LedgerResponse>, String>,
                    >
            };

            let writer = BatchWriter::new(batch_config, submit_fn, region.to_string());
            let handle = writer.handle();
            tokio::spawn(writer.run());
            Some(handle)
        } else {
            None
        };

        // Start background jobs if enabled
        let background_jobs = if enable_background_jobs {
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

        // Spawn state root divergence handler — halts vaults on mismatch.
        // Runs alongside AutoRecoveryJob and other background workers.
        let divergence_handler = crate::state_root_verifier::StateRootDivergenceHandler::new(
            raft.clone(),
            divergence_receiver,
            region.to_string(),
        );
        tokio::spawn(divergence_handler.run());

        // Create region group
        let region_group = Arc::new(RegionGroup {
            region,
            raft,
            state,
            block_archive,
            applied_state,
            block_announcements,
            background_jobs: parking_lot::Mutex::new(background_jobs),
            batch_handle,
            commitment_buffer,
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
    /// An optional [`EventWriter`] can be provided for apply-phase audit event persistence.
    fn open_region_storage(
        &self,
        region: Region,
        event_writer: Option<EventWriter<FileBackend>>,
        divergence_sender: tokio::sync::mpsc::UnboundedSender<crate::types::StateRootDivergence>,
    ) -> Result<OpenedRegionStorage> {
        // Open databases via storage manager (creates directory + state.db, blocks.db, events.db)
        let storage = self.storage_manager.open_region(region).map_err(|e| {
            if matches!(e, crate::region_storage::RegionStorageError::AlreadyOpen { .. }) {
                RaftManagerError::RegionAlreadyOpen { region }
            } else {
                RaftManagerError::Storage { region, message: format!("{e}") }
            }
        })?;

        // Wrap raw databases in domain-specific types
        let state = Arc::new(StateLayer::new(storage.state_db().clone()));
        let block_archive = Arc::new(BlockArchive::new(storage.blocks_db().clone()));

        // Create block announcements broadcast channel for real-time notifications.
        // Buffer size of 1024 allows for burst handling during high commit rates.
        let (block_announcements, _) = broadcast::channel(1024);

        // Open Raft log store (uses inferadb-ledger-store storage - handles open/create internally)
        let log_path = self.storage_manager.raft_db_path(region);
        let mut log_store = RaftLogStore::<FileBackend>::open(&log_path)
            .map_err(|e| RaftManagerError::Storage {
                region,
                message: format!("Failed to open log store: {e}"),
            })?
            .with_state_layer(state.clone())
            .with_block_archive(block_archive.clone())
            .with_region_config(region, NodeId::new(self.config.node_id.to_string()))
            .with_block_announcements(block_announcements.clone())
            .with_divergence_sender(divergence_sender);

        // Wire event writer if provided
        if let Some(writer) = event_writer {
            log_store = log_store.with_event_writer(writer);
        }

        Ok((state, block_archive, log_store, block_announcements))
    }

    /// Bootstraps a region as a new cluster.
    async fn bootstrap_region(
        &self,
        raft: &Raft<LedgerTypeConfig>,
        region: Region,
        initial_members: &[(LedgerNodeId, String)],
    ) -> Result<()> {
        use std::collections::BTreeMap;

        let mut members: BTreeMap<LedgerNodeId, BasicNode> = BTreeMap::new();
        for (node_id, addr) in initial_members {
            members.insert(*node_id, BasicNode { addr: addr.clone() });
        }

        raft.initialize(members).await.map_err(|e| RaftManagerError::Raft {
            region,
            message: format!("Failed to initialize: {}", e),
        })?;

        info!(
            region = region.as_str(),
            members = initial_members.len(),
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
    /// Returns [`RaftManagerError::RegionNotFound`] if no region with the given ID
    /// is currently active.
    pub async fn stop_region(&self, region: Region) -> Result<()> {
        let region_group = {
            let mut regions = self.regions.write();
            regions.remove(&region).ok_or(RaftManagerError::RegionNotFound { region })?
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

        info!("Raft Manager shutdown complete");
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
    pub fn stats(&self) -> RaftManagerStats {
        let regions = self.regions.read();
        let mut leader_count = 0;

        for region in regions.values() {
            if region.is_leader(self.config.node_id) {
                leader_count += 1;
            }
        }

        RaftManagerStats {
            total_regions: regions.len(),
            leader_regions: leader_count,
            node_id: self.config.node_id,
        }
    }
}

/// Statistics about the Raft Manager.
#[derive(Debug, Clone)]
pub struct RaftManagerStats {
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

    fn create_test_config(temp_dir: &TestDir) -> RaftManagerConfig {
        RaftManagerConfig::new(temp_dir.path().to_path_buf(), 1, Region::GLOBAL)
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
    fn test_raft_manager_config_builder() {
        let temp = TestDir::new();
        let config = RaftManagerConfig::builder()
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
    fn test_raft_manager_config_builder_custom_timeouts() {
        let temp = TestDir::new();
        let config = RaftManagerConfig::builder()
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
    fn test_raft_manager_config_builder_matches_new() {
        let temp = TestDir::new();
        let from_builder = RaftManagerConfig::builder()
            .data_dir(temp.path().to_path_buf())
            .node_id(1)
            .local_region(Region::GLOBAL)
            .build();
        let from_new = RaftManagerConfig::new(temp.path().to_path_buf(), 1, Region::GLOBAL);
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
        let manager = RaftManager::new(config);

        assert_eq!(manager.list_regions().len(), 0);
        assert!(!manager.has_region(Region::GLOBAL));
    }

    #[test]
    fn test_manager_stats_empty() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = RaftManager::new(config);

        let stats = manager.stats();
        assert_eq!(stats.total_regions, 0);
        assert_eq!(stats.leader_regions, 0);
        assert_eq!(stats.node_id, 1);
    }

    #[test]
    fn test_manager_local_region() {
        let temp = TestDir::new();
        let config =
            RaftManagerConfig::new(temp.path().to_path_buf(), 1, Region::DE_CENTRAL_FRANKFURT);
        let manager = RaftManager::new(config);
        assert_eq!(manager.local_region(), Region::DE_CENTRAL_FRANKFURT);
    }

    #[test]
    fn test_raft_manager_config_local_region_default_global() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        assert_eq!(config.local_region, Region::GLOBAL);
    }

    #[tokio::test]
    async fn test_system_region_required() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = RaftManager::new(config);

        // Try to start data region without system region
        let region_config =
            RegionConfig::data(Region::US_EAST_VA, vec![(1, "127.0.0.1:50051".to_string())]);
        let result = manager.start_data_region(region_config).await;

        assert!(matches!(result, Err(RaftManagerError::SystemRegionRequired)));
    }

    #[tokio::test]
    async fn test_start_system_region() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = RaftManager::new(config);

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
        let manager = RaftManager::new(config);

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
        let manager = RaftManager::new(config);

        // Start system region
        let region_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(region_config.clone()).await.expect("start system");

        // Try to start again
        let result = manager.start_system_region(region_config).await;
        assert!(
            matches!(result, Err(RaftManagerError::RegionExists { region }) if region == Region::GLOBAL)
        );
    }

    #[tokio::test]
    async fn test_stop_region() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = RaftManager::new(config);

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
        let manager = RaftManager::new(config);

        // Try to get non-existent region
        let result = manager.get_region_group(Region::GLOBAL);
        assert!(
            matches!(result, Err(RaftManagerError::RegionNotFound { region }) if region == Region::GLOBAL)
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
        assert!(jobs.integrity_scrubber_handle.is_none());
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
        assert!(jobs.btree_compactor_handle.is_none());
        assert!(jobs.integrity_scrubber_handle.is_none());
        assert!(jobs.dek_rewrap_handle.is_none());
    }

    #[tokio::test]
    async fn test_region_with_background_jobs_disabled() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = RaftManager::new(config);

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
        let manager = RaftManager::new(config);

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
        let manager = RaftManager::new(config);

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
        let manager = RaftManager::new(config);

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
            Err(RaftManagerError::InsufficientNodes { region, required, found }) => {
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
        let manager = RaftManager::new(config);

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
        if let Err(RaftManagerError::InsufficientNodes { .. }) =
            manager.start_data_region(data_config).await
        {
            panic!("Should not reject sufficient nodes with InsufficientNodes");
        }
    }

    #[tokio::test]
    async fn test_non_protected_region_accepts_any_member_count() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = RaftManager::new(config);

        // Start system region first
        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        // US_EAST_VA is non-protected (requires_residency = false)
        // Only 1 member — should pass (no minimum for non-protected)
        let data_config =
            RegionConfig::data(Region::US_EAST_VA, vec![(1, "127.0.0.1:50051".to_string())]);
        if let Err(RaftManagerError::InsufficientNodes { .. }) =
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

    #[tokio::test]
    async fn test_ensure_data_region_creates_new() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = RaftManager::new(config);

        // Start system region first
        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        // ensure_data_region should create a new region
        let region_config =
            RegionConfig::data(Region::US_EAST_VA, vec![(1, "127.0.0.1:50051".to_string())]);
        let result = manager.ensure_data_region(region_config).await;

        assert!(result.is_ok(), "ensure_data_region failed: {:?}", result.err());
        let (_group, created) = result.expect("just asserted ok");
        assert!(created, "should report created=true for new region");
        assert!(manager.has_region(Region::US_EAST_VA));
        assert_eq!(manager.list_regions().len(), 2);
    }

    #[tokio::test]
    async fn test_ensure_data_region_returns_existing() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = RaftManager::new(config);

        // Start system region and a data region
        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let data_config =
            RegionConfig::data(Region::US_EAST_VA, vec![(1, "127.0.0.1:50051".to_string())]);
        manager.start_data_region(data_config).await.expect("start data");

        // ensure_data_region should return the existing group
        let region_config =
            RegionConfig::data(Region::US_EAST_VA, vec![(1, "127.0.0.1:50051".to_string())]);
        let result = manager.ensure_data_region(region_config).await;

        assert!(result.is_ok(), "ensure_data_region on existing failed: {:?}", result.err());
        let (_group, created) = result.expect("just asserted ok");
        assert!(!created, "should report created=false for existing region");
        assert_eq!(manager.list_regions().len(), 2); // Still 2 regions
    }

    #[tokio::test]
    async fn test_ensure_data_region_requires_system() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = RaftManager::new(config);

        // Without system region, ensure_data_region should fail
        let region_config =
            RegionConfig::data(Region::US_EAST_VA, vec![(1, "127.0.0.1:50051".to_string())]);
        let result = manager.ensure_data_region(region_config).await;

        assert!(matches!(result, Err(RaftManagerError::SystemRegionRequired)));
    }

    #[tokio::test]
    async fn test_ensure_data_region_global_returns_system() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = RaftManager::new(config);

        // Before system region: GLOBAL should return RegionNotFound
        let region_config =
            RegionConfig::data(Region::GLOBAL, vec![(1, "127.0.0.1:50051".to_string())]);
        let result = manager.ensure_data_region(region_config).await;
        assert!(matches!(result, Err(RaftManagerError::RegionNotFound { .. })));

        // After system region: GLOBAL should return the existing system group
        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let region_config =
            RegionConfig::data(Region::GLOBAL, vec![(1, "127.0.0.1:50051".to_string())]);
        let result = manager.ensure_data_region(region_config).await;
        assert!(result.is_ok());
        let (group, created) = result.expect("just asserted ok");
        assert!(!created, "GLOBAL should never be reported as newly created");
        assert_eq!(group.region(), Region::GLOBAL);
    }

    #[tokio::test]
    async fn test_discover_and_reopen_regions_on_restart() {
        let temp = TestDir::new();

        // First "session": create regions
        {
            let config = create_test_config(&temp);
            let manager = RaftManager::new(config);

            let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
            manager.start_system_region(system_config).await.expect("start system");

            let data_config =
                RegionConfig::data(Region::US_EAST_VA, vec![(1, "127.0.0.1:50051".to_string())]);
            manager.start_data_region(data_config).await.expect("start us-east-va");

            // Shutdown
            manager.shutdown().await;
        }

        // Second "session": discover and reopen
        {
            let config = create_test_config(&temp);
            let manager = RaftManager::new(config);

            // Discover regions that have existing data on disk
            let existing = manager.storage_manager().discover_existing_regions();

            // Should find the data region we created
            assert_eq!(existing.len(), 1);
            assert!(existing.contains(&Region::US_EAST_VA));

            // Re-open system region (no bootstrap on restart)
            let mut system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
            system_config.bootstrap = false;
            manager.start_system_region(system_config).await.expect("restart system");

            // Re-open discovered regions (no bootstrap on restart)
            for region in &existing {
                let mut region_config =
                    RegionConfig::data(*region, vec![(1, "127.0.0.1:50051".to_string())]);
                region_config.bootstrap = false;
                manager.start_data_region(region_config).await.expect("restart data region");
            }

            // Both regions should be active
            assert_eq!(manager.list_regions().len(), 2);
            assert!(manager.has_region(Region::GLOBAL));
            assert!(manager.has_region(Region::US_EAST_VA));
        }
    }

    /// Verifies that concurrent `ensure_data_region` calls for the same region
    /// are handled safely via defense-in-depth (RegionStorageManager's AlreadyOpen
    /// guard + ensure_data_region's error fallthrough).
    ///
    /// This exercises the TOCTOU window in `start_region` where two callers can
    /// both pass `has_region` before either inserts. The storage layer rejects
    /// the second opener, and `ensure_data_region` treats that as "already exists."
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_ensure_data_region_is_safe() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = Arc::new(RaftManager::new(config));

        // System region must exist first
        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let concurrency = 10;
        let barrier = Arc::new(tokio::sync::Barrier::new(concurrency));

        let mut handles = Vec::new();
        for _ in 0..concurrency {
            let mgr = Arc::clone(&manager);
            let bar = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                // Synchronize so all tasks attempt creation at roughly the same time
                bar.wait().await;
                let region_config = RegionConfig::data(
                    Region::US_EAST_VA,
                    vec![(1, "127.0.0.1:50051".to_string())],
                );
                mgr.ensure_data_region(region_config).await
            }));
        }

        let mut created_count = 0;
        for handle in handles {
            let result = handle.await.expect("task panicked");
            let (group, created) = result.expect("ensure_data_region failed");
            assert_eq!(group.region(), Region::US_EAST_VA);
            if created {
                created_count += 1;
            }
        }

        // Exactly one caller should have created the region
        assert_eq!(created_count, 1, "expected exactly 1 creator, got {created_count}");
        // Region should be registered exactly once
        assert!(manager.has_region(Region::US_EAST_VA));
        assert_eq!(manager.list_regions().len(), 2); // GLOBAL + US_EAST_VA
    }
}
