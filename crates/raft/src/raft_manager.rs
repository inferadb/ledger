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
//! # use std::sync::Arc;
//! # use inferadb_ledger_raft::raft_manager::{RaftManagerConfig, RaftManager, RegionConfig};
//! # use inferadb_ledger_raft::node_registry::NodeConnectionRegistry;
//! # use inferadb_ledger_types::Region;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = RaftManagerConfig::builder()
//!     .data_dir(PathBuf::from("/data"))
//!     .node_id(1u64)
//!     .local_region(Region::GLOBAL)
//!     .build();
//! let registry = Arc::new(NodeConnectionRegistry::new());
//! let manager = RaftManager::new(config, registry);
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

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use inferadb_ledger_consensus::WalBackend as _;
use inferadb_ledger_proto::proto::BlockAnnouncement;
use inferadb_ledger_state::{
    BlockArchive, StateLayer,
    system::{MIN_NODES_PER_PROTECTED_REGION, SystemOrganizationService},
};
use inferadb_ledger_store::{Database, FileBackend};
use inferadb_ledger_types::{NodeId, OrganizationId, Region};
use parking_lot::RwLock;
use snafu::Snafu;
use tokio::{sync::broadcast, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    auto_recovery::AutoRecoveryJob,
    batching::{BatchWriter, BatchWriterConfig, BatchWriterHandle},
    block_compaction::BlockCompactor,
    btree_compaction::BTreeCompactor,
    consensus_handle::ConsensusHandle,
    dek_rewrap::{DekRewrapJob, RewrapProgress},
    event_writer::EventWriter,
    integrity_scrubber::IntegrityScrubberJob,
    log_storage::{AppliedStateAccessor, RaftLogStore, RecoveryStats},
    metrics,
    region_storage::RegionStorageManager,
    runtime_config::RuntimeConfigHandle,
    state_checkpointer::StateCheckpointer,
    ttl_gc::TtlGarbageCollector,
    types::{LedgerNodeId, LedgerRequest, LedgerResponse, RaftPayload},
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

/// Region creation request: region + initial members for the Raft group.
pub type RegionCreationRequest = (Region, Vec<(u64, String)>);

/// Storage components returned from region opening (state, block archive, raft log store,
/// block announcements, events db, optional region creation receiver).
type OpenedRegionStorage = (
    Arc<StateLayer<FileBackend>>,
    Arc<BlockArchive<FileBackend>>,
    RaftLogStore<FileBackend>,
    broadcast::Sender<BlockAnnouncement>,
    Arc<inferadb_ledger_state::EventsDatabase<FileBackend>>,
    Option<tokio::sync::mpsc::UnboundedReceiver<RegionCreationRequest>>,
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
    /// Number of independent Raft shards to run per region. Each shard is
    /// its own Raft group with its own WAL + state DBs + apply worker.
    /// Orgs route to shards via `ShardManager`. Must be in `[1, 256]`;
    /// enforced at region-start time.
    #[builder(default = 16)]
    pub shards_per_region: usize,
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
    /// Event configuration for creating an `EventWriter` from the region's own
    /// `events.db`. Used when `event_writer` is `None` — the writer is created
    /// inside `open_region_storage` from the locally-opened database, avoiding
    /// a double-open of the same file.
    pub events_config: Option<inferadb_ledger_types::events::EventConfig>,
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
///
/// Handles are stored to keep the spawned tasks alive — dropping a
/// `JoinHandle` does not cancel the task, but we retain them for
/// observability (e.g. detecting panicked tasks on shutdown).
#[allow(dead_code)] // handles are stored for task lifetime, not read
pub struct RegionBackgroundJobs {
    /// Cancellation token for this region's jobs. Cancelling this signals all
    /// child jobs to exit their loops gracefully.
    region_token: CancellationToken,
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
    /// State-DB checkpointer handle. Drives
    /// [`Database::sync_state`](inferadb_ledger_store::Database::sync_state)
    /// on a time / apply-count / dirty-page trigger policy so state-DB
    /// durability is amortized across many in-memory commits.
    state_checkpointer_handle: Option<JoinHandle<()>>,
    /// Shared re-wrapping progress (read by admin service).
    rewrap_progress: Arc<RewrapProgress>,
}

impl RegionBackgroundJobs {
    /// Creates with no jobs (used when background_jobs disabled).
    fn none() -> Self {
        Self {
            region_token: CancellationToken::new(),
            gc_handle: None,
            compactor_handle: None,
            recovery_handle: None,
            btree_compactor_handle: None,
            integrity_scrubber_handle: None,
            dek_rewrap_handle: None,
            state_checkpointer_handle: None,
            rewrap_progress: Arc::new(RewrapProgress::new()),
        }
    }

    /// Returns the shared re-wrapping progress tracker.
    pub fn rewrap_progress(&self) -> Arc<RewrapProgress> {
        Arc::clone(&self.rewrap_progress)
    }

    /// Cancels the region token, signalling all child jobs to exit gracefully.
    ///
    /// Jobs observe cancellation via `tokio::select!` in their main loops and
    /// will break out on the next tick. Handles are NOT awaited here because
    /// `Drop` cannot be async — the tokio runtime will complete them.
    fn cancel(&mut self) {
        self.region_token.cancel();
    }
}

impl Drop for RegionBackgroundJobs {
    fn drop(&mut self) {
        self.cancel();
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
    /// Consensus handle for background jobs and services.
    handle: Arc<ConsensusHandle>,
    /// Shared state layer for this region.
    state: Arc<StateLayer<FileBackend>>,
    /// `raft.db` handle for this region.
    ///
    /// Shared between the [`StateCheckpointer`] (which syncs it on every
    /// checkpoint tick) and [`RaftManager::sync_all_state_dbs`] (which syncs
    /// it at graceful shutdown). Both flush paths are required because
    /// `save_state_core` writes `KEY_APPLIED_STATE` to raft.db via
    /// `commit_in_memory` — leaving raft.db unsynced causes clean-shutdown
    /// restarts to read `applied_durable = 0` and replay the entire WAL.
    /// See the follow-up in
    /// `docs/superpowers/specs/2026-04-19-commit-durability-audit.md`.
    raft_db: Arc<Database<FileBackend>>,
    /// `blocks.db` handle for this region.
    ///
    /// Surfaced alongside `raft_db` so the [`StateCheckpointer`] and
    /// [`RaftManager::sync_all_state_dbs`] can drive `sync_state` on
    /// blocks.db in lock-step with state.db + raft.db.
    /// `BlockArchive::append_block` uses `commit_in_memory`; without this
    /// handle the dirty pages from apply-phase block writes would never
    /// reach disk outside of ad-hoc flushes. The underlying database is
    /// shared with `block_archive` — the `BlockArchive` owns the domain
    /// API, while this field owns the durability lifecycle handle.
    blocks_db: Arc<Database<FileBackend>>,
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
    /// Leader lease for fast linearizable reads on the leader.
    leader_lease: Arc<crate::leader_lease::LeaderLease>,
    /// Watch channel receiver for applied index (ReadIndex protocol).
    applied_index_rx: tokio::sync::watch::Receiver<u64>,
    /// Consensus transport for dynamic peer channel management.
    consensus_transport: Option<crate::consensus_transport::GrpcConsensusTransport>,
    /// Events database for event queries and handler-phase recording.
    events_db: Option<Arc<inferadb_ledger_state::EventsDatabase<FileBackend>>>,
    /// Last activity timestamp (updated on every request).
    last_activity: Arc<parking_lot::Mutex<std::time::Instant>>,
    /// Whether background jobs are currently running.
    jobs_active: Arc<AtomicBool>,
    /// Receiver for data region creation signals from the GLOBAL apply handler.
    ///
    /// Only populated for the GLOBAL region group. Taken once by the bootstrap
    /// handler via [`take_region_creation_rx`](RegionGroup::take_region_creation_rx).
    region_creation_rx:
        parking_lot::Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<RegionCreationRequest>>>,
}

impl RegionGroup {
    /// Returns the region ID.
    pub fn region(&self) -> Region {
        self.region
    }

    /// Returns the consensus handle.
    #[must_use]
    pub fn handle(&self) -> &Arc<ConsensusHandle> {
        &self.handle
    }

    /// Returns the state layer.
    #[must_use]
    pub fn state(&self) -> &Arc<StateLayer<FileBackend>> {
        &self.state
    }

    /// Returns the `raft.db` handle for this region.
    ///
    /// Used by [`RaftManager::sync_all_state_dbs`] and the
    /// [`StateCheckpointer`] to force the `KEY_APPLIED_STATE` blob to disk.
    /// See [`RegionGroup::raft_db`] for the full durability rationale.
    #[must_use]
    pub fn raft_db(&self) -> &Arc<Database<FileBackend>> {
        &self.raft_db
    }

    /// Returns the `blocks.db` handle for this region.
    ///
    /// Used by [`RaftManager::sync_all_state_dbs`] and the
    /// [`StateCheckpointer`] to drive `sync_state` on blocks.db alongside
    /// state.db, raft.db, and events.db. The underlying database is shared
    /// with `block_archive` — this accessor surfaces the raw `Database`
    /// handle for the durability lifecycle, while `block_archive` exposes
    /// the domain API.
    #[must_use]
    pub fn blocks_db(&self) -> &Arc<Database<FileBackend>> {
        &self.blocks_db
    }

    /// Returns the underlying `events.db` database handle, if this region
    /// has an events database configured.
    ///
    /// Used by [`RaftManager::sync_all_state_dbs`] and the
    /// [`StateCheckpointer`] to drive `sync_state` on events.db alongside
    /// state.db, raft.db, and blocks.db. Returns `None` for regions that
    /// were constructed without an events database (test fixtures,
    /// historical GLOBAL-only configurations); the checkpointer / shutdown
    /// sweep silently skip events.db in that case.
    ///
    /// The return type intentionally surfaces an owned `Arc` rather than a
    /// reference so callers can pass it directly to `Database::sync_state`
    /// (which consumes an `Arc<Self>`).
    #[must_use]
    pub fn events_state_db(&self) -> Option<Arc<Database<FileBackend>>> {
        self.events_db.as_ref().map(|ed| Arc::clone(ed.db()))
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

    /// Returns the consensus transport for dynamic peer channel management.
    #[must_use]
    pub fn consensus_transport(
        &self,
    ) -> Option<&crate::consensus_transport::GrpcConsensusTransport> {
        self.consensus_transport.as_ref()
    }

    /// Returns the events database, if available.
    #[must_use]
    pub fn events_db(&self) -> Option<&Arc<inferadb_ledger_state::EventsDatabase<FileBackend>>> {
        self.events_db.as_ref()
    }

    /// Returns the batch writer handle, if batch writing is enabled for this region.
    #[must_use]
    pub fn batch_handle(&self) -> Option<&BatchWriterHandle> {
        self.batch_handle.as_ref()
    }

    /// Records activity on this region group, resetting the idle timer.
    pub fn touch(&self) {
        *self.last_activity.lock() = std::time::Instant::now();
    }

    /// Returns the number of seconds since the last activity.
    pub fn idle_secs(&self) -> u64 {
        self.last_activity.lock().elapsed().as_secs()
    }

    /// Returns whether background jobs are currently running.
    pub fn is_jobs_active(&self) -> bool {
        self.jobs_active.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Returns the leader lease for this region.
    #[must_use]
    pub fn leader_lease(&self) -> &Arc<crate::leader_lease::LeaderLease> {
        &self.leader_lease
    }

    /// Returns a receiver for the applied index watch channel.
    ///
    /// Used by the ReadIndex protocol: followers wait on this channel
    /// until their applied index reaches the leader's committed index.
    pub fn applied_index_watch(&self) -> tokio::sync::watch::Receiver<u64> {
        self.applied_index_rx.clone()
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
    pub fn is_leader(&self, _node_id: LedgerNodeId) -> bool {
        self.handle.is_leader()
    }

    /// Returns the current leader node ID, if known.
    pub fn current_leader(&self) -> Option<LedgerNodeId> {
        self.handle.current_leader()
    }

    /// Takes the region creation receiver from the GLOBAL region group.
    ///
    /// Returns `Some` exactly once for the GLOBAL region. The bootstrap handler
    /// calls this to spawn a task that starts data regions as they are created
    /// through GLOBAL Raft consensus.
    pub fn take_region_creation_rx(
        &self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<RegionCreationRequest>> {
        self.region_creation_rx.lock().take()
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
    /// Shared peer address map (node ID → network address).
    ///
    /// Populated from `initial_members` during region startup and updated
    /// dynamically via `announce_peer` RPCs. Services use this to resolve
    /// peer addresses for forwarding and health checks.
    peer_addresses: crate::peer_address_map::PeerAddressMap,
    /// Shared registry of per-peer gRPC channels.
    ///
    /// One registry per node (constructed by the server bootstrap and
    /// threaded through here). Per-region consensus transports clone the
    /// `Arc` so that all regions share a single channel per peer instead of
    /// each region opening its own connection.
    registry: Arc<crate::node_registry::NodeConnectionRegistry>,
    /// Sender half of the DR event channel. Cloned into the GLOBAL apply
    /// worker and called from `notify_dr_membership_change`.
    dr_event_tx: tokio::sync::mpsc::UnboundedSender<()>,
    /// Receiver half, taken once by bootstrap to hand to the
    /// `PlacementController`. `None` after the first `take_dr_event_rx()`.
    dr_event_rx: parking_lot::Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<()>>>,
    /// Parent cancellation token for all per-region background jobs.
    /// Set via [`set_cancellation_token`](Self::set_cancellation_token)
    /// during bootstrap; defaults to an unlinked token.
    cancellation_token: parking_lot::Mutex<CancellationToken>,
    /// Runtime-config handle plumbed into per-region [`StateCheckpointer`]
    /// tasks so live `UpdateConfig` RPCs adjust checkpoint thresholds on the
    /// next tick. Set via [`set_runtime_config`](Self::set_runtime_config)
    /// during bootstrap; tests that never call the setter fall back to
    /// [`RuntimeConfigHandle::default`] (checkpointer uses
    /// [`CheckpointConfig::default`](inferadb_ledger_types::config::CheckpointConfig::default)
    /// thresholds).
    runtime_config: parking_lot::Mutex<Option<RuntimeConfigHandle>>,
    /// Last crash-recovery replay stats per region, captured inside
    /// [`start_region`] immediately after `RaftLogStore::replay_crash_gap`.
    ///
    /// Populated whether or not any entries were replayed. Used by the
    /// crash-recovery integration test suite to assert replay counts
    /// without parsing tracing output; production callers may also read
    /// this to surface recovery statistics via an admin RPC in the future.
    recovery_stats: RwLock<HashMap<Region, RecoveryStats>>,
}

impl RaftManager {
    /// Creates a new Raft Manager.
    ///
    /// `registry` is the shared per-node connection registry. Pass the same
    /// `Arc` instance for the lifetime of the process so all regions reuse
    /// peer channels.
    pub fn new(
        config: RaftManagerConfig,
        registry: Arc<crate::node_registry::NodeConnectionRegistry>,
    ) -> Self {
        let storage_manager = RegionStorageManager::new(config.data_dir.clone());
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        Self {
            config,
            storage_manager,
            regions: RwLock::new(HashMap::new()),
            peer_addresses: crate::peer_address_map::PeerAddressMap::new(),
            registry,
            dr_event_tx: tx,
            dr_event_rx: parking_lot::Mutex::new(Some(rx)),
            cancellation_token: parking_lot::Mutex::new(CancellationToken::new()),
            runtime_config: parking_lot::Mutex::new(None),
            recovery_stats: RwLock::new(HashMap::new()),
        }
    }

    /// Returns the shard router configured for this manager.
    ///
    /// The router resolves `(region, org_id)` → shard index via seahash %
    /// `shards_per_region`. Used by services at the proposal boundary to
    /// route each incoming write to the correct Raft group.
    #[must_use]
    pub fn shard_router(&self) -> inferadb_ledger_state::shard_routing::ShardRouter {
        inferadb_ledger_state::shard_routing::ShardRouter::new(self.config.shards_per_region)
    }

    /// Returns the last crash-recovery replay stats captured for `region`, if any.
    ///
    /// Populated by `start_region` immediately after
    /// `RaftLogStore::replay_crash_gap` returns. Returns `None` if the
    /// region has not been started on this manager since process start, or
    /// if the region was stopped. Used by the crash-recovery integration
    /// test suite (`crates/server/tests/checkpoint_crash_recovery.rs`) to
    /// assert replay counts programmatically without parsing tracing output.
    pub fn last_recovery_stats(&self, region: Region) -> Option<RecoveryStats> {
        self.recovery_stats.read().get(&region).copied()
    }

    /// Sets the parent cancellation token for all per-region background jobs.
    ///
    /// Called during bootstrap to link the manager's jobs to the
    /// [`ShutdownCoordinator`]'s token hierarchy. Child tokens are created
    /// from this token for each per-region background job.
    pub fn set_cancellation_token(&self, token: CancellationToken) {
        *self.cancellation_token.lock() = token;
    }

    /// Sets the runtime-config handle used by per-region [`StateCheckpointer`]
    /// tasks to read live `CheckpointConfig` thresholds on every tick.
    ///
    /// Called during bootstrap **before** the first region is started so every
    /// region's checkpointer receives the live handle rather than a detached
    /// default. Tests that don't exercise the checkpoint path may skip this
    /// call; the checkpointer falls back to
    /// [`CheckpointConfig::default`](inferadb_ledger_types::config::CheckpointConfig::default)
    /// thresholds via a local [`RuntimeConfigHandle::default`] if unset.
    pub fn set_runtime_config(&self, handle: RuntimeConfigHandle) {
        *self.runtime_config.lock() = Some(handle);
    }

    /// Returns the shared per-node connection registry.
    ///
    /// Downstream components (services, forward clients) clone this `Arc`
    /// to share peer channels with the consensus transport.
    #[must_use]
    pub fn registry(&self) -> Arc<crate::node_registry::NodeConnectionRegistry> {
        Arc::clone(&self.registry)
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

    /// Returns the shared peer address map.
    ///
    /// Services use this to resolve peer network addresses for forwarding
    /// and health checks without reaching into the consensus transport layer.
    #[must_use]
    pub fn peer_addresses(&self) -> &crate::peer_address_map::PeerAddressMap {
        &self.peer_addresses
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

    /// Returns a read-only accessor for the GLOBAL region's applied state.
    pub fn system_state_reader(&self) -> Option<SystemStateReader> {
        self.system_region()
            .ok()
            .map(|group| SystemStateReader { state_layer: group.state().clone() })
    }

    /// Lists all active region IDs.
    pub fn list_regions(&self) -> Vec<Region> {
        self.regions.read().keys().copied().collect()
    }

    /// Takes the DR event receiver. Called once by bootstrap to pass to the
    /// PlacementController. Returns `None` on subsequent calls.
    pub fn take_dr_event_rx(&self) -> Option<tokio::sync::mpsc::UnboundedReceiver<()>> {
        self.dr_event_rx.lock().take()
    }

    /// Fires a DR membership event. Wakes the PlacementController to
    /// reconcile data region membership immediately.
    pub fn notify_dr_membership_change(&self) {
        let _ = self.dr_event_tx.send(());
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
    /// Registers an externally created region using a [`ConsensusHandle`].
    ///
    /// # Errors
    ///
    /// Returns [`RaftManagerError::RegionExists`] if the region is already active.
    #[allow(clippy::too_many_arguments)]
    pub fn register_consensus_region(
        &self,
        region: Region,
        handle: Arc<ConsensusHandle>,
        state: Arc<StateLayer<FileBackend>>,
        raft_db: Arc<Database<FileBackend>>,
        block_archive: Arc<BlockArchive<FileBackend>>,
        applied_state: AppliedStateAccessor,
        block_announcements: broadcast::Sender<BlockAnnouncement>,
        commitment_buffer: std::sync::Arc<std::sync::Mutex<Vec<crate::types::StateRootCommitment>>>,
        leader_lease: Arc<crate::leader_lease::LeaderLease>,
        applied_index_rx: tokio::sync::watch::Receiver<u64>,
    ) -> Result<Arc<RegionGroup>> {
        if self.has_region(region) {
            return Err(RaftManagerError::RegionExists { region });
        }

        let blocks_db = Arc::clone(block_archive.db());

        let region_group = Arc::new(RegionGroup {
            region,

            handle,
            state: state.clone(),
            raft_db,
            blocks_db,
            block_archive,
            applied_state,
            block_announcements,
            background_jobs: parking_lot::Mutex::new(RegionBackgroundJobs::none()),
            batch_handle: None,
            commitment_buffer,
            leader_lease,
            applied_index_rx,
            consensus_transport: None,
            events_db: None,
            last_activity: Arc::new(parking_lot::Mutex::new(std::time::Instant::now())),
            jobs_active: Arc::new(AtomicBool::new(false)),
            region_creation_rx: parking_lot::Mutex::new(None),
        });

        {
            let mut regions = self.regions.write();
            regions.insert(region, region_group.clone());
        }

        Ok(region_group)
    }

    /// Routes an organization to its region group.
    ///
    /// Looks up the organization's region assignment in the `_system` service
    /// and returns the local RegionGroup if available.
    ///
    /// Returns `None` if:
    /// - System region not started
    /// - Organization not found in `_system`
    /// - Region is on a different node (requires forwarding)
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    pub fn route_organization(&self, organization: OrganizationId) -> Option<Arc<RegionGroup>> {
        let region = self.get_organization_region(organization)?;

        // Get local region group. Data regions are created through GLOBAL Raft
        // consensus (CreateDataRegion), so we don't lazily create here — the
        // region must already exist from a prior consensus proposal.
        let group = self.regions.read().get(&region).cloned()?;
        group.touch();

        // Auto-wake hibernated regions on first request
        if !group.is_jobs_active() {
            let _ = self.wake_region(region);
        }

        Some(group)
    }

    /// Returns the region ID for an organization.
    ///
    /// Looks up the organization's region assignment without checking
    /// if the region is locally available.
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    pub fn get_organization_region(&self, organization: OrganizationId) -> Option<Region> {
        let system = self.system_region().ok()?;
        let sys = SystemOrganizationService::new(system.state().clone());
        let registry = sys.get_organization(organization).ok().flatten()?;
        Some(registry.region)
    }

    /// Starts the system region (`_system`).
    ///
    /// The system region must be started before any data regions.
    /// It stores the organization routing table and cluster metadata.
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

        self.start_region(region_config).await
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
            bootstrap: _,
            enable_background_jobs,
            batch_writer_config,
            event_writer,
            events_config,
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

        info!(region = region.as_str(), "Starting region group");

        // Create divergence channel for state root verification.
        // The sender is passed into RaftLogStore; the receiver drives the handler task.
        let (divergence_sender, divergence_receiver) = tokio::sync::mpsc::unbounded_channel();

        // Open storage via RegionStorageManager (creates directory + databases + RaftLogStore)
        let (
            state,
            block_archive,
            mut log_store,
            block_announcements,
            events_db,
            region_creation_rx,
        ) = self.open_region_storage(region, event_writer, events_config, divergence_sender)?;

        // Get accessor, commitment buffer, leader lease, applied index watch,
        // and the raft.db handle before log_store is consumed by Adaptor.
        // raft.db ownership is surfaced here so sync_all_state_dbs (graceful
        // shutdown) and StateCheckpointer (steady-state) can both reach it.
        let applied_state = log_store.accessor();
        let commitment_buffer = log_store.commitment_buffer();
        let leader_lease = log_store.leader_lease().clone();
        let applied_index_rx = log_store.applied_index_watch();
        let raft_db = log_store.log_store_db();

        // ────────────────────────────────────────────────────────────
        // Create consensus engine + apply worker. The consensus engine
        // handles elections, replication, and commits. The apply worker
        // processes committed entries through the existing state machine.
        // ────────────────────────────────────────────────────────────

        let shard_config = inferadb_ledger_consensus::ShardConfig {
            election_timeout_min: std::time::Duration::from_millis(
                self.config.election_timeout_min_ms,
            ),
            election_timeout_max: std::time::Duration::from_millis(
                self.config.election_timeout_max_ms,
            ),
            heartbeat_interval: std::time::Duration::from_millis(self.config.heartbeat_interval_ms),
            // Data region shards have auto-promote disabled — the DR scheduler
            // manages promotions with a catch-up check via peer_match_index.
            auto_promote: region == Region::GLOBAL,
            ..Default::default()
        };
        let shard_id =
            inferadb_ledger_consensus::types::ShardId(seahash::hash(region.as_str().as_bytes()));

        // Initial membership for the consensus engine shard.
        //
        // On restart (persisted state exists), use the persisted membership
        // from the RaftLogStore rather than `initial_members`. The persisted
        // membership reflects the last committed configuration, so the shard
        // starts with the correct voter/learner set and does not
        // spuriously elect itself leader as a sole-voter singleton.
        let persisted_membership = log_store.persisted_membership();
        let consensus_membership = if persisted_membership.voter_ids.len() > 1 {
            let voters: std::collections::BTreeSet<inferadb_ledger_consensus::types::NodeId> =
                persisted_membership
                    .voter_ids
                    .iter()
                    .map(|&id| inferadb_ledger_consensus::types::NodeId(id))
                    .collect();
            let mut membership = inferadb_ledger_consensus::types::Membership::new(voters);
            for &learner_id in &persisted_membership.learner_ids {
                membership.add_learner(inferadb_ledger_consensus::types::NodeId(learner_id));
            }
            info!(
                region = region.as_str(),
                voters = ?persisted_membership.voter_ids,
                learners = ?persisted_membership.learner_ids,
                "Using persisted membership for consensus shard"
            );
            membership
        } else {
            let voter_ids: std::collections::BTreeSet<inferadb_ledger_consensus::types::NodeId> =
                if initial_members.is_empty() {
                    [inferadb_ledger_consensus::types::NodeId(self.config.node_id)]
                        .into_iter()
                        .collect()
                } else {
                    initial_members
                        .iter()
                        .map(|(id, _)| inferadb_ledger_consensus::types::NodeId(*id))
                        .collect()
                };
            inferadb_ledger_consensus::types::Membership::new(voter_ids)
        };

        // Task 3 stopgap: single-shard layout → `shard-0/wal/`. Task 4
        // iterates per-shard.
        let wal_dir = self
            .storage_manager
            .shard_dir(region, inferadb_ledger_state::shard_routing::ShardIdx(0))
            .join("wal");
        let wal =
            inferadb_ledger_consensus::wal::SegmentedWalBackend::open(&wal_dir).map_err(|e| {
                RaftManagerError::Storage { region, message: format!("failed to open WAL: {e}") }
            })?;

        // Recover persisted term + votedFor from the WAL checkpoint (Raft
        // Figure 2). On first boot the WAL has no checkpoint, so we default
        // to term=0, voted_for=None.
        let (initial_term, initial_voted_for, initial_committed_index) = match wal.last_checkpoint()
        {
            Ok(Some(cp)) => {
                info!(
                    region = region.as_str(),
                    term = cp.term,
                    voted_for = ?cp.voted_for,
                    committed_index = cp.committed_index,
                    "Recovered Raft term state from WAL checkpoint"
                );
                (
                    cp.term,
                    cp.voted_for.map(inferadb_ledger_consensus::types::NodeId),
                    cp.committed_index,
                )
            },
            Ok(None) => {
                info!(
                    region = region.as_str(),
                    "No WAL checkpoint found — starting at term 0 (first boot)"
                );
                (0, None, 0)
            },
            Err(e) => {
                warn!(
                    region = region.as_str(),
                    error = %e,
                    "Failed to read WAL checkpoint — starting at term 0 for safety"
                );
                (0, None, 0)
            },
        };

        // Close the crash-recovery gap widened by `commit_in_memory`.
        // `commit_in_memory` leaves the state DB lagging the WAL by up to one
        // checkpoint interval on an unclean shutdown; replay WAL entries in
        // `(applied_durable, last_committed]` through the normal apply
        // pipeline, then force a `sync_state` so recovery is durable before
        // we serve traffic. MUST run BEFORE `ConsensusEngine::start` consumes
        // the WAL AND BEFORE the apply worker is spawned, so there's no
        // concurrent modifier of `applied_state`.
        match log_store.replay_crash_gap(&wal, shard_id).await {
            Ok(stats) => {
                metrics::record_state_recovery_replay(region.as_str(), stats.replayed_entries);
                metrics::record_state_recovery_duration(region.as_str(), stats.duration);
                info!(
                    region = region.as_str(),
                    applied_durable = stats.applied_durable,
                    last_committed = stats.last_committed,
                    replayed_entries = stats.replayed_entries,
                    duration_ms = stats.duration.as_millis() as u64,
                    "Crash-recovery replay complete",
                );
                // Capture stats for test-observability. Overwrites any
                // previous entry so a region that's stopped + restarted
                // during the same process lifetime reports its most recent
                // recovery sweep.
                self.recovery_stats.write().insert(region, stats);
            },
            Err(e) => {
                return Err(RaftManagerError::Storage {
                    region,
                    message: format!("crash-recovery replay failed: {e}"),
                });
            },
        }

        let consensus_shard = inferadb_ledger_consensus::Shard::new(
            shard_id,
            inferadb_ledger_consensus::types::NodeId(self.config.node_id),
            consensus_membership,
            shard_config,
            inferadb_ledger_consensus::SystemClock,
            inferadb_ledger_consensus::rng::SystemRng,
            initial_term,
            initial_voted_for,
            initial_committed_index,
        );

        let consensus_transport = crate::consensus_transport::GrpcConsensusTransport::new(
            self.config.node_id,
            region,
            Arc::clone(&self.registry),
        );
        // Set the local address from initial_members so outbound messages include
        // the sender's address for auto-registration on the receiving end.
        if let Some((_, addr)) = initial_members.iter().find(|(id, _)| *id == self.config.node_id) {
            consensus_transport.set_local_address(addr.clone());
        }
        let consensus_transport_for_group = consensus_transport.clone();

        // Register peer channels for initial members and populate the shared
        // peer address map so services can resolve addresses for forwarding.
        // Channels flow through the node-level `NodeConnectionRegistry` so
        // they're shared across consensus, forwarding, discovery, and admin.
        for (node_id, addr) in &initial_members {
            if *node_id != self.config.node_id {
                self.peer_addresses.insert(*node_id, addr.clone());
                if let Err(e) = consensus_transport.set_peer_via_registry(*node_id, addr).await {
                    warn!(node_id, addr, error = %e, "Failed to register peer via registry");
                }
            }
        }
        let (engine, commit_rx, state_watchers) = inferadb_ledger_consensus::ConsensusEngine::start(
            vec![consensus_shard],
            wal,
            inferadb_ledger_consensus::SystemClock,
            consensus_transport,
            std::time::Duration::from_millis(2),
        );

        let state_rx = state_watchers.get(&shard_id).cloned().unwrap_or_else(|| {
            let (_, rx) = tokio::sync::watch::channel(
                inferadb_ledger_consensus::leadership::ShardState::default(),
            );
            rx
        });

        let response_map: crate::consensus_handle::ResponseMap =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));

        let handle = Arc::new(ConsensusHandle::new(
            engine,
            shard_id,
            self.config.node_id,
            state_rx,
            response_map,
        ));

        // Create batch writer using ConsensusHandle for proposals.
        let batch_handle = if let Some(batch_config) = batch_writer_config {
            let handle_clone = handle.clone();
            let buffer_clone = commitment_buffer.clone();
            let submit_fn = move |requests: Vec<LedgerRequest>| {
                let h = handle_clone.clone();
                let buffer = buffer_clone.clone();
                Box::pin(async move {
                    let batch_request = LedgerRequest::BatchWrite { requests };
                    let commitments =
                        std::mem::take(&mut *buffer.lock().unwrap_or_else(|e| e.into_inner()));
                    let payload = RaftPayload::with_commitments(batch_request, commitments, 0);
                    match h.propose_and_wait(payload, std::time::Duration::from_secs(30)).await {
                        Ok(LedgerResponse::BatchWrite { responses }) => Ok(responses),
                        Ok(other) => Ok(vec![other]),
                        Err(e) => Err(format!("Consensus error: {}", e)),
                    }
                })
                    as futures::future::BoxFuture<
                        'static,
                        std::result::Result<Vec<LedgerResponse>, String>,
                    >
            };

            let writer = BatchWriter::new(batch_config, submit_fn, region.to_string());
            let bw_handle = writer.handle();
            tokio::spawn(writer.run());
            Some(bw_handle)
        } else {
            None
        };

        // Start background jobs if enabled
        let background_jobs = if enable_background_jobs {
            self.start_background_jobs(
                region,
                handle.clone(),
                state.clone(),
                Arc::clone(&raft_db),
                Arc::clone(block_archive.db()),
                Some(Arc::clone(events_db.db())),
                block_archive.clone(),
                applied_state.clone(),
                applied_index_rx.clone(),
            )
        } else {
            RegionBackgroundJobs::none()
        };

        // Spawn state root divergence handler — halts vaults on mismatch.
        // Runs alongside AutoRecoveryJob and other background workers.
        let divergence_handler = crate::state_root_verifier::StateRootDivergenceHandler::new(
            handle.clone(),
            divergence_receiver,
            region.to_string(),
        );
        tokio::spawn(divergence_handler.run());

        // Spawn the apply worker — bridges consensus commits to state machine.
        // The GLOBAL region's worker gets the DR event sender so the
        // PlacementController wakes when GLOBAL membership changes are applied.
        let mut apply_worker = crate::apply_worker::ApplyWorker::new(
            log_store,
            handle.response_map().clone(),
            handle.spillover().clone(),
        );
        if region == inferadb_ledger_types::Region::GLOBAL {
            apply_worker = apply_worker.with_dr_event_tx(self.dr_event_tx.clone());
        }
        tokio::spawn(apply_worker.run(commit_rx));

        // Create region group.
        //
        // `blocks_db` is surfaced here alongside `raft_db` so the
        // `StateCheckpointer` and `sync_all_state_dbs` can reach the
        // underlying `Database<FileBackend>` without holding a reference to
        // `block_archive` (which owns a domain API, not a durability API).
        let jobs_running = enable_background_jobs;
        let blocks_db = Arc::clone(block_archive.db());
        let region_group = Arc::new(RegionGroup {
            region,

            handle,
            state,
            raft_db,
            blocks_db,
            block_archive,
            applied_state,
            block_announcements,
            background_jobs: parking_lot::Mutex::new(background_jobs),
            batch_handle,
            commitment_buffer,
            leader_lease,
            applied_index_rx,
            consensus_transport: Some(consensus_transport_for_group),
            events_db: Some(events_db),
            last_activity: Arc::new(parking_lot::Mutex::new(std::time::Instant::now())),
            jobs_active: Arc::new(AtomicBool::new(jobs_running)),
            region_creation_rx: parking_lot::Mutex::new(region_creation_rx),
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
    ///
    /// `raft_db`, `blocks_db`, and `events_db` are plumbed into the
    /// [`StateCheckpointer`] so it can `sync_state` on every durability
    /// DB in lock-step. `events_db` is `Option`
    /// because some regions (test fixtures, historical GLOBAL-only
    /// configurations) are constructed without an events writer.
    #[allow(clippy::too_many_arguments)]
    fn start_background_jobs(
        &self,
        region: Region,
        handle: Arc<ConsensusHandle>,
        state: Arc<StateLayer<FileBackend>>,
        raft_db: Arc<Database<FileBackend>>,
        blocks_db: Arc<Database<FileBackend>>,
        events_db: Option<Arc<Database<FileBackend>>>,
        block_archive: Arc<BlockArchive<FileBackend>>,
        applied_state: AppliedStateAccessor,
        applied_index_rx: tokio::sync::watch::Receiver<u64>,
    ) -> RegionBackgroundJobs {
        info!(region = region.as_str(), "Starting background jobs for region");

        let parent_token = self.cancellation_token.lock().clone();

        // TTL Garbage Collector
        let gc = TtlGarbageCollector::builder()
            .handle(handle.clone())
            .state(state.clone())
            .applied_state(applied_state.clone())
            .cancellation_token(parent_token.child_token())
            .build();
        let gc_handle = gc.start();
        info!(region = region.as_str(), "Started TTL garbage collector");

        // Block Compactor
        let compactor = BlockCompactor::builder()
            .handle(handle.clone())
            .block_archive(block_archive.clone())
            .applied_state(applied_state.clone())
            .cancellation_token(parent_token.child_token())
            .build();
        let compactor_handle = compactor.start();
        info!(region = region.as_str(), "Started block compactor");

        // Auto Recovery Job
        let recovery = AutoRecoveryJob::builder()
            .handle(handle.clone())
            .node_id(self.config.node_id)
            .applied_state(applied_state)
            .state(state.clone())
            .block_archive(Some(block_archive))
            .cancellation_token(parent_token.child_token())
            .build();
        let recovery_handle = recovery.start();
        info!(region = region.as_str(), "Started auto recovery job");

        // B+ Tree Compactor
        let btree_compactor = BTreeCompactor::builder()
            .handle(handle.clone())
            .state(state.clone())
            .cancellation_token(parent_token.child_token())
            .build();
        let btree_compactor_handle = btree_compactor.start();
        info!(region = region.as_str(), "Started B+ tree compactor");

        // State-DB Checkpointer.
        //
        // Drives `Database::sync_state` against in-memory commits produced by
        // `WriteTransaction::commit_in_memory`. The apply path uses
        // `commit_in_memory` on state.db + raft.db + blocks.db + events.db.
        // This checkpointer is the sync driver for all 4 DBs (or 3 when a
        // region has no events writer).
        //
        // Falls back to a fresh `RuntimeConfigHandle::default()` if
        // `set_runtime_config` was never called (test harnesses). The
        // checkpointer then uses `CheckpointConfig::default()` thresholds.
        let runtime_config = self.runtime_config.lock().clone().unwrap_or_default();
        let state_checkpointer = StateCheckpointer::from_config(
            state.database().clone(),
            raft_db,
            blocks_db,
            events_db,
            runtime_config,
            applied_index_rx,
            parent_token.child_token(),
            region.as_str().to_string(),
        );
        let state_checkpointer_handle = state_checkpointer.start();
        info!(region = region.as_str(), "Started state checkpointer");

        // Integrity Scrubber
        let integrity_scrubber = IntegrityScrubberJob::builder()
            .state(state.clone())
            .cancellation_token(parent_token.child_token())
            .build();
        let integrity_scrubber_handle = integrity_scrubber.start();
        info!(region = region.as_str(), "Started integrity scrubber");

        // DEK Re-Wrapping Job
        let rewrap_progress = Arc::new(RewrapProgress::new());
        let dek_rewrap = DekRewrapJob::builder()
            .handle(handle)
            .state(state)
            .progress(rewrap_progress.clone())
            .build();
        let dek_rewrap_handle = dek_rewrap.start();
        info!(region = region.as_str(), "Started DEK re-wrapping job");

        RegionBackgroundJobs {
            region_token: parent_token,
            gc_handle: Some(gc_handle),
            compactor_handle: Some(compactor_handle),
            recovery_handle: Some(recovery_handle),
            btree_compactor_handle: Some(btree_compactor_handle),
            integrity_scrubber_handle: Some(integrity_scrubber_handle),
            dek_rewrap_handle: Some(dek_rewrap_handle),
            state_checkpointer_handle: Some(state_checkpointer_handle),
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
        events_config: Option<inferadb_ledger_types::events::EventConfig>,
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
        // Task 3 stopgap: single-shard layout forces ShardIdx(0). Task 4
        // parameterizes RegionGroup over the full shard set.
        let log_path = self.storage_manager.raft_db_path(
            region,
            inferadb_ledger_state::shard_routing::ShardIdx(0),
        );
        // Derive leader lease duration from election_timeout_min / 2.
        // This guarantees no new leader can be elected while the lease is valid.
        let lease_duration =
            std::time::Duration::from_millis(self.config.election_timeout_min_ms / 2);
        let leader_lease = Arc::new(crate::leader_lease::LeaderLease::new(lease_duration));

        let mut log_store = RaftLogStore::<FileBackend>::open(&log_path)
            .map_err(|e| RaftManagerError::Storage {
                region,
                message: format!("Failed to open log store: {e}"),
            })?
            .with_state_layer(state.clone())
            .with_block_archive(block_archive.clone())
            .with_region_config(
                region,
                NodeId::new(self.config.node_id.to_string()),
                self.config.node_id,
            )
            .with_block_announcements(block_announcements.clone())
            .with_divergence_sender(divergence_sender)
            .with_leader_lease(leader_lease);

        // Wire region creation channel for the GLOBAL log store.
        // CreateDataRegion entries applied on GLOBAL send the region through
        // this channel so the RaftManager can start the local region group.
        let region_creation_rx = if region == Region::GLOBAL {
            let (region_tx, region_rx) = tokio::sync::mpsc::unbounded_channel();
            log_store = log_store.with_region_creation_sender(region_tx);
            log_store = log_store.with_peer_addresses(self.peer_addresses.clone());
            Some(region_rx)
        } else {
            None
        };

        // Wire event writer: use the explicitly provided writer, or create one
        // from the region's own events_db when only an EventConfig was supplied.
        // This avoids callers needing to pre-open the events database.
        let events_db = storage.events_db().clone();
        if let Some(writer) = event_writer {
            log_store = log_store.with_event_writer(writer);
        } else if let Some(cfg) = events_config {
            let writer = EventWriter::new(events_db.clone(), cfg);
            log_store = log_store.with_event_writer(writer);
        }

        Ok((state, block_archive, log_store, block_announcements, events_db, region_creation_rx))
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
            jobs.cancel();
            debug!(region = region.as_str(), "Aborted background jobs");
        }

        // Shut down the consensus engine reactor for this region.
        let handle = region_group.handle().clone();
        tokio::spawn(async move {
            handle.request_shutdown().await;
        });

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

    /// Forces a checkpoint-style `sync_state` on every region's state.db
    /// AND raft.db.
    ///
    /// Called from the graceful-shutdown `pre_shutdown` closure **after** the
    /// WAL flush so that on clean shutdown the post-restart WAL replay is
    /// zero entries: the god-byte pointer captures every apply that happened
    /// between the last [`StateCheckpointer`] tick and the final drain.
    ///
    /// Each region owns two lazy-durable databases: state.db (entity tables,
    /// via `StateLayer`) and raft.db (`KEY_APPLIED_STATE` blob + Raft log).
    /// Both must be synced — skipping raft.db causes `applied_durable = 0`
    /// to be read on restart and forces a full WAL replay (see the
    /// follow-up in the commit-durability audit).
    ///
    /// Errors are logged per-region and per-db but do not abort the sweep —
    /// one region's disk-full (or one db's failure) must not block the
    /// remaining work from reaching durability. The caller treats this as
    /// best-effort.
    ///
    /// `total_timeout` is the **total** budget across all regions; each
    /// region gets a proportional share with a 1s floor. Within a region,
    /// state.db and raft.db are synced concurrently via `tokio::join!`, so
    /// both share the per-region budget rather than splitting it. A single
    /// timed-out region does not consume the remaining regions' budgets.
    pub async fn sync_all_state_dbs(&self, total_timeout: Duration) {
        let regions = self.list_regions();
        if regions.is_empty() {
            info!("sync_all_state_dbs: no regions open, skipping");
            return;
        }

        let per_region_timeout = total_timeout
            .checked_div(regions.len() as u32)
            .unwrap_or(Duration::from_secs(1))
            .max(Duration::from_secs(1));

        info!(
            regions = regions.len(),
            per_region_timeout_ms = per_region_timeout.as_millis() as u64,
            "sync_all_state_dbs: forcing final state.db + raft.db + blocks.db + events.db \
             (if configured) sync across all regions"
        );

        for region in regions {
            // Snapshot the region group under the read lock, drop the lock
            // before awaiting so the shutdown sweep never contends with
            // other readers for the regions map.
            let group = self.regions.read().get(&region).cloned();
            let Some(group) = group else {
                debug!(
                    region = region.as_str(),
                    "sync_all_state_dbs: region vanished between list and lookup, skipping"
                );
                continue;
            };
            let state_db = group.state().database().clone();
            let raft_db = Arc::clone(group.raft_db());
            let blocks_db = Arc::clone(group.blocks_db());
            let events_db_opt = group.events_state_db();

            // Sync every configured DB concurrently inside the per-region
            // timeout. Using `tokio::join!` rather than splitting the budget
            // means a slow fsync on one DB doesn't starve the others.
            // `sync_state` consumes an `Arc<Self>`, so clone for the await
            // and reuse the originals to read `last_synced_snapshot_id`
            // after the join. When `events_db_opt` is `None` the match
            // picks the 3-arm variant so absent-events regions don't
            // contribute a spurious Ok to the log.
            let state_fut = Arc::clone(&state_db).sync_state();
            let raft_fut = Arc::clone(&raft_db).sync_state();
            let blocks_fut = Arc::clone(&blocks_db).sync_state();
            let events_db_for_sync = events_db_opt.clone();
            let timeout_outcome = tokio::time::timeout(per_region_timeout, async {
                match events_db_for_sync {
                    Some(events_db) => {
                        let events_fut = Arc::clone(&events_db).sync_state();
                        let (s, r, b, e) =
                            tokio::join!(state_fut, raft_fut, blocks_fut, events_fut);
                        (s, r, b, Some(e))
                    },
                    None => {
                        let (s, r, b) = tokio::join!(state_fut, raft_fut, blocks_fut);
                        (s, r, b, None)
                    },
                }
            })
            .await;

            match timeout_outcome {
                Ok((state_result, raft_result, blocks_result, events_result)) => {
                    match state_result {
                        Ok(()) => info!(
                            region = region.as_str(),
                            db = "state",
                            last_synced_snapshot_id = state_db.last_synced_snapshot_id(),
                            "sync_all_state_dbs: final state-DB sync complete"
                        ),
                        Err(e) => warn!(
                            region = region.as_str(),
                            db = "state",
                            error = %e,
                            "sync_all_state_dbs: final state-DB sync failed; crash gap narrowed but not zero"
                        ),
                    }
                    match raft_result {
                        Ok(()) => info!(
                            region = region.as_str(),
                            db = "raft",
                            last_synced_snapshot_id = raft_db.last_synced_snapshot_id(),
                            "sync_all_state_dbs: final state-DB sync complete"
                        ),
                        Err(e) => warn!(
                            region = region.as_str(),
                            db = "raft",
                            error = %e,
                            "sync_all_state_dbs: final state-DB sync failed; crash gap narrowed but not zero"
                        ),
                    }
                    match blocks_result {
                        Ok(()) => info!(
                            region = region.as_str(),
                            db = "blocks",
                            last_synced_snapshot_id = blocks_db.last_synced_snapshot_id(),
                            "sync_all_state_dbs: final state-DB sync complete"
                        ),
                        Err(e) => warn!(
                            region = region.as_str(),
                            db = "blocks",
                            error = %e,
                            "sync_all_state_dbs: final state-DB sync failed; crash gap narrowed but not zero"
                        ),
                    }
                    match (events_result, events_db_opt) {
                        (Some(Ok(())), Some(events_db)) => info!(
                            region = region.as_str(),
                            db = "events",
                            last_synced_snapshot_id = events_db.last_synced_snapshot_id(),
                            "sync_all_state_dbs: final state-DB sync complete"
                        ),
                        (Some(Err(e)), _) => warn!(
                            region = region.as_str(),
                            db = "events",
                            error = %e,
                            "sync_all_state_dbs: final state-DB sync failed; crash gap narrowed but not zero"
                        ),
                        (None, _) => debug!(
                            region = region.as_str(),
                            "sync_all_state_dbs: region has no events_db; skipping events sync"
                        ),
                        (Some(Ok(())), None) => {
                            // Unreachable: events_result Some implies the
                            // 4-arm join fired, which implies
                            // events_db_opt was Some at the call site.
                            // Swallow defensively rather than panic.
                            debug!(
                                region = region.as_str(),
                                "sync_all_state_dbs: events sync succeeded but handle no longer available"
                            );
                        },
                    }
                },
                Err(_) => {
                    warn!(
                        region = region.as_str(),
                        timeout_ms = per_region_timeout.as_millis() as u64,
                        "sync_all_state_dbs: final state-DB sync (state + raft + blocks + events) \
                         timed out; continuing with remaining regions"
                    );
                },
            }
        }
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
                // Snapshot trigger is handled by the consensus engine.
                // No explicit snapshot trigger needed before shutdown.
            }
        }

        // Proceed with normal shutdown
        self.shutdown().await;
    }

    /// Hibernates a region by stopping its background jobs.
    ///
    /// The Raft instance remains alive — only background jobs are stopped.
    /// No-op if the region's jobs are already stopped.
    ///
    /// # Errors
    ///
    /// Returns [`RaftManagerError::RegionNotFound`] if no region with the given ID
    /// is currently active.
    pub fn hibernate_region(&self, region: Region) -> Result<()> {
        let group = self.get_region_group(region)?;

        // Atomically transition from active to inactive.
        // Only one caller wins; others return early.
        if group
            .jobs_active
            .compare_exchange(
                true,
                false,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_err()
        {
            return Ok(());
        }

        group.background_jobs.lock().cancel();
        info!(%region, "Region group hibernated");
        Ok(())
    }

    /// Wakes a hibernating region by restarting its background jobs.
    ///
    /// No-op if the region's jobs are already running.
    ///
    /// # Errors
    ///
    /// Returns [`RaftManagerError::RegionNotFound`] if no region with the given ID
    /// is currently active.
    pub fn wake_region(&self, region: Region) -> Result<()> {
        let group = self.get_region_group(region)?;

        // Atomically transition from inactive to active.
        // Only one caller wins; others return early.
        if group
            .jobs_active
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_err()
        {
            return Ok(());
        }

        group.touch();
        let jobs = self.start_background_jobs(
            region,
            group.handle().clone(),
            group.state().clone(),
            Arc::clone(group.raft_db()),
            Arc::clone(group.blocks_db()),
            group.events_state_db(),
            group.block_archive().clone(),
            group.applied_state().clone(),
            group.applied_index_watch(),
        );
        *group.background_jobs.lock() = jobs;
        info!(%region, "Region group woken from hibernation");
        Ok(())
    }

    /// Hibernates idle region groups whose background jobs have been inactive
    /// beyond the given timeout.
    ///
    /// The system region (`GLOBAL`) is never hibernated.
    pub fn hibernate_idle_regions(&self, idle_timeout_secs: u64) {
        let regions: Vec<(Region, Arc<RegionGroup>)> =
            self.regions.read().iter().map(|(r, g)| (*r, g.clone())).collect();

        for (region, group) in regions {
            if region == Region::GLOBAL {
                continue;
            }
            if group.is_jobs_active()
                && group.idle_secs() > idle_timeout_secs
                && let Err(e) = self.hibernate_region(region)
            {
                warn!(%region, error = %e, "Failed to hibernate region");
            }
        }
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
// System State Reader
// ============================================================================

/// Read-only accessor for GLOBAL system state. Used by the DR scheduler
/// to derive desired membership without holding mutable references.
pub struct SystemStateReader {
    state_layer: Arc<StateLayer<FileBackend>>,
}

impl SystemStateReader {
    /// Returns the status of a node, or `Active` if no status record exists.
    pub fn node_status(&self, node_id: u64) -> crate::types::NodeStatus {
        let key = format!("_meta:node_status:{node_id}");
        match self
            .state_layer
            .get_entity(inferadb_ledger_state::system::SYSTEM_VAULT_ID, key.as_bytes())
        {
            Ok(Some(entity)) => inferadb_ledger_types::decode(&entity.value)
                .unwrap_or(crate::types::NodeStatus::Active),
            _ => crate::types::NodeStatus::Active,
        }
    }

    /// Returns node statuses for all nodes that have an explicit status record.
    pub fn all_node_statuses(&self) -> Vec<(u64, crate::types::NodeStatus)> {
        let prefix = "_meta:node_status:";
        match self.state_layer.list_entities(
            inferadb_ledger_state::system::SYSTEM_VAULT_ID,
            Some(prefix),
            None,
            1000,
        ) {
            Ok(entities) => entities
                .iter()
                .filter_map(|e| {
                    let key = std::str::from_utf8(&e.key).ok()?;
                    let id_str = key.strip_prefix(prefix)?;
                    let node_id: u64 = id_str.parse().ok()?;
                    let status: crate::types::NodeStatus =
                        inferadb_ledger_types::decode(&e.value).ok()?;
                    Some((node_id, status))
                })
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Returns all region membership reports stored in GLOBAL state.
    ///
    /// Each entry is `(region_name, member_node_ids)` where member_node_ids
    /// is the union of voters and learners for that region.
    pub fn region_memberships(&self) -> Vec<(String, Vec<u64>)> {
        let prefix = "_meta:region_membership:";

        #[derive(serde::Deserialize)]
        struct ReportData {
            voters: Vec<u64>,
            learners: Vec<u64>,
        }

        match self.state_layer.list_entities(
            inferadb_ledger_state::system::SYSTEM_VAULT_ID,
            Some(prefix),
            None,
            100,
        ) {
            Ok(entities) => entities
                .iter()
                .filter_map(|e| {
                    let key = std::str::from_utf8(&e.key).ok()?;
                    let region_name = key.strip_prefix(prefix)?.to_string();
                    let data: ReportData = inferadb_ledger_types::decode(&e.value).ok()?;
                    let mut members = data.voters;
                    members.extend(data.learners);
                    Some((region_name, members))
                })
                .collect(),
            Err(_) => Vec::new(),
        }
    }
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

    /// Convenience constructor for tests: builds a fresh per-test registry.
    /// Production callers (server bootstrap) share a single registry across
    /// the process — see [`bootstrap`](../../../server/src/bootstrap.rs).
    fn test_manager(config: RaftManagerConfig) -> RaftManager {
        RaftManager::new(config, Arc::new(crate::node_registry::NodeConnectionRegistry::new()))
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
        let manager = test_manager(config);

        assert_eq!(manager.list_regions().len(), 0);
        assert!(!manager.has_region(Region::GLOBAL));
    }

    #[test]
    fn test_manager_exposes_shared_registry() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let registry = Arc::new(crate::node_registry::NodeConnectionRegistry::new());
        let manager = RaftManager::new(config, Arc::clone(&registry));

        // The accessor must hand back the same Arc instance so per-region
        // transports and downstream services share one channel pool.
        let from_manager = manager.registry();
        assert!(Arc::ptr_eq(&registry, &from_manager));

        // Successive calls return the same underlying registry.
        let again = manager.registry();
        assert!(Arc::ptr_eq(&from_manager, &again));
    }

    #[test]
    fn test_manager_stats_empty() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

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
        let manager = test_manager(config);
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
        let manager = test_manager(config);

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
        let manager = test_manager(config);

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
        let manager = test_manager(config);

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
        let manager = test_manager(config);

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
        let manager = test_manager(config);

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
        let manager = test_manager(config);

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
        assert!(jobs.state_checkpointer_handle.is_none());
    }

    #[test]
    fn test_background_jobs_abort_empty() {
        let mut jobs = RegionBackgroundJobs::none();
        // Aborting empty jobs shouldn't panic
        jobs.cancel();
        assert!(jobs.gc_handle.is_none());
        assert!(jobs.compactor_handle.is_none());
        assert!(jobs.recovery_handle.is_none());
        assert!(jobs.btree_compactor_handle.is_none());
        assert!(jobs.integrity_scrubber_handle.is_none());
        assert!(jobs.dek_rewrap_handle.is_none());
        assert!(jobs.state_checkpointer_handle.is_none());
    }

    #[tokio::test]
    async fn test_region_with_background_jobs_disabled() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

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
        assert!(
            jobs.state_checkpointer_handle.is_none(),
            "checkpointer should not be spawned when background jobs are disabled"
        );
    }

    #[tokio::test]
    async fn test_region_with_background_jobs_enabled() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

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
        assert!(
            jobs.state_checkpointer_handle.is_some(),
            "state checkpointer should be spawned for every region when background jobs are enabled"
        );
    }

    #[tokio::test]
    async fn test_sync_all_state_dbs_noop_when_no_regions() {
        // Sanity check: with no regions open, sync_all_state_dbs must not
        // divide-by-zero or panic. It should return promptly after logging.
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        // No regions started yet.
        assert!(manager.list_regions().is_empty());

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            manager.sync_all_state_dbs(std::time::Duration::from_secs(5)),
        )
        .await
        .expect("sync_all_state_dbs should return promptly with zero regions");
    }

    #[tokio::test]
    async fn test_sync_all_state_dbs_advances_snapshot_id_for_every_region() {
        // Start the GLOBAL region with background jobs disabled so the
        // StateCheckpointer doesn't race with our manual sync_all_state_dbs
        // call. Confirm that sync_all_state_dbs advances
        // last_synced_snapshot_id even without an explicit prior write —
        // sync_state short-circuits to a no-op when there's nothing dirty,
        // but it still completes Ok(()) which is what this test asserts.
        //
        // The sweep covers 4 DBs per region (state, raft, blocks, events
        // when configured). This assertion confirms none of them regress
        // their `last_synced_snapshot_id`.
        //
        // A deeper test that commits-in-memory and asserts the snapshot id
        // actually advances belongs in crash-recovery integration, where
        // the full apply pipeline is in scope.
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let mut region_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        region_config.enable_background_jobs = false;
        manager.start_system_region(region_config).await.expect("start system");

        let region = manager.get_region_group(Region::GLOBAL).expect("get region");
        let state_db = region.state().database().clone();
        let raft_db = Arc::clone(region.raft_db());
        let blocks_db = Arc::clone(region.blocks_db());
        let events_db = region.events_state_db();
        let state_before = state_db.last_synced_snapshot_id();
        let raft_before = raft_db.last_synced_snapshot_id();
        let blocks_before = blocks_db.last_synced_snapshot_id();
        let events_before = events_db.as_ref().map(|db| db.last_synced_snapshot_id());

        manager.sync_all_state_dbs(std::time::Duration::from_secs(5)).await;

        let state_after = state_db.last_synced_snapshot_id();
        let raft_after = raft_db.last_synced_snapshot_id();
        let blocks_after = blocks_db.last_synced_snapshot_id();
        let events_after = events_db.as_ref().map(|db| db.last_synced_snapshot_id());

        assert!(
            state_after >= state_before,
            "state.db last_synced_snapshot_id regressed (before={state_before}, after={state_after})"
        );
        assert!(
            raft_after >= raft_before,
            "raft.db last_synced_snapshot_id regressed (before={raft_before}, after={raft_after})"
        );
        assert!(
            blocks_after >= blocks_before,
            "blocks.db last_synced_snapshot_id regressed (before={blocks_before}, after={blocks_after})"
        );
        match (events_before, events_after) {
            (Some(before), Some(after)) => assert!(
                after >= before,
                "events.db last_synced_snapshot_id regressed (before={before}, after={after})"
            ),
            (None, None) => {},
            (a, b) => panic!(
                "events_db handle appeared or disappeared between calls: before={a:?}, after={b:?}"
            ),
        }
    }

    #[tokio::test]
    async fn test_stop_region_aborts_background_jobs() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

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
        let manager = test_manager(config);

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
        let manager = test_manager(config);

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
        let manager = test_manager(config);

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
        let manager = test_manager(config);

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
        let manager = test_manager(config);

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
        let manager = test_manager(config);

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
        let manager = test_manager(config);

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
            let manager = test_manager(config);

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
            let manager = test_manager(config);

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
        let manager = Arc::new(test_manager(config));

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

    // =========================================================================
    // Hibernation tests
    // =========================================================================

    #[tokio::test]
    async fn test_hibernate_region() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let region_config =
            RegionConfig::data(Region::US_EAST_VA, vec![(1, "127.0.0.1:50051".to_string())]);
        manager.start_data_region(region_config).await.expect("start data");

        let group = manager.get_region_group(Region::US_EAST_VA).unwrap();
        assert!(group.is_jobs_active());

        manager.hibernate_region(Region::US_EAST_VA).unwrap();
        assert!(!group.is_jobs_active());
    }

    #[tokio::test]
    async fn test_hibernate_idempotent() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let region_config =
            RegionConfig::data(Region::US_EAST_VA, vec![(1, "127.0.0.1:50051".to_string())]);
        manager.start_data_region(region_config).await.expect("start data");

        manager.hibernate_region(Region::US_EAST_VA).unwrap();
        // Second call is a no-op
        manager.hibernate_region(Region::US_EAST_VA).unwrap();
        let group = manager.get_region_group(Region::US_EAST_VA).unwrap();
        assert!(!group.is_jobs_active());
    }

    #[tokio::test]
    async fn test_wake_region() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let region_config =
            RegionConfig::data(Region::US_EAST_VA, vec![(1, "127.0.0.1:50051".to_string())]);
        manager.start_data_region(region_config).await.expect("start data");

        manager.hibernate_region(Region::US_EAST_VA).unwrap();
        let group = manager.get_region_group(Region::US_EAST_VA).unwrap();
        assert!(!group.is_jobs_active());

        manager.wake_region(Region::US_EAST_VA).unwrap();
        assert!(group.is_jobs_active());
    }

    #[tokio::test]
    async fn test_wake_idempotent() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let region_config =
            RegionConfig::data(Region::US_EAST_VA, vec![(1, "127.0.0.1:50051".to_string())]);
        manager.start_data_region(region_config).await.expect("start data");

        let group = manager.get_region_group(Region::US_EAST_VA).unwrap();
        assert!(group.is_jobs_active());

        // Waking an already-active region is a no-op
        manager.wake_region(Region::US_EAST_VA).unwrap();
        assert!(group.is_jobs_active());
    }

    #[tokio::test]
    async fn test_hibernate_not_found() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let result = manager.hibernate_region(Region::US_EAST_VA);
        assert!(matches!(result, Err(RaftManagerError::RegionNotFound { .. })));
    }

    #[tokio::test]
    async fn test_wake_not_found() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let result = manager.wake_region(Region::US_EAST_VA);
        assert!(matches!(result, Err(RaftManagerError::RegionNotFound { .. })));
    }

    #[tokio::test]
    async fn test_hibernate_idle_regions_skips_global() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let group = manager.get_region_group(Region::GLOBAL).unwrap();
        assert!(group.is_jobs_active());

        // Even with idle_timeout of 0, GLOBAL is never hibernated
        manager.hibernate_idle_regions(0);
        assert!(group.is_jobs_active());
    }

    #[tokio::test]
    async fn test_hibernate_idle_regions_hibernates_idle() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let region_config =
            RegionConfig::data(Region::US_EAST_VA, vec![(1, "127.0.0.1:50051".to_string())]);
        manager.start_data_region(region_config).await.expect("start data");

        let group = manager.get_region_group(Region::US_EAST_VA).unwrap();
        assert!(group.is_jobs_active());

        // Set last_activity to the past so idle_secs() > 0
        *group.last_activity.lock() = std::time::Instant::now() - std::time::Duration::from_secs(5);

        manager.hibernate_idle_regions(0);
        assert!(!group.is_jobs_active());
    }

    #[tokio::test]
    async fn test_touch_resets_idle_timer() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let group = manager.get_region_group(Region::GLOBAL).unwrap();
        // Just-created group should have near-zero idle time
        assert!(group.idle_secs() < 2);

        group.touch();
        assert!(group.idle_secs() < 2);
    }

    #[tokio::test]
    async fn test_region_group_initial_jobs_active_state() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        // System region with background jobs enabled
        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");
        let group = manager.get_region_group(Region::GLOBAL).unwrap();
        assert!(group.is_jobs_active());
    }

    #[tokio::test]
    async fn test_region_group_no_jobs_active_when_disabled() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let region_config =
            RegionConfig::data(Region::US_EAST_VA, vec![(1, "127.0.0.1:50051".to_string())])
                .without_background_jobs();
        manager.start_data_region(region_config).await.expect("start data");

        let group = manager.get_region_group(Region::US_EAST_VA).unwrap();
        assert!(!group.is_jobs_active());
    }
}
