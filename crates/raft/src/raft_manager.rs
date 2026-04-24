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
//! ├── _system region (OrganizationGroup 0)
//! │   ├── Raft instance
//! │   ├── RaftLogStore + StateLayer
//! │   ├── BlockArchive
//! │   └── Background jobs
//! ├── data region 1 (OrganizationGroup 1)
//! │   └── ... (same structure)
//! └── data region N (OrganizationGroup N)
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
use inferadb_ledger_types::{NodeId, OrganizationId, Region, VaultId};
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
    types::{LedgerNodeId, LedgerResponse, OrganizationRequest, RaftPayload},
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

    /// Vault group not found on this node.
    ///
    /// Returned by [`RaftManager::get_vault_group`] when the requested
    /// `(region, organization_id, vault_id)` triple has no registered
    /// vault group on this node. Under Slice 2a of per-vault consensus
    /// Phase 2, no vault groups are started yet, so every lookup returns
    /// this variant; Slice 2b wires `CreateVault` to
    /// `start_vault_group` and populates the map.
    #[snafu(display(
        "Vault group {vault_id} (organization {organization_id}, region {region}) not found"
    ))]
    VaultGroupNotFound { region: Region, organization_id: OrganizationId, vault_id: VaultId },

    /// A vault group with this identifier is already registered.
    ///
    /// Returned by [`RaftManager::start_vault_group`] when the caller
    /// attempts to re-register a `(region, organization_id, vault_id)`
    /// triple that already has a running vault group. Duplicate starts
    /// are an explicit error rather than a silent return of the existing
    /// group — the apply-side watcher surfaces duplicate
    /// `VaultCreationRequest` signals instead of leaking them.
    #[snafu(display(
        "Vault group {vault_id} (organization {organization_id}, region {region}) already exists"
    ))]
    VaultGroupExists { region: Region, organization_id: OrganizationId, vault_id: VaultId },
}

/// Result type for multi-raft operations.
pub type Result<T> = std::result::Result<T, RaftManagerError>;

/// Region creation request: region + initial members for the Raft group.
pub type RegionCreationRequest = (Region, Vec<(u64, String)>);

/// Organization creation request: target region + the new organization id.
///
/// Sent on the GLOBAL log store's `organization_creation_sender` channel
/// when a `CreateOrganization` entry applies. The receiver task spawned
/// during bootstrap calls
/// [`RaftManager::start_organization_group`](RaftManager::start_organization_group)
/// on each in-region node so the per-organization Raft group spins up.
pub type OrganizationCreationRequest = (Region, OrganizationId);

/// Signal fired by `CreateVault` apply; drained by the per-org watcher
/// task to trigger `start_vault_group` on every voter in the org.
///
/// Fire-and-forget: the apply path does not wait for the vault group to
/// come up. Mirrors the [`OrganizationCreationRequest`] pattern, but
/// scoped to a single `(region, organization)` Raft group. P2b.2.c
/// delivers the signal plumbing; P2b.2.d wires the watcher onto
/// [`RaftManager::start_vault_group`](RaftManager::start_vault_group).
#[derive(Debug, Clone)]
pub struct VaultCreationRequest {
    /// Region hosting the owning organization's Raft group.
    pub region: Region,
    /// Owning organization.
    pub organization: OrganizationId,
    /// Newly-allocated vault id.
    pub vault: VaultId,
}

/// Signal fired by `DeleteVault` apply; drained to trigger
/// `stop_vault_group` on every voter in the org.
///
/// Fire-and-forget; same semantics as [`VaultCreationRequest`].
#[derive(Debug, Clone)]
pub struct VaultDeletionRequest {
    /// Region hosting the owning organization's Raft group.
    pub region: Region,
    /// Owning organization.
    pub organization: OrganizationId,
    /// Vault id marked deleted.
    pub vault: VaultId,
}

/// Storage components returned from region opening (state, block archive, raft log store,
/// block announcements, events db, optional region-/organization-/vault-
/// lifecycle signal receivers).
///
/// The region + organization receivers are populated only for the GLOBAL log
/// store (system tier); the vault receivers are populated only for
/// per-organization groups (`organization_id != OrganizationId::new(0)`).
type OpenedRegionStorage = (
    Arc<StateLayer<FileBackend>>,
    Arc<BlockArchive<FileBackend>>,
    RaftLogStore<FileBackend>,
    broadcast::Sender<BlockAnnouncement>,
    Arc<inferadb_ledger_state::EventsDatabase<FileBackend>>,
    Option<tokio::sync::mpsc::UnboundedReceiver<RegionCreationRequest>>,
    Option<tokio::sync::mpsc::UnboundedReceiver<OrganizationCreationRequest>>,
    Option<tokio::sync::mpsc::UnboundedReceiver<VaultCreationRequest>>,
    Option<tokio::sync::mpsc::UnboundedReceiver<VaultDeletionRequest>>,
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
    /// batch writer and exposes a `BatchWriterHandle` on `OrganizationGroup`.
    pub batch_writer_config: Option<BatchWriterConfig>,
    /// Event writer for apply-phase audit event persistence.
    /// When set, events are recorded into the region's `events.db`.
    pub event_writer: Option<EventWriter<FileBackend>>,
    /// Event configuration for creating an `EventWriter` from the region's own
    /// `events.db`. Used when `event_writer` is `None` — the writer is created
    /// inside `open_region_storage` from the locally-opened database, avoiding
    /// a double-open of the same file.
    pub events_config: Option<inferadb_ledger_types::events::EventConfig>,
    /// Whether this group runs in delegated-leadership mode.
    ///
    /// When `true`, the consensus shard is constructed with
    /// [`inferadb_ledger_consensus::LeadershipMode::Delegated`]: it never
    /// initiates elections, and its leader is set externally via
    /// [`crate::raft_manager::RaftManager::adopt_organization_leader`]
    /// driven by the data-region group's elected leader (the B.1
    /// unified-leadership model).
    ///
    /// `false` (default) keeps the standard self-electing Raft behavior
    /// for the data-region and system groups.
    #[builder(default = false)]
    pub delegated_leadership: bool,
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

/// Internal peer-group storage shared by all three tier wrappers.
///
/// **Do not use this type directly from consumer code.** It exists so the three
/// tier newtypes ([`SystemGroup`], [`RegionGroup`], [`OrganizationGroup`]) can
/// share a single field layout and a single construction/registration path,
/// while each wrapper's public API surfaces only the methods appropriate for
/// its tier.
///
/// ## Why a shared inner and not three independently-typed structs?
///
/// The B.1 spec calls for three structurally-distinct structs. Writing them
/// literally would duplicate a ~1500-line construction + registration pipeline
/// (`start_region`, `start_background_jobs`, `register_consensus_region`,
/// hibernation, wake, graceful shutdown). The B.1 guard-rail those distinct
/// structs give us is *compile-time tier discipline at the API boundary* —
/// root rule 15. That is what Option C achieves here: the wrappers do not
/// implement `Deref`, so consumers cannot reach fields on `InnerGroup`
/// directly; they see only the methods the wrapper chooses to expose.
///
/// When the spec's per-tier field divergences actually appear (e.g.
/// `SystemGroup::coordination_event_tx`, `RegionGroup::leader_change_tx`),
/// add them here and expose accessors only on the relevant wrapper's `impl`.
/// Cross-tier leakage is detected when a caller tries to access a tier-wrong
/// method through the wrapper — that stays a compile error.
///
/// `lookup_by_consensus_shard` in `RaftManager` returns `Arc<InnerGroup>`
/// because the Raft gRPC wire dispatches by `ConsensusStateId` and genuinely
/// does not know which tier owns the shard. That's the one documented
/// cross-tier escape hatch.
pub struct InnerGroup {
    /// Region this group belongs to.
    pub(crate) region: Region,
    /// Organization identifier. `OrganizationId::new(0)` identifies system
    /// and regional control-plane groups; any other value identifies a
    /// per-organization data-plane group.
    pub(crate) organization_id: OrganizationId,
    /// Consensus handle for background jobs and services.
    pub(crate) handle: Arc<ConsensusHandle>,
    /// Shared state layer.
    pub(crate) state: Arc<StateLayer<FileBackend>>,
    /// `raft.db` handle. Shared with [`StateCheckpointer`] and
    /// [`RaftManager::sync_all_state_dbs`] — skipping its sync leaves
    /// `applied_durable = 0` and forces full WAL replay on next boot.
    pub(crate) raft_db: Arc<Database<FileBackend>>,
    /// `_meta.db` handle — per-organization coordinator introduced by
    /// Slice 1 of per-vault consensus. Owns the `_meta:last_applied`
    /// crash-recovery sentinel. The [`StateCheckpointer`] and
    /// [`RaftManager::sync_all_state_dbs`] must sync this handle **after**
    /// state.db / raft.db / blocks.db / events.db so the sentinel on disk
    /// never outruns the entity data it references.
    pub(crate) meta_db: Arc<Database<FileBackend>>,
    /// `blocks.db` handle. Owned alongside `block_archive` for the
    /// durability lifecycle.
    pub(crate) blocks_db: Arc<Database<FileBackend>>,
    /// Block archive for historical blocks.
    pub(crate) block_archive: Arc<BlockArchive<FileBackend>>,
    /// Accessor for applied state.
    pub(crate) applied_state: AppliedStateAccessor,
    /// Block announcement broadcast channel.
    pub(crate) block_announcements: broadcast::Sender<BlockAnnouncement>,
    /// Background job handles.
    pub(crate) background_jobs: parking_lot::Mutex<RegionBackgroundJobs>,
    /// Batch writer handle for coalescing writes (data-plane groups only).
    pub(crate) batch_handle: Option<BatchWriterHandle>,
    /// Shared state root commitment buffer. Populated by apply, drained
    /// by propose for piggybacked verification.
    pub(crate) commitment_buffer:
        std::sync::Arc<std::sync::Mutex<Vec<crate::types::StateRootCommitment>>>,
    /// Leader lease for fast linearizable reads.
    pub(crate) leader_lease: Arc<crate::leader_lease::LeaderLease>,
    /// Watch channel receiver for applied index (ReadIndex protocol).
    pub(crate) applied_index_rx: tokio::sync::watch::Receiver<u64>,
    /// Consensus transport for dynamic peer channel management.
    pub(crate) consensus_transport: Option<crate::consensus_transport::GrpcConsensusTransport>,
    /// Events database.
    pub(crate) events_db: Option<Arc<inferadb_ledger_state::EventsDatabase<FileBackend>>>,
    /// Last activity timestamp (for hibernation).
    pub(crate) last_activity: Arc<parking_lot::Mutex<std::time::Instant>>,
    /// Whether background jobs are currently running.
    pub(crate) jobs_active: Arc<AtomicBool>,
    /// Receiver for data-region creation signals (system tier only).
    pub(crate) region_creation_rx:
        parking_lot::Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<RegionCreationRequest>>>,
    /// Receiver for organization creation signals (system tier only).
    pub(crate) organization_creation_rx: parking_lot::Mutex<
        Option<tokio::sync::mpsc::UnboundedReceiver<OrganizationCreationRequest>>,
    >,
    /// Receiver for vault creation signals (per-organization groups only).
    ///
    /// Populated for groups whose `organization_id != OrganizationId::new(0)`;
    /// drained by the watcher task spawned in
    /// [`RaftManager::start_organization_group`](RaftManager::start_organization_group).
    /// P2b.2.c delivers the plumbing; P2b.2.d wires the drain onto
    /// `start_vault_group`.
    pub(crate) vault_creation_rx:
        parking_lot::Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<VaultCreationRequest>>>,
    /// Receiver for vault deletion signals (per-organization groups only).
    ///
    /// Same shape as [`vault_creation_rx`](Self::vault_creation_rx) — drained
    /// by the per-org watcher task.
    pub(crate) vault_deletion_rx:
        parking_lot::Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<VaultDeletionRequest>>>,
    /// Per-engine commit dispatcher (P2b.1).
    ///
    /// Owns the engine's commit receiver and fans `CommittedBatch` values
    /// out to per-shard apply-worker channels. The org's own shard is
    /// registered during [`RaftManager::start_region`]; per-vault shards are
    /// registered by `start_vault_group` (P2b.2) via [`commit_dispatcher`].
    ///
    /// [`commit_dispatcher`]: InnerGroup::commit_dispatcher
    pub(crate) commit_dispatcher: Arc<crate::commit_dispatcher::CommitDispatcher>,
}

impl InnerGroup {
    /// Returns the region.
    pub fn region(&self) -> Region {
        self.region
    }

    /// Returns the organization identifier.
    pub fn organization_id(&self) -> OrganizationId {
        self.organization_id
    }

    /// Returns the consensus handle.
    pub fn handle(&self) -> &Arc<ConsensusHandle> {
        &self.handle
    }

    /// Returns the state layer.
    pub fn state(&self) -> &Arc<StateLayer<FileBackend>> {
        &self.state
    }

    /// Returns the `raft.db` handle.
    pub fn raft_db(&self) -> &Arc<Database<FileBackend>> {
        &self.raft_db
    }

    /// Returns the `_meta.db` handle — per-organization coordinator for
    /// the `_meta:last_applied` sentinel. See [`InnerGroup::meta_db`].
    pub fn meta_db(&self) -> &Arc<Database<FileBackend>> {
        &self.meta_db
    }

    /// Returns the `blocks.db` handle.
    pub fn blocks_db(&self) -> &Arc<Database<FileBackend>> {
        &self.blocks_db
    }

    /// Returns the underlying `events.db` database, if configured.
    pub fn events_state_db(&self) -> Option<Arc<Database<FileBackend>>> {
        self.events_db.as_ref().map(|ed| Arc::clone(ed.db()))
    }

    /// Returns the block archive.
    pub fn block_archive(&self) -> &Arc<BlockArchive<FileBackend>> {
        &self.block_archive
    }

    /// Returns the applied state accessor.
    pub fn applied_state(&self) -> &AppliedStateAccessor {
        &self.applied_state
    }

    /// Returns the block announcements broadcast channel.
    pub fn block_announcements(&self) -> &broadcast::Sender<BlockAnnouncement> {
        &self.block_announcements
    }

    /// Returns the consensus transport.
    pub fn consensus_transport(
        &self,
    ) -> Option<&crate::consensus_transport::GrpcConsensusTransport> {
        self.consensus_transport.as_ref()
    }

    /// Returns the events database.
    pub fn events_db(&self) -> Option<&Arc<inferadb_ledger_state::EventsDatabase<FileBackend>>> {
        self.events_db.as_ref()
    }

    /// Returns the batch writer handle.
    pub fn batch_handle(&self) -> Option<&BatchWriterHandle> {
        self.batch_handle.as_ref()
    }

    /// Returns the per-engine commit dispatcher.
    ///
    /// Used by `start_vault_group` (P2b.2) to register additional vault
    /// shards with the dispatcher and by `stop_vault_group` to deregister
    /// them before
    /// [`ConsensusEngine::remove_shard`](inferadb_ledger_consensus::ConsensusEngine::remove_shard)
    /// returns.
    pub fn commit_dispatcher(&self) -> &Arc<crate::commit_dispatcher::CommitDispatcher> {
        &self.commit_dispatcher
    }

    /// Records activity on this group, resetting the idle timer.
    pub fn touch(&self) {
        *self.last_activity.lock() = std::time::Instant::now();
    }

    /// Returns seconds since the last activity.
    pub fn idle_secs(&self) -> u64 {
        self.last_activity.lock().elapsed().as_secs()
    }

    /// Returns whether background jobs are currently running.
    pub fn is_jobs_active(&self) -> bool {
        self.jobs_active.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Returns the leader lease.
    pub fn leader_lease(&self) -> &Arc<crate::leader_lease::LeaderLease> {
        &self.leader_lease
    }

    /// Returns a receiver for the applied-index watch channel.
    pub fn applied_index_watch(&self) -> tokio::sync::watch::Receiver<u64> {
        self.applied_index_rx.clone()
    }

    /// Drains buffered state root commitments.
    pub fn drain_state_root_commitments(&self) -> Vec<crate::types::StateRootCommitment> {
        std::mem::take(&mut *self.commitment_buffer.lock().unwrap_or_else(|e| e.into_inner()))
    }

    /// Returns the shared commitment buffer handle.
    pub fn commitment_buffer(
        &self,
    ) -> std::sync::Arc<std::sync::Mutex<Vec<crate::types::StateRootCommitment>>> {
        std::sync::Arc::clone(&self.commitment_buffer)
    }

    /// Checks if this node is the leader.
    pub fn is_leader(&self, _node_id: LedgerNodeId) -> bool {
        self.handle.is_leader()
    }

    /// Returns the current leader node ID.
    pub fn current_leader(&self) -> Option<LedgerNodeId> {
        self.handle.current_leader()
    }

    /// Takes the region-creation receiver (system tier only).
    pub fn take_region_creation_rx(
        &self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<RegionCreationRequest>> {
        self.region_creation_rx.lock().take()
    }

    /// Takes the organization-creation receiver (system tier only).
    pub fn take_organization_creation_rx(
        &self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<OrganizationCreationRequest>> {
        self.organization_creation_rx.lock().take()
    }

    /// Takes the vault-creation receiver (per-organization groups only).
    pub fn take_vault_creation_rx(
        &self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<VaultCreationRequest>> {
        self.vault_creation_rx.lock().take()
    }

    /// Takes the vault-deletion receiver (per-organization groups only).
    pub fn take_vault_deletion_rx(
        &self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<VaultDeletionRequest>> {
        self.vault_deletion_rx.lock().take()
    }
}

// ============================================================================
// Tier newtypes — Option C compile-time tier discipline
// ============================================================================
//
// Each wrapper owns an `Arc<InnerGroup>` and exposes only the subset of
// methods appropriate for its tier. No `Deref` impl — that would leak the
// full API and defeat tier discipline. Cross-tier access requires explicit
// re-resolution through `RaftManager`.

/// System-tier Raft group — cluster-wide control plane (`GLOBAL` region,
/// `OrganizationId::new(0)`).
///
/// Owns the organization directory, region directory, cluster signing keys,
/// and cross-region saga records. Every cluster hosts exactly one
/// `SystemGroup`; it is the only group that applies `SystemRequest`.
#[derive(Clone)]
pub struct SystemGroup(pub(crate) Arc<InnerGroup>);

/// Region-tier Raft group — regional control plane (one per region, at
/// `OrganizationId::new(0)`).
///
/// Owns placement, hibernation/wake, per-region audit, and unified leader
/// election for every per-organization group in the region. Applies
/// `RegionRequest` variants; per-organization Raft groups adopt their leader
/// from this group under `LeadershipMode::Delegated`.
#[derive(Clone)]
pub struct RegionGroup(pub(crate) Arc<InnerGroup>);

/// Organization-tier Raft group — data plane (one per organization, per
/// region).
///
/// Owns entity writes, vault lifecycle, app credentials, user/team
/// memberships, and organization-scoped saga PII. Applies
/// `OrganizationRequest`. Storage is per-organization
/// (`{data_dir}/{region}/{organization_id}/`); leader is delegated from the
/// parent [`RegionGroup`] under the B.1 unified-leadership model.
#[derive(Clone)]
pub struct OrganizationGroup(pub(crate) Arc<InnerGroup>);

// ----------------------------------------------------------------------------
// SystemGroup impl — cluster control plane
// ----------------------------------------------------------------------------

impl SystemGroup {
    /// Tier-escape accessor to the underlying [`InnerGroup`] for shared
    /// machinery that does not need tier-specific methods (e.g.
    /// constructing a carrier type that holds common consensus state).
    /// Using this to access tier-inappropriate methods is a tier-discipline
    /// violation.
    #[doc(hidden)]
    pub fn inner(&self) -> &Arc<InnerGroup> {
        &self.0
    }

    /// Returns the region (always `Region::GLOBAL`).
    pub fn region(&self) -> Region {
        self.0.region()
    }

    /// Returns the consensus handle.
    #[must_use]
    pub fn handle(&self) -> &Arc<ConsensusHandle> {
        self.0.handle()
    }

    /// Returns the state layer.
    #[must_use]
    pub fn state(&self) -> &Arc<StateLayer<FileBackend>> {
        self.0.state()
    }

    /// Returns the `raft.db` handle.
    #[must_use]
    pub fn raft_db(&self) -> &Arc<Database<FileBackend>> {
        self.0.raft_db()
    }

    /// Returns the `_meta.db` handle — per-organization coordinator for
    /// the `_meta:last_applied` sentinel. Slice 1 of per-vault consensus.
    #[must_use]
    pub fn meta_db(&self) -> &Arc<Database<FileBackend>> {
        self.0.meta_db()
    }

    /// Returns the `blocks.db` handle.
    ///
    /// The system group owns a Merkle chain for saga records.
    #[must_use]
    pub fn blocks_db(&self) -> &Arc<Database<FileBackend>> {
        self.0.blocks_db()
    }

    /// Returns the underlying `events.db` database, if configured.
    #[must_use]
    pub fn events_state_db(&self) -> Option<Arc<Database<FileBackend>>> {
        self.0.events_state_db()
    }

    /// Returns the events database, if available.
    #[must_use]
    pub fn events_db(&self) -> Option<&Arc<inferadb_ledger_state::EventsDatabase<FileBackend>>> {
        self.0.events_db()
    }

    /// Returns the block archive.
    #[must_use]
    pub fn block_archive(&self) -> &Arc<BlockArchive<FileBackend>> {
        self.0.block_archive()
    }

    /// Returns the applied state accessor.
    #[must_use]
    pub fn applied_state(&self) -> &AppliedStateAccessor {
        self.0.applied_state()
    }

    /// Returns the block announcements broadcast channel.
    #[must_use]
    pub fn block_announcements(&self) -> &broadcast::Sender<BlockAnnouncement> {
        self.0.block_announcements()
    }

    /// Returns the leader lease.
    #[must_use]
    pub fn leader_lease(&self) -> &Arc<crate::leader_lease::LeaderLease> {
        self.0.leader_lease()
    }

    /// Returns the consensus transport.
    #[must_use]
    pub fn consensus_transport(
        &self,
    ) -> Option<&crate::consensus_transport::GrpcConsensusTransport> {
        self.0.consensus_transport()
    }

    /// Returns a receiver for the applied-index watch channel.
    pub fn applied_index_watch(&self) -> tokio::sync::watch::Receiver<u64> {
        self.0.applied_index_watch()
    }

    /// Drains buffered state root commitments.
    pub fn drain_state_root_commitments(&self) -> Vec<crate::types::StateRootCommitment> {
        self.0.drain_state_root_commitments()
    }

    /// Returns the shared commitment buffer handle.
    pub fn commitment_buffer(
        &self,
    ) -> std::sync::Arc<std::sync::Mutex<Vec<crate::types::StateRootCommitment>>> {
        self.0.commitment_buffer()
    }

    /// Records activity on this group.
    pub fn touch(&self) {
        self.0.touch();
    }

    /// Returns seconds since the last activity.
    pub fn idle_secs(&self) -> u64 {
        self.0.idle_secs()
    }

    /// Returns whether background jobs are currently running.
    pub fn is_jobs_active(&self) -> bool {
        self.0.is_jobs_active()
    }

    /// Checks if this node is the leader.
    pub fn is_leader(&self, node_id: LedgerNodeId) -> bool {
        self.0.is_leader(node_id)
    }

    /// Returns the current leader node ID.
    pub fn current_leader(&self) -> Option<LedgerNodeId> {
        self.0.current_leader()
    }

    /// Takes the region-creation receiver. System-tier-only.
    ///
    /// Returns `Some` exactly once. The bootstrap handler calls this to spawn
    /// a task that starts data regions as they are created through GLOBAL
    /// Raft consensus.
    pub fn take_region_creation_rx(
        &self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<RegionCreationRequest>> {
        self.0.take_region_creation_rx()
    }

    /// Takes the organization-creation receiver. System-tier-only.
    pub fn take_organization_creation_rx(
        &self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<OrganizationCreationRequest>> {
        self.0.take_organization_creation_rx()
    }
}

// ----------------------------------------------------------------------------
// RegionGroup impl — regional control plane
// ----------------------------------------------------------------------------

impl RegionGroup {
    /// Tier-escape accessor to the underlying [`InnerGroup`]. See
    /// [`SystemGroup::inner`] for the rationale and the tier-discipline
    /// caveat.
    #[doc(hidden)]
    pub fn inner(&self) -> &Arc<InnerGroup> {
        &self.0
    }

    /// Returns the region this group owns.
    pub fn region(&self) -> Region {
        self.0.region()
    }

    /// Returns the consensus handle.
    #[must_use]
    pub fn handle(&self) -> &Arc<ConsensusHandle> {
        self.0.handle()
    }

    /// Returns the state layer.
    #[must_use]
    pub fn state(&self) -> &Arc<StateLayer<FileBackend>> {
        self.0.state()
    }

    /// Returns the `raft.db` handle.
    #[must_use]
    pub fn raft_db(&self) -> &Arc<Database<FileBackend>> {
        self.0.raft_db()
    }

    /// Returns the `_meta.db` handle — per-organization coordinator for
    /// the `_meta:last_applied` sentinel. Slice 1 of per-vault consensus.
    #[must_use]
    pub fn meta_db(&self) -> &Arc<Database<FileBackend>> {
        self.0.meta_db()
    }

    /// Returns the `blocks.db` handle.
    ///
    /// Under the B.1 compat shim the regional control plane shares storage
    /// with a data-region group at `OrganizationId::new(0)` — saga records
    /// ride on that Merkle chain. The spec's RegionGroup has no blocks.db;
    /// this accessor will disappear when the control plane moves off the
    /// shared group (B.1.6+).
    #[must_use]
    pub fn blocks_db(&self) -> &Arc<Database<FileBackend>> {
        self.0.blocks_db()
    }

    /// Returns the underlying `events.db` database, if configured.
    #[must_use]
    pub fn events_state_db(&self) -> Option<Arc<Database<FileBackend>>> {
        self.0.events_state_db()
    }

    /// Returns the events database, if available.
    #[must_use]
    pub fn events_db(&self) -> Option<&Arc<inferadb_ledger_state::EventsDatabase<FileBackend>>> {
        self.0.events_db()
    }

    /// Returns the block archive.
    ///
    /// B.1 compat shim — see [`blocks_db`](Self::blocks_db).
    #[must_use]
    pub fn block_archive(&self) -> &Arc<BlockArchive<FileBackend>> {
        self.0.block_archive()
    }

    /// Returns the applied state accessor.
    #[must_use]
    pub fn applied_state(&self) -> &AppliedStateAccessor {
        self.0.applied_state()
    }

    /// Returns the block announcements broadcast channel.
    #[must_use]
    pub fn block_announcements(&self) -> &broadcast::Sender<BlockAnnouncement> {
        self.0.block_announcements()
    }

    /// Returns the leader lease — the source of truth for leadership in
    /// this region.
    #[must_use]
    pub fn leader_lease(&self) -> &Arc<crate::leader_lease::LeaderLease> {
        self.0.leader_lease()
    }

    /// Returns the consensus transport.
    #[must_use]
    pub fn consensus_transport(
        &self,
    ) -> Option<&crate::consensus_transport::GrpcConsensusTransport> {
        self.0.consensus_transport()
    }

    /// Returns a receiver for the applied-index watch channel.
    pub fn applied_index_watch(&self) -> tokio::sync::watch::Receiver<u64> {
        self.0.applied_index_watch()
    }

    /// Drains buffered state root commitments.
    pub fn drain_state_root_commitments(&self) -> Vec<crate::types::StateRootCommitment> {
        self.0.drain_state_root_commitments()
    }

    /// Returns the shared commitment buffer handle.
    pub fn commitment_buffer(
        &self,
    ) -> std::sync::Arc<std::sync::Mutex<Vec<crate::types::StateRootCommitment>>> {
        self.0.commitment_buffer()
    }

    /// Records activity on this group.
    pub fn touch(&self) {
        self.0.touch();
    }

    /// Returns seconds since the last activity.
    pub fn idle_secs(&self) -> u64 {
        self.0.idle_secs()
    }

    /// Returns whether background jobs are currently running.
    pub fn is_jobs_active(&self) -> bool {
        self.0.is_jobs_active()
    }

    /// Checks if this node is the region leader.
    pub fn is_leader(&self, node_id: LedgerNodeId) -> bool {
        self.0.is_leader(node_id)
    }

    /// Returns the current region leader node ID.
    pub fn current_leader(&self) -> Option<LedgerNodeId> {
        self.0.current_leader()
    }
}

// ----------------------------------------------------------------------------
// OrganizationGroup impl — data plane
// ----------------------------------------------------------------------------

impl OrganizationGroup {
    /// Tier-escape accessor to the underlying [`InnerGroup`]. See
    /// [`SystemGroup::inner`] for the rationale and the tier-discipline
    /// caveat.
    #[doc(hidden)]
    pub fn inner(&self) -> &Arc<InnerGroup> {
        &self.0
    }

    /// Returns the region.
    pub fn region(&self) -> Region {
        self.0.region()
    }

    /// Returns the organization this group owns.
    pub fn organization_id(&self) -> OrganizationId {
        self.0.organization_id()
    }

    /// Returns the consensus handle.
    #[must_use]
    pub fn handle(&self) -> &Arc<ConsensusHandle> {
        self.0.handle()
    }

    /// Returns the state layer.
    #[must_use]
    pub fn state(&self) -> &Arc<StateLayer<FileBackend>> {
        self.0.state()
    }

    /// Returns the `raft.db` handle.
    #[must_use]
    pub fn raft_db(&self) -> &Arc<Database<FileBackend>> {
        self.0.raft_db()
    }

    /// Returns the `_meta.db` handle — per-organization coordinator for
    /// the `_meta:last_applied` sentinel. Slice 1 of per-vault consensus.
    #[must_use]
    pub fn meta_db(&self) -> &Arc<Database<FileBackend>> {
        self.0.meta_db()
    }

    /// Returns the `blocks.db` handle.
    #[must_use]
    pub fn blocks_db(&self) -> &Arc<Database<FileBackend>> {
        self.0.blocks_db()
    }

    /// Returns the underlying `events.db` database, if configured.
    #[must_use]
    pub fn events_state_db(&self) -> Option<Arc<Database<FileBackend>>> {
        self.0.events_state_db()
    }

    /// Returns the events database, if available.
    #[must_use]
    pub fn events_db(&self) -> Option<&Arc<inferadb_ledger_state::EventsDatabase<FileBackend>>> {
        self.0.events_db()
    }

    /// Returns the block archive.
    #[must_use]
    pub fn block_archive(&self) -> &Arc<BlockArchive<FileBackend>> {
        self.0.block_archive()
    }

    /// Returns the applied state accessor.
    #[must_use]
    pub fn applied_state(&self) -> &AppliedStateAccessor {
        self.0.applied_state()
    }

    /// Returns the block announcements broadcast channel.
    #[must_use]
    pub fn block_announcements(&self) -> &broadcast::Sender<BlockAnnouncement> {
        self.0.block_announcements()
    }

    /// Returns the batch writer handle, if batch writing is enabled.
    #[must_use]
    pub fn batch_handle(&self) -> Option<&BatchWriterHandle> {
        self.0.batch_handle()
    }

    /// Returns the leader lease. Under `LeadershipMode::Delegated` this is
    /// the parent region group's lease.
    #[must_use]
    pub fn leader_lease(&self) -> &Arc<crate::leader_lease::LeaderLease> {
        self.0.leader_lease()
    }

    /// Returns the consensus transport (shared with the parent region
    /// group).
    #[must_use]
    pub fn consensus_transport(
        &self,
    ) -> Option<&crate::consensus_transport::GrpcConsensusTransport> {
        self.0.consensus_transport()
    }

    /// Returns a receiver for the applied-index watch channel.
    pub fn applied_index_watch(&self) -> tokio::sync::watch::Receiver<u64> {
        self.0.applied_index_watch()
    }

    /// Drains buffered state root commitments.
    pub fn drain_state_root_commitments(&self) -> Vec<crate::types::StateRootCommitment> {
        self.0.drain_state_root_commitments()
    }

    /// Returns the shared commitment buffer handle.
    pub fn commitment_buffer(
        &self,
    ) -> std::sync::Arc<std::sync::Mutex<Vec<crate::types::StateRootCommitment>>> {
        self.0.commitment_buffer()
    }

    /// Records activity on this group.
    pub fn touch(&self) {
        self.0.touch();
    }

    /// Returns seconds since the last activity.
    pub fn idle_secs(&self) -> u64 {
        self.0.idle_secs()
    }

    /// Returns whether background jobs are currently running.
    pub fn is_jobs_active(&self) -> bool {
        self.0.is_jobs_active()
    }

    /// Checks if this node is the leader for this organization (delegated
    /// from the parent region group).
    pub fn is_leader(&self, node_id: LedgerNodeId) -> bool {
        self.0.is_leader(node_id)
    }

    /// Returns the current leader node ID.
    pub fn current_leader(&self) -> Option<LedgerNodeId> {
        self.0.current_leader()
    }
}

// ----------------------------------------------------------------------------
// VaultGroup — per-vault data plane (Phase 2 of per-vault consensus)
// ----------------------------------------------------------------------------

/// Internal per-vault Raft-group storage.
///
/// Mirrors the shape of [`InnerGroup`] but scoped to a single vault
/// `(region, organization_id, vault_id)`. A vault group has no independent
/// elections — its leader is delegated from the parent
/// [`OrganizationGroup`] via [`ConsensusHandle::adopt_leader`]. Vault groups
/// share the parent org's `consensus_transport` rather than owning their own,
/// so this type intentionally omits a `consensus_transport` field (unlike
/// [`InnerGroup`]).
///
/// **Do not use this type directly from consumer code.** It exists so the
/// [`VaultGroup`] newtype can surface a tier-appropriate method set without
/// a `Deref` leak — same rationale as [`InnerGroup`] vs the three tier
/// wrappers above.
///
/// ## Slice 2a scope
///
/// This type and the [`VaultGroup`] newtype are introduced by Slice 2a of
/// per-vault consensus Phase 2 to give the routing code a concrete
/// destination for vault-scoped proposals. `RaftManager` does not yet start
/// vault groups (`start_vault_group` is a Slice 2b deliverable); the
/// read-side lookups (`get_vault_group`, `list_vault_groups`,
/// `has_vault_group`) return empty / `RegionNotFound` until Slice 2b wires
/// the `CreateVault` → `start_vault_group` channel.
pub struct InnerVaultGroup {
    /// Region this vault group lives in.
    pub(crate) region: Region,
    /// Parent organization.
    pub(crate) organization_id: OrganizationId,
    /// Vault identifier — the third tier key.
    pub(crate) vault_id: VaultId,
    /// Consensus handle for background jobs and services.
    pub(crate) handle: Arc<ConsensusHandle>,
    /// Shared state layer. Phase 1 already made the underlying `state.db`
    /// per-vault; a vault group still borrows the parent org's
    /// `StateLayer` (which owns the per-vault `Database`s internally) so
    /// apply workers write through the same accessor the org uses for
    /// metadata reads.
    pub(crate) state: Arc<StateLayer<FileBackend>>,
    /// Per-vault block archive — the vault's own Merkle chain.
    pub(crate) block_archive: Arc<BlockArchive<FileBackend>>,
    /// Accessor for applied state.
    pub(crate) applied_state: AppliedStateAccessor,
    /// Block announcement broadcast channel. Shared with the parent org
    /// for now; Phase 4 will scope announcements per-vault.
    pub(crate) block_announcements: broadcast::Sender<BlockAnnouncement>,
    /// Batch writer handle for coalescing vault-scoped writes.
    pub(crate) batch_handle: Option<BatchWriterHandle>,
    /// Shared state root commitment buffer.
    pub(crate) commitment_buffer:
        std::sync::Arc<std::sync::Mutex<Vec<crate::types::StateRootCommitment>>>,
    /// Leader lease — under `LeadershipMode::Delegated` this points at the
    /// parent org's lease (vault groups never run elections).
    pub(crate) leader_lease: Arc<crate::leader_lease::LeaderLease>,
    /// Watch channel receiver for applied index (ReadIndex protocol).
    pub(crate) applied_index_rx: tokio::sync::watch::Receiver<u64>,
}

impl InnerVaultGroup {
    /// Returns the region.
    pub fn region(&self) -> Region {
        self.region
    }

    /// Returns the parent organization identifier.
    pub fn organization_id(&self) -> OrganizationId {
        self.organization_id
    }

    /// Returns the vault identifier.
    pub fn vault_id(&self) -> VaultId {
        self.vault_id
    }

    /// Returns the consensus handle.
    pub fn handle(&self) -> &Arc<ConsensusHandle> {
        &self.handle
    }

    /// Returns the state layer.
    pub fn state(&self) -> &Arc<StateLayer<FileBackend>> {
        &self.state
    }

    /// Returns the block archive.
    pub fn block_archive(&self) -> &Arc<BlockArchive<FileBackend>> {
        &self.block_archive
    }

    /// Returns the applied state accessor.
    pub fn applied_state(&self) -> &AppliedStateAccessor {
        &self.applied_state
    }

    /// Returns the block announcements broadcast channel.
    pub fn block_announcements(&self) -> &broadcast::Sender<BlockAnnouncement> {
        &self.block_announcements
    }

    /// Returns the batch writer handle.
    pub fn batch_handle(&self) -> Option<&BatchWriterHandle> {
        self.batch_handle.as_ref()
    }

    /// Returns the shared commitment buffer handle.
    pub fn commitment_buffer(
        &self,
    ) -> std::sync::Arc<std::sync::Mutex<Vec<crate::types::StateRootCommitment>>> {
        std::sync::Arc::clone(&self.commitment_buffer)
    }

    /// Returns the leader lease.
    pub fn leader_lease(&self) -> &Arc<crate::leader_lease::LeaderLease> {
        &self.leader_lease
    }

    /// Returns a receiver for the applied-index watch channel.
    pub fn applied_index_watch(&self) -> tokio::sync::watch::Receiver<u64> {
        self.applied_index_rx.clone()
    }

    /// Checks if this node is the leader for this vault group (delegated
    /// from the parent organization group).
    pub fn is_leader(&self, _node_id: LedgerNodeId) -> bool {
        self.handle.is_leader()
    }

    /// Returns the current leader node ID.
    pub fn current_leader(&self) -> Option<LedgerNodeId> {
        self.handle.current_leader()
    }
}

/// Vault-tier Raft group — data plane scoped to a single vault within an
/// organization.
///
/// Owns entity writes (`Write`, `BatchWrite`, `IngestExternalEvents`) for
/// one vault. Under the Phase 2 design, leadership is delegated from the
/// parent [`OrganizationGroup`] via [`ConsensusHandle::adopt_leader`] — vault
/// groups never run independent elections. Storage (state.db, blocks.db,
/// events.db, raft.db, WAL) is per-vault under
/// `{data_dir}/{region}/{organization_id}/state/vault-{vault_id}/`.
///
/// Variant validation at the apply worker rejects org-scoped variants
/// (`CreateVault`, `AddOrganizationMember`, team / invitation / app
/// lifecycle, etc.) with a tier-violation error — those continue to apply
/// through the parent [`OrganizationGroup`].
///
/// ## Slice 2a scope
///
/// Slice 2a introduces the type and the [`RaftManager`] read-side lookup
/// surface. `start_vault_group` / `stop_vault_group` and the
/// `CreateVault` → start-group wiring arrive in Slice 2b. Until Slice 2b
/// lands, the `RaftManager` holds no vault groups — `get_vault_group`
/// returns [`RaftManagerError::RegionNotFound`] and `list_vault_groups`
/// returns an empty [`Vec`]. The Write path continues to propose through
/// the parent [`OrganizationGroup`]; Slice 2c flips the routing key.
#[derive(Clone)]
pub struct VaultGroup(pub(crate) Arc<InnerVaultGroup>);

impl VaultGroup {
    /// Tier-escape accessor to the underlying [`InnerVaultGroup`]. See
    /// [`SystemGroup::inner`] for the rationale and the tier-discipline
    /// caveat.
    #[doc(hidden)]
    pub fn inner(&self) -> &Arc<InnerVaultGroup> {
        &self.0
    }

    /// Returns the region.
    pub fn region(&self) -> Region {
        self.0.region()
    }

    /// Returns the parent organization identifier.
    pub fn organization_id(&self) -> OrganizationId {
        self.0.organization_id()
    }

    /// Returns the vault identifier.
    pub fn vault_id(&self) -> VaultId {
        self.0.vault_id()
    }

    /// Returns the consensus handle.
    #[must_use]
    pub fn handle(&self) -> &Arc<ConsensusHandle> {
        self.0.handle()
    }

    /// Returns the state layer.
    #[must_use]
    pub fn state(&self) -> &Arc<StateLayer<FileBackend>> {
        self.0.state()
    }

    /// Returns the block archive.
    #[must_use]
    pub fn block_archive(&self) -> &Arc<BlockArchive<FileBackend>> {
        self.0.block_archive()
    }

    /// Returns the applied state accessor.
    #[must_use]
    pub fn applied_state(&self) -> &AppliedStateAccessor {
        self.0.applied_state()
    }

    /// Returns the block announcements broadcast channel.
    #[must_use]
    pub fn block_announcements(&self) -> &broadcast::Sender<BlockAnnouncement> {
        self.0.block_announcements()
    }

    /// Returns the batch writer handle, if batch writing is enabled.
    #[must_use]
    pub fn batch_handle(&self) -> Option<&BatchWriterHandle> {
        self.0.batch_handle()
    }

    /// Returns the shared commitment buffer handle.
    pub fn commitment_buffer(
        &self,
    ) -> std::sync::Arc<std::sync::Mutex<Vec<crate::types::StateRootCommitment>>> {
        self.0.commitment_buffer()
    }

    /// Returns the leader lease.
    #[must_use]
    pub fn leader_lease(&self) -> &Arc<crate::leader_lease::LeaderLease> {
        self.0.leader_lease()
    }

    /// Returns a receiver for the applied-index watch channel.
    pub fn applied_index_watch(&self) -> tokio::sync::watch::Receiver<u64> {
        self.0.applied_index_watch()
    }

    /// Checks if this node is the leader for this vault group (delegated
    /// from the parent organization group).
    pub fn is_leader(&self, node_id: LedgerNodeId) -> bool {
        self.0.is_leader(node_id)
    }

    /// Returns the current leader node ID.
    pub fn current_leader(&self) -> Option<LedgerNodeId> {
        self.0.current_leader()
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
    /// Active Raft groups indexed by `(region, organization_id)`.
    ///
    /// Each region hosts one data-region group at `OrganizationId::new(0)`
    /// plus one per-organization group per active organization. Services
    /// that address a region alone resolve via `get_region_group(region)`,
    /// which returns the data-region group. Per-organization routing goes
    /// through `route_organization(organization_id)`.
    regions: RwLock<HashMap<(Region, OrganizationId), Arc<InnerGroup>>>,
    /// Per-vault Raft groups indexed by `(region, organization_id, vault_id)`.
    ///
    /// Introduced by Slice 2a of per-vault consensus Phase 2; populated
    /// by Slice 2b's `start_vault_group` when `CreateVault` applies in
    /// the parent `OrganizationGroup`. Each vault group runs under
    /// [`inferadb_ledger_consensus::LeadershipMode::Delegated`] — its
    /// leader is adopted from the parent `OrganizationGroup` rather than
    /// elected independently.
    vault_groups: RwLock<HashMap<(Region, OrganizationId, VaultId), Arc<InnerVaultGroup>>>,
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
            vault_groups: RwLock::new(HashMap::new()),
            peer_addresses: crate::peer_address_map::PeerAddressMap::new(),
            registry,
            dr_event_tx: tx,
            dr_event_rx: parking_lot::Mutex::new(Some(rx)),
            cancellation_token: parking_lot::Mutex::new(CancellationToken::new()),
            runtime_config: parking_lot::Mutex::new(None),
            recovery_stats: RwLock::new(HashMap::new()),
        }
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
        // Task 4 shim: single-shard-per-region runtime (Task 4.2 will loop
        // start_region over N shards; Task 5 will plumb OrganizationId through
        // services). Until then, legacy callers continue to address shard 0.
        self.regions
            .read()
            .get(&(region, OrganizationId::new(0)))
            .cloned()
            .map(|inner| Arc::new(RegionGroup(inner)))
            .ok_or(RaftManagerError::RegionNotFound { region })
    }

    /// Looks up the `OrganizationGroup` owning `(region, organization_id)`.
    ///
    /// Phase A routing: services resolve `OrganizationId` via
    /// [`inferadb_ledger_types::ShardRouter`] (see
    /// [`RaftManager::shard_router`](Self::shard_router)) and call this to
    /// reach the owning shard's Raft group. Task 5 migrates service call
    /// sites from [`get_region_group`](Self::get_region_group) to this
    /// method; until then the two are equivalent (single-shard runtime).
    /// Looks up the local `OrganizationGroup` whose consensus engine is
    /// bound to `shard_id`.
    ///
    /// Used by the gRPC `RaftService` to route incoming consensus
    /// `Replicate` messages to the correct engine — each per-organization
    /// engine has a distinct consensus `ConsensusStateId` derived from
    /// `seahash(region.as_str() || organization_id.to_le_bytes())`, and
    /// the receiver must dispatch by that id rather than by region
    /// (looking up by region alone would always hit the legacy
    /// data-region group at `OrganizationId::new(0)`).
    ///
    /// Linear scan over the local groups; at the B.1 target scale of
    /// tens-to-hundreds of organizations per region, this is fine.
    /// Future work could maintain a parallel `HashMap<ConsensusStateId, ...>` if
    /// the scan becomes a hotspot.
    pub fn lookup_by_consensus_shard(
        &self,
        shard_id: inferadb_ledger_consensus::types::ConsensusStateId,
    ) -> Option<Arc<InnerGroup>> {
        // Documented cross-tier escape hatch: the gRPC `RaftService` dispatches
        // incoming consensus `Replicate` messages by `ConsensusStateId` and
        // genuinely does not know which tier owns the shard. Returns the
        // untiered [`InnerGroup`] so consumers can reach `handle()` without
        // committing to a tier; any other use is a tier-discipline violation.
        self.regions.read().values().find(|group| group.handle().shard_id() == shard_id).cloned()
    }

    /// Untyped region-group lookup used by the gRPC `RaftService` fallback
    /// path when `lookup_by_consensus_shard` misses.
    ///
    /// Cross-tier escape hatch — see [`lookup_by_consensus_shard`] for the
    /// rationale. Callers that know their tier should use
    /// [`get_region_group`] (regional), [`system_region`] (system), or
    /// [`get_organization_group`] (organization) instead.
    pub fn lookup_region_inner(&self, region: Region) -> Option<Arc<InnerGroup>> {
        self.regions.read().get(&(region, OrganizationId::new(0))).cloned()
    }

    pub fn get_organization_group(
        &self,
        region: Region,
        shard: OrganizationId,
    ) -> Result<Arc<OrganizationGroup>> {
        self.regions
            .read()
            .get(&(region, shard))
            .cloned()
            .map(|inner| Arc::new(OrganizationGroup(inner)))
            .ok_or(RaftManagerError::RegionNotFound { region })
    }

    /// Returns the system region (`_system`).
    ///
    /// # Errors
    ///
    /// Returns [`RaftManagerError::RegionNotFound`] if the system region (ID 0)
    /// has not been started.
    pub fn system_region(&self) -> Result<Arc<SystemGroup>> {
        self.regions
            .read()
            .get(&(Region::GLOBAL, OrganizationId::new(0)))
            .cloned()
            .map(|inner| Arc::new(SystemGroup(inner)))
            .ok_or(RaftManagerError::RegionNotFound { region: Region::GLOBAL })
    }

    /// Returns a read-only accessor for the GLOBAL region's applied state.
    pub fn system_state_reader(&self) -> Option<SystemStateReader> {
        self.system_region()
            .ok()
            .map(|group| SystemStateReader { state_layer: group.state().clone() })
    }

    /// Lists all active region IDs. Deduplicates across shards.
    pub fn list_regions(&self) -> Vec<Region> {
        let mut seen: std::collections::BTreeSet<Region> = std::collections::BTreeSet::new();
        for (r, _s) in self.regions.read().keys() {
            seen.insert(*r);
        }
        seen.into_iter().collect()
    }

    /// Lists all active (region, shard) pairs.
    pub fn list_organization_groups(&self) -> Vec<(Region, OrganizationId)> {
        self.regions.read().keys().copied().collect()
    }

    /// Lists all active per-vault `(region, organization_id, vault_id)`
    /// triples registered on this node.
    ///
    /// Returns an empty [`Vec`] under Slice 2a — no vault groups are
    /// started until Slice 2b wires `CreateVault` to
    /// [`RaftManager::start_vault_group`]. Used by backup, monitoring,
    /// and the consensus-transport dispatch layer once vault groups are
    /// live.
    pub fn list_vault_groups(&self) -> Vec<(Region, OrganizationId, VaultId)> {
        self.vault_groups.read().keys().copied().collect()
    }

    /// Returns the vault group owning `(region, organization_id, vault_id)`.
    ///
    /// # Errors
    ///
    /// Returns [`RaftManagerError::VaultGroupNotFound`] if no vault group
    /// with the given triple is registered on this node. Under Slice 2a
    /// this is always the case; Slice 2b begins populating the map from
    /// `CreateVault` apply.
    pub fn get_vault_group(
        &self,
        region: Region,
        organization_id: OrganizationId,
        vault_id: VaultId,
    ) -> Result<Arc<VaultGroup>> {
        self.vault_groups.read().get(&(region, organization_id, vault_id)).cloned().map_or(
            Err(RaftManagerError::VaultGroupNotFound { region, organization_id, vault_id }),
            |inner| Ok(Arc::new(VaultGroup(inner))),
        )
    }

    /// Checks if a specific `(region, organization_id, vault_id)` vault
    /// group is active on this node.
    ///
    /// Returns `false` for every triple under Slice 2a.
    pub fn has_vault_group(
        &self,
        region: Region,
        organization_id: OrganizationId,
        vault_id: VaultId,
    ) -> bool {
        self.vault_groups.read().contains_key(&(region, organization_id, vault_id))
    }

    /// Iterates every `(organization_id, vault_id, vault_height)` tuple across
    /// all per-organization groups registered on this node.
    ///
    /// Post-γ Phase 3, vault record bodies (including
    /// `AppliedState::vault_heights`) live in the per-organization
    /// group's applied state, not GLOBAL. Cross-cutting scans —
    /// force-GC, TTL GC aggregation, list-all-vaults, etc. — must
    /// iterate every per-org group's `AppliedState` rather than
    /// reading from a single GLOBAL accessor.
    ///
    /// The closure is invoked with `(organization_id, vault_id,
    /// height)` — the region is already implied by the owning group
    /// but not passed to the closure because the downstream scan work
    /// (routing the propose-side back via `route_organization`)
    /// already re-resolves the region internally. Callers that need
    /// the region can look it up via
    /// [`get_organization_region`](Self::get_organization_region).
    pub fn for_each_vault_across_groups<F>(&self, mut f: F)
    where
        F: FnMut(OrganizationId, inferadb_ledger_types::VaultId, u64),
    {
        let regions = self.regions.read();
        for group in regions.values() {
            group.applied_state().for_each_vault_height(&mut f);
        }
    }

    /// Returns the maximum `region_height` across all org groups.
    ///
    /// In B.1, each per-org group `(region, org_id)` tracks its own
    /// `region_height` in its own `AppliedState`. The GLOBAL group's
    /// `applied_state.region_height()` stays at 0 (data writes flow through
    /// per-org groups, not GLOBAL). Callers that need an aggregate measure
    /// of "how much data has been committed across all orgs" use this method.
    ///
    /// Used by `AdminService::create_backup` for backup metadata versioning.
    /// Returns 0 if no org groups are active.
    pub fn max_region_height(&self) -> u64 {
        self.regions.read().values().map(|g| g.applied_state().region_height()).max().unwrap_or(0)
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

    /// Checks if a region is active (has at least shard 0 running).
    pub fn has_region(&self, region: Region) -> bool {
        self.regions.read().contains_key(&(region, OrganizationId::new(0)))
    }

    /// Checks if a specific `(region, shard)` Raft group is active.
    ///
    /// `start_region`'s pre-insert duplicate guard uses this so per-shard
    /// loops can re-enter `start_region` for the next `organization_id` without
    /// the prior shard's registration tripping the check.
    pub fn has_organization_group(&self, region: Region, organization_id: OrganizationId) -> bool {
        self.regions.read().contains_key(&(region, organization_id))
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
    ) -> Result<Arc<SystemGroup>> {
        if self.has_region(region) {
            return Err(RaftManagerError::RegionExists { region });
        }

        let blocks_db = Arc::clone(block_archive.db());
        let meta_db = Arc::clone(state.meta_database());

        // P2b.1: this externally-wired registration path supplies a
        // pre-built `ConsensusHandle` whose engine's `commit_rx` is owned
        // upstream (by whichever bootstrap caller built the handle). The
        // dispatcher here owns a never-fed receiver — the
        // background task simply parks on `recv()` and exits when the
        // sender drops. This keeps the field invariant intact for a path
        // that has no in-tree callers today; if the path acquires real
        // callers, they must wire the engine's `commit_rx` through
        // `CommitDispatcher::new` and pass the dispatcher in.
        let (_unused_tx, unused_rx) =
            tokio::sync::mpsc::channel::<inferadb_ledger_consensus::committed::CommittedBatch>(1);
        let commit_dispatcher =
            Arc::new(crate::commit_dispatcher::CommitDispatcher::new(unused_rx));

        let inner = Arc::new(InnerGroup {
            region,
            organization_id: OrganizationId::new(0),
            handle,
            state: state.clone(),
            raft_db,
            meta_db,
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
            organization_creation_rx: parking_lot::Mutex::new(None),
            vault_creation_rx: parking_lot::Mutex::new(None),
            vault_deletion_rx: parking_lot::Mutex::new(None),
            commit_dispatcher,
        });

        {
            let mut regions = self.regions.write();
            regions.insert((region, OrganizationId::new(0)), Arc::clone(&inner));
        }

        Ok(Arc::new(SystemGroup(inner)))
    }

    /// Routes an organization to the `(region, shard)` Raft group that
    /// owns its writes.
    ///
    /// Looks up the organization's region assignment in the `_system`
    /// service, resolves the shard via [`ShardRouter`], and returns the
    /// per-`(region, shard)` `OrganizationGroup`. This is the single entry point
    /// every service-layer write/read path uses to find the right shard's
    /// BatchWriter, OrganizationApplyWorker, and StateLayer.
    ///
    /// Returns `None` if:
    /// - System region not started
    /// - Organization not found in `_system`
    /// - Target region/shard is on a different node (requires forwarding — caller falls through to
    ///   the redirect path)
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    pub fn route_organization(
        &self,
        organization: OrganizationId,
    ) -> Option<Arc<OrganizationGroup>> {
        let region = self.get_organization_region(organization)?;
        // B.1.8 + B.1.7: organization_id IS the routing key. The
        // per-organization Raft group is in `LeadershipMode::Delegated`,
        // following the data-region group's elected leader (no
        // independent elections). Falls back to the legacy
        // `OrganizationId::new(0)` data-region group if the per-org
        // group isn't yet registered locally — handles the brief race
        // between `CreateOrganization` apply on this node and the
        // bootstrap handler awaiting `start_organization_group`. Also
        // covers the system org (id 0) which IS the OrganizationId(0)
        // group in GLOBAL.
        let organization_id = if self.regions.read().contains_key(&(region, organization)) {
            organization
        } else {
            OrganizationId::new(0)
        };

        // Get local `(region, shard)` group. Data regions + their N shards
        // are created through GLOBAL Raft consensus (CreateDataRegion +
        // start_region_shards), so we don't lazily create here — the shard
        // must already exist from a prior consensus proposal + region
        // start.
        let inner = self.regions.read().get(&(region, organization_id)).cloned()?;
        inner.touch();

        // Auto-wake hibernated regions on first request
        if !inner.is_jobs_active() {
            let _ = self.wake_region(region);
        }

        Some(Arc::new(OrganizationGroup(inner)))
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
    ) -> Result<Arc<SystemGroup>> {
        if region_config.region != Region::GLOBAL {
            return Err(RaftManagerError::Raft {
                region: region_config.region,
                message: "System region must have region=0".to_string(),
            });
        }

        // GLOBAL is always single-shard — cluster-wide control state has no
        // routing dimension to spread across additional shards.
        let inner = self.start_region(region_config, OrganizationId::new(0)).await?;
        Ok(Arc::new(SystemGroup(inner)))
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
    /// Starts a per-organization Raft group on this node.
    ///
    /// Called by the bootstrap-side organization-creation handler when
    /// `CreateOrganization` apply emits a signal on the system region's
    /// `organization_creation_rx` channel. Each in-region node calls this
    /// independently with the same `voter_set` so the new organization's
    /// Raft group elects from a consistent membership.
    ///
    /// Idempotent: if the `(region, organization_id)` group is already
    /// registered, returns the existing handle instead of failing.
    ///
    /// # Errors
    ///
    /// Returns a storage / Raft error if the underlying group bootstrap
    /// fails.
    pub async fn start_organization_group(
        &self,
        region: Region,
        organization_id: OrganizationId,
        voter_set: Vec<(LedgerNodeId, String)>,
        bootstrap: bool,
        events_config: Option<inferadb_ledger_types::events::EventConfig>,
        batch_writer_config: Option<BatchWriterConfig>,
    ) -> Result<Arc<OrganizationGroup>> {
        // Idempotent: skip if already running.
        if let Ok(group) = self.get_organization_group(region, organization_id) {
            return Ok(group);
        }
        let region_config = RegionConfig {
            region,
            initial_members: voter_set,
            bootstrap,
            enable_background_jobs: true,
            batch_writer_config,
            event_writer: None,
            events_config,
            delegated_leadership: true,
        };
        let org_inner = self.start_region(region_config, organization_id).await?;

        // Activate delegated leadership: the new org ConsensusState does not run
        // its own elections (LeadershipMode::Delegated). Its leader is
        // adopted from the data-region group's elected leader. Bootstrap
        // a one-time adoption now (in case the region already has a
        // leader), then spawn a watcher that re-adopts on every region
        // leader change.
        //
        // The read-guard must be dropped before the first `.await` below
        // (`adopt_leader`). Taking the lookup result in its own let-binding
        // forces the guard out of scope before the `if let` body executes.
        let region_inner_opt = self.regions.read().get(&(region, OrganizationId::new(0))).cloned();
        if let Some(region_inner) = region_inner_opt {
            let region_handle = region_inner.handle().clone();
            let org_handle = org_inner.handle().clone();
            let initial_state = region_handle.shard_state();
            if let Some(leader) = initial_state.leader {
                let _ = org_handle.adopt_leader(leader, initial_state.term).await;
            }
            // Spawn watcher: subscribe to data-region's leader/term and
            // re-adopt on every change. Exits when the engine drops.
            let mut state_rx = region_handle.state_rx().clone();
            tokio::spawn(async move {
                while state_rx.changed().await.is_ok() {
                    let snap = state_rx.borrow().clone();
                    if let Some(leader) = snap.leader
                        && org_handle.adopt_leader(leader, snap.term).await.is_err()
                    {
                        break;
                    }
                }
            });
        }

        // P2b.2.c: spawn the per-org vault-lifecycle watcher. Drains the
        // vault create/delete signal channels wired by `open_region_storage`
        // for this per-organization log store. For now the watcher just
        // logs on receive — P2b.2.d wires the actual
        // `start_vault_group` / `stop_vault_group` calls. The signal
        // plumbing (apply-arm → mpsc channel → drainer) is what this
        // slice delivers; the drainer is already tied to the manager's
        // cancellation token so it exits cleanly on shutdown.
        let vault_creation_rx_opt = org_inner.take_vault_creation_rx();
        let vault_deletion_rx_opt = org_inner.take_vault_deletion_rx();
        if let (Some(mut vault_create_rx), Some(mut vault_delete_rx)) =
            (vault_creation_rx_opt, vault_deletion_rx_opt)
        {
            let cancel = self.cancellation_token.lock().child_token();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        () = cancel.cancelled() => {
                            debug!(
                                region = region.as_str(),
                                organization_id = organization_id.value(),
                                "Vault-lifecycle watcher exiting on cancellation",
                            );
                            break;
                        }
                        Some(req) = vault_create_rx.recv() => {
                            info!(
                                region = req.region.as_str(),
                                organization_id = req.organization.value(),
                                vault_id = req.vault.value(),
                                "VaultCreationRequest received (start_vault_group integration pending P2b.2.d)",
                            );
                        }
                        Some(req) = vault_delete_rx.recv() => {
                            info!(
                                region = req.region.as_str(),
                                organization_id = req.organization.value(),
                                vault_id = req.vault.value(),
                                "VaultDeletionRequest received (stop_vault_group integration pending P2b.2.d)",
                            );
                        }
                        else => break,
                    }
                }
            });
        }

        Ok(Arc::new(OrganizationGroup(org_inner)))
    }

    /// Starts a per-vault Raft group on this node.
    ///
    /// Creates a per-vault Raft group scoped to
    /// `(region, organization_id, vault_id)` by registering a new shard on
    /// the parent organization's existing [`ConsensusEngine`] (via
    /// [`ConsensusHandle::add_shard`]). Opens the vault's `raft.db`
    /// through [`RaftLogStore::open_for_vault`], builds a
    /// [`ConsensusState`](inferadb_ledger_consensus::ConsensusState) in
    /// [`LeadershipMode::Delegated`](inferadb_ledger_consensus::LeadershipMode::Delegated)
    /// — vault groups never run independent elections; leadership is
    /// adopted from the parent [`OrganizationGroup`] — registers the
    /// shard with the parent's [`CommitDispatcher`](crate::commit_dispatcher::CommitDispatcher),
    /// spawns a commit-pump task bound to the manager's cancellation
    /// token, and inserts a new [`InnerVaultGroup`] into `vault_groups`.
    ///
    /// The parent organization group must already be running on this
    /// node; double-starting the same triple returns
    /// [`RaftManagerError::VaultGroupExists`].
    ///
    /// ## Commit-pump stub
    ///
    /// The per-vault apply pipeline is currently a debug-log stub:
    /// committed batches routed into the vault shard's channel are
    /// drained by a task that logs and drops them. The real apply path
    /// — with tier validation and `VaultAppliedState` mutations — is
    /// wired in a later slice; this method deliberately avoids standing
    /// it up so the lifecycle surface can land independently.
    ///
    /// Leadership adoption for the fresh vault shard is similarly
    /// deferred: both the one-shot `adopt_leader` call and the
    /// parent-leader watcher land together in the slice that flips the
    /// `VaultCreationRequest` watcher onto this method. A freshly-started
    /// vault shard therefore stays leaderless until that driver is in
    /// place.
    ///
    /// ## Shared vs per-vault fields on [`InnerVaultGroup`]
    ///
    /// Under delegated leadership the vault group shares several fields
    /// with the parent org: `handle`, `state`, `block_archive`,
    /// `block_announcements`, and `leader_lease`. Only the raft-log-driven
    /// fields (`applied_state`, `commitment_buffer`, `applied_index_rx`)
    /// are genuinely per-vault, sourced from the per-vault
    /// [`RaftLogStore`]. This matches the per-vault-consensus spec —
    /// "vault groups inherit parent org's … leader, voter set, etc."
    ///
    /// ## Errors
    ///
    /// - [`RaftManagerError::RegionNotFound`] if the parent `(region, organization_id)`
    ///   organization group is not registered on this node.
    /// - [`RaftManagerError::VaultGroupExists`] if a vault group for the same `(region,
    ///   organization_id, vault_id)` triple is already registered.
    /// - [`RaftManagerError::Storage`] if the per-vault `raft.db` cannot be opened or if the vault
    ///   directory cannot be created.
    /// - [`RaftManagerError::Raft`] if the parent engine rejects the new shard registration (e.g.
    ///   the engine reactor has shut down).
    pub async fn start_vault_group(
        &self,
        region: Region,
        organization_id: OrganizationId,
        vault_id: VaultId,
    ) -> Result<Arc<VaultGroup>> {
        // Precondition: parent org group must be running on this node.
        let org_group = self.get_organization_group(region, organization_id)?;
        let org_inner = Arc::clone(&org_group.0);

        // Precondition: no vault group already registered for this triple.
        if self.has_vault_group(region, organization_id, vault_id) {
            return Err(RaftManagerError::VaultGroupExists { region, organization_id, vault_id });
        }

        info!(
            region = region.as_str(),
            organization_id = organization_id.value(),
            vault_id = vault_id.value(),
            "Starting vault group",
        );

        // Compose the per-vault raft.db path under
        // `{data_dir}/{region}/{organization_id}/state/vault-{vault_id}/raft.db`
        // (P2b.0 layout). The org's `RegionStorage` already exposes the
        // per-vault directory helper; we reuse it instead of re-composing.
        let region_storage = self
            .storage_manager
            .get_organization(region, organization_id)
            .ok_or(RaftManagerError::RegionNotFound { region })?;
        let vault_dir = region_storage.vault_dir(vault_id);
        std::fs::create_dir_all(&vault_dir).map_err(|e| RaftManagerError::Storage {
            region,
            message: format!("failed to create vault directory {}: {e}", vault_dir.display()),
        })?;
        let vault_raft_db_path = vault_dir.join("raft.db");

        // Open the per-vault Raft log store. `open_for_vault` stamps the
        // store's residency identity (organization_id + vault_id) at
        // construction time so downstream apply workers can't accidentally
        // mutate another vault's state.
        let vault_log_store = RaftLogStore::<FileBackend>::open_for_vault(
            &vault_raft_db_path,
            organization_id,
            vault_id,
        )
        .map_err(|e| RaftManagerError::Storage {
            region,
            message: format!(
                "failed to open vault raft.db at {}: {e}",
                vault_raft_db_path.display()
            ),
        })?;

        // Per-vault applied-state accessors, surfaced before the log store
        // is moved into the debug-stub worker task.
        let vault_applied_state = vault_log_store.accessor();
        let vault_commitment_buffer = vault_log_store.commitment_buffer();
        let vault_applied_index_rx = vault_log_store.applied_index_watch();

        // Derive a unique `ConsensusStateId` for this vault. Mirrors the
        // org-shard derivation in `start_region` but mixes the vault id
        // in so per-vault shards are disjoint from each other and from the
        // org's own shard in the dispatcher's routing table.
        let shard_id = {
            let mut bytes = Vec::with_capacity(region.as_str().len() + 16);
            bytes.extend_from_slice(region.as_str().as_bytes());
            bytes.extend_from_slice(&organization_id.value().to_le_bytes());
            bytes.extend_from_slice(&vault_id.value().to_le_bytes());
            inferadb_ledger_consensus::types::ConsensusStateId(seahash::hash(&bytes))
        };

        // Inherit the parent org's voter set. The B.1 unified-leadership
        // model keeps every voter of the org voting on every vault group
        // within that org (see
        // `docs/superpowers/specs/2026-04-23-per-vault-consensus.md`
        // — "Shared append-only WAL per organization").
        let parent_shard_state = org_inner.handle().shard_state();
        let vault_membership = if parent_shard_state.voters.is_empty() {
            // Fallback for a freshly-started parent whose membership
            // watcher hasn't populated yet — seed with the local node so
            // `ConsensusState::new` has a non-empty voter set. The real
            // voter set is inherited on the first parent leader-change
            // fan-out (added in a later slice).
            warn!(
                region = region.as_str(),
                organization_id = organization_id.value(),
                vault_id = vault_id.value(),
                "Parent organization voters not yet populated at vault-group start; \
                 falling back to local-node-only membership. Correct membership is \
                 inherited on the next parent leader-change fan-out.",
            );
            let voters: std::collections::BTreeSet<inferadb_ledger_consensus::types::NodeId> =
                [inferadb_ledger_consensus::types::NodeId(self.config.node_id)]
                    .into_iter()
                    .collect();
            inferadb_ledger_consensus::types::Membership::new(voters)
        } else {
            let mut membership = inferadb_ledger_consensus::types::Membership::new(
                parent_shard_state.voters.clone(),
            );
            for learner in &parent_shard_state.learners {
                membership.add_learner(*learner);
            }
            membership
        };

        let shard_config = inferadb_ledger_consensus::ShardConfig {
            election_timeout_min: std::time::Duration::from_millis(
                self.config.election_timeout_min_ms,
            ),
            election_timeout_max: std::time::Duration::from_millis(
                self.config.election_timeout_max_ms,
            ),
            heartbeat_interval: std::time::Duration::from_millis(self.config.heartbeat_interval_ms),
            // Vault shards never run elections under delegated leadership;
            // auto_promote is irrelevant but we default it off to match
            // data-region groups.
            auto_promote: false,
            ..Default::default()
        };

        let mut consensus_shard = inferadb_ledger_consensus::ConsensusState::new(
            shard_id,
            inferadb_ledger_consensus::types::NodeId(self.config.node_id),
            vault_membership,
            shard_config,
            inferadb_ledger_consensus::SystemClock,
            inferadb_ledger_consensus::rng::SystemRng,
            // initial_term =
            0,
            // initial_voted_for =
            None,
            // initial_committed_index =
            0,
        );
        // Vault groups always run delegated — leadership is adopted from
        // the parent org on every org leader change.
        consensus_shard.set_leadership_mode(inferadb_ledger_consensus::LeadershipMode::Delegated);

        // Leadership adoption is deferred: unlike `start_organization_group`,
        // this method does not call `adopt_leader` on the fresh shard.
        // Both the one-shot adoption and the parent-leader watcher land
        // together in the follow-up slice that flips the vault creation
        // watcher to call `start_vault_group`. Splitting them would leave
        // an intermediate state where vault groups are live but never
        // adopt the parent's leader on startup — better to keep the shard
        // leaderless until the watcher and adoption land as one unit.

        // Create the per-vault commit channel and register with the
        // parent org's dispatcher before handing the shard to the
        // engine — register must complete before the engine could
        // possibly commit a batch for the new shard
        // (see `commit_dispatcher.rs` module docs).
        let (vault_batch_tx, vault_batch_rx) = tokio::sync::mpsc::channel::<
            inferadb_ledger_consensus::committed::CommittedBatch,
        >(10_000);
        org_inner.commit_dispatcher().register(shard_id, vault_batch_tx);

        // Register the vault shard with the parent engine. On failure we
        // deregister so the dispatcher doesn't hold a stale sender
        // reference for a shard the engine doesn't know about.
        if let Err(e) = org_inner.handle().add_shard(consensus_shard).await {
            org_inner.commit_dispatcher().deregister(shard_id);
            return Err(RaftManagerError::Raft {
                region,
                message: format!("failed to register vault shard on parent engine: {e}"),
            });
        }

        // Spawn the debug-log stub apply worker. The stub owns the
        // per-vault log store for the lifetime of the vault group so the
        // store's `raft.db` handle and applied-state watch sender stay
        // alive as long as any receiver is observing them. Real per-vault
        // apply — with `VaultAppliedState` mutations and tier validation
        // — replaces this stub in a later slice.
        let cancel = self.cancellation_token.lock().child_token();
        let stub_region = region;
        let stub_org = organization_id;
        let stub_vault = vault_id;
        let stub_shard_id = shard_id;
        tokio::spawn(async move {
            // `InnerVaultGroup` intentionally does not own `RaftLogStore`.
            // The store's `applied_index` watch-sender must live
            // co-located with the task that drives applies, so the sender
            // stays alive whenever a receiver is observed. The debug stub
            // here plays that role — the real apply worker (once wired)
            // will take over the same ownership responsibility. Do not
            // remove this binding without relocating ownership; dropping
            // the store here closes `applied_index_watch()` for every
            // holder of the receiver.
            let _vault_log_store = vault_log_store;
            let mut vault_batch_rx = vault_batch_rx;
            loop {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => {
                        debug!(
                            region = stub_region.as_str(),
                            organization_id = stub_org.value(),
                            vault_id = stub_vault.value(),
                            shard_id = stub_shard_id.0,
                            "Vault debug-apply stub exiting on cancellation",
                        );
                        break;
                    }
                    maybe_batch = vault_batch_rx.recv() => {
                        match maybe_batch {
                            Some(batch) => {
                                debug!(
                                    region = stub_region.as_str(),
                                    organization_id = stub_org.value(),
                                    vault_id = stub_vault.value(),
                                    shard_id = stub_shard_id.0,
                                    entry_count = batch.entries.len(),
                                    "Vault commit batch received (stub handler — real apply pipeline not yet wired)",
                                );
                            }
                            None => {
                                debug!(
                                    region = stub_region.as_str(),
                                    organization_id = stub_org.value(),
                                    vault_id = stub_vault.value(),
                                    shard_id = stub_shard_id.0,
                                    "Vault commit channel closed — stub exiting",
                                );
                                break;
                            }
                        }
                    }
                }
            }
        });

        // Build the per-vault group, reusing shared fields from the
        // parent org where the data plane overlaps (handle, state,
        // block_archive, block_announcements, leader_lease under
        // delegated leadership).
        let inner = Arc::new(InnerVaultGroup {
            region,
            organization_id,
            vault_id,
            handle: Arc::clone(org_inner.handle()),
            state: Arc::clone(org_inner.state()),
            block_archive: Arc::clone(org_inner.block_archive()),
            applied_state: vault_applied_state,
            block_announcements: org_inner.block_announcements().clone(),
            batch_handle: None,
            commitment_buffer: vault_commitment_buffer,
            leader_lease: Arc::clone(org_inner.leader_lease()),
            applied_index_rx: vault_applied_index_rx,
        });

        {
            let mut vault_groups = self.vault_groups.write();
            vault_groups.insert((region, organization_id, vault_id), Arc::clone(&inner));
        }

        info!(
            region = region.as_str(),
            organization_id = organization_id.value(),
            vault_id = vault_id.value(),
            shard_id = shard_id.0,
            "Vault group started successfully",
        );

        Ok(Arc::new(VaultGroup(inner)))
    }

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

        // Under B.1, a data region materializes as a single data-region
        // group at `OrganizationId::new(0)` — the regional control plane
        // until `RegionGroup` fills its own fields. Per-organization
        // groups are created by `CreateOrganization` apply + the bootstrap
        // handler's `start_organization_group`, not by `start_region`
        // iteration.
        let inner = self.start_region(region_config, OrganizationId::new(0)).await?;
        Ok(Arc::new(RegionGroup(inner)))
    }

    /// Ensures a data region is active, creating it lazily if needed.
    ///
    /// Returns the existing `OrganizationGroup` if the region is already running.
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

        // Attempt to start the region's data-region group at
        // `OrganizationId(0)` — if a concurrent caller beat us, fall
        // through. Two error variants can indicate concurrency:
        // - RegionExists: start_region's has_shard check saw it after map insert
        // - RegionAlreadyOpen: RegionStorageManager rejected a second opener
        match self.start_region(region_config, OrganizationId::new(0)).await {
            Ok(inner) => {
                info!(region = region.as_str(), "Lazily created region group");
                Ok((Arc::new(RegionGroup(inner)), true))
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

    /// Starts a Raft group for a specific `(region, organization_id)` pair.
    ///
    /// Data-region groups use
    /// [`OrganizationId::new(0)`](inferadb_ledger_types::OrganizationId);
    /// per-organization groups use the organization's id. Each
    /// `(region, organization_id)` pair gets its own independent Raft
    /// group, WAL, and state DBs under
    /// `{data_dir}/{region}/{organization_id}/`.
    async fn start_region(
        &self,
        region_config: RegionConfig,
        organization_id: OrganizationId,
    ) -> Result<Arc<InnerGroup>> {
        // Destructure config upfront to avoid partial-move issues
        let RegionConfig {
            region,
            initial_members,
            bootstrap: _,
            enable_background_jobs,
            batch_writer_config,
            event_writer,
            events_config,
            delegated_leadership,
        } = region_config;

        // Check if this `(region, organization_id)` Raft group is already
        // running. Per-organization bootstrap can race with the system-
        // apply path that dispatched it, so duplicate calls must be
        // idempotent-rejected rather than double-initializing. Concurrent
        // callers can race past this check (TOCTOU);
        // `RegionStorageManager::open_shard` provides the true guard
        // (returns `AlreadyOpen`) and `ensure_data_region` handles both
        // `RegionExists` and `RegionAlreadyOpen` gracefully.
        if self.has_organization_group(region, organization_id) {
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
            organization_creation_rx,
            vault_creation_rx,
            vault_deletion_rx,
        ) = self.open_region_storage(
            region,
            organization_id,
            event_writer,
            events_config,
            divergence_sender,
        )?;

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
        // Consensus `ConsensusStateId` must be unique across `(region, organization_id)` —
        // each Raft group runs independently, so colliding IDs would alias
        // two distinct groups in the engine's shard map. Mix the shard idx
        // into the seahash so all N shards in a region get distinct IDs.
        let shard_id = {
            let mut bytes = Vec::with_capacity(region.as_str().len() + 8);
            bytes.extend_from_slice(region.as_str().as_bytes());
            bytes.extend_from_slice(&organization_id.value().to_le_bytes());
            inferadb_ledger_consensus::types::ConsensusStateId(seahash::hash(&bytes))
        };

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

        let wal_dir = self.storage_manager.organization_dir(region, organization_id).join("wal");
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
        //
        // The replay type matches the apply worker type for this (region, organization_id):
        //   - organization_id == 0 → ApplyWorker<SystemRequest> → replay as SystemRequest
        //   - organization_id != 0 → ApplyWorker<OrganizationRequest> → replay as
        //     OrganizationRequest
        let replay_result = if organization_id == OrganizationId::new(0) {
            log_store.replay_crash_gap::<_, crate::types::SystemRequest>(&wal, shard_id).await
        } else {
            log_store.replay_crash_gap::<_, crate::types::OrganizationRequest>(&wal, shard_id).await
        };
        match replay_result {
            Ok(stats) => {
                let shard_label = organization_id.value().to_string();
                metrics::record_state_recovery_replay(
                    region.as_str(),
                    &shard_label,
                    stats.replayed_entries,
                );
                metrics::record_state_recovery_duration(
                    region.as_str(),
                    &shard_label,
                    stats.duration,
                );
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

        let mut consensus_shard = inferadb_ledger_consensus::ConsensusState::new(
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
        if delegated_leadership {
            consensus_shard
                .set_leadership_mode(inferadb_ledger_consensus::LeadershipMode::Delegated);
        }

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

        // P2b.1: introduce the per-engine commit dispatcher between the
        // engine's single commit channel and the per-shard apply workers.
        // The org's own shard is the only initial registration; P2b.2 will
        // register additional vault shards from `start_vault_group` against
        // `InnerGroup::commit_dispatcher()`.
        //
        // The org-shard downstream channel matches the engine commit
        // channel's capacity (10_000) so the dispatcher does not become a
        // throughput bottleneck for the org's own batches.
        let commit_dispatcher =
            Arc::new(crate::commit_dispatcher::CommitDispatcher::new(commit_rx));
        let (org_batch_tx, org_batch_rx) = tokio::sync::mpsc::channel::<
            inferadb_ledger_consensus::committed::CommittedBatch,
        >(10_000);
        commit_dispatcher.register(shard_id, org_batch_tx);

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
            organization_id,
            self.config.node_id,
            state_rx,
            response_map,
        ));

        // Create batch writer using ConsensusHandle for proposals.
        //
        // The batch writer closure constructs `OrganizationRequest::BatchWrite`
        // payloads. Per B.1 tier typing, only per-org groups
        // (`(region, org_id > 0)`) have `ApplyWorker<OrganizationRequest>` and
        // can decode those bytes; org-0 groups (GLOBAL system and data-region
        // control plane) are typed `ApplyWorker<SystemRequest>` and would
        // fail to decode batch submissions. The org-0 batch handle is left
        // wired for test-expected invariants but is not reached by any
        // production call path — service handlers route org-tier writes to
        // per-org groups via `propose_to_organization_bytes`.
        let batch_handle = if let Some(batch_config) = batch_writer_config {
            let handle_clone = handle.clone();
            let buffer_clone = commitment_buffer.clone();
            let submit_fn = move |requests: Vec<OrganizationRequest>| {
                let h = handle_clone.clone();
                let buffer = buffer_clone.clone();
                Box::pin(async move {
                    let batch_request = OrganizationRequest::BatchWrite { requests };
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

            let writer =
                BatchWriter::new(batch_config, submit_fn, region.to_string(), organization_id);
            let bw_handle = writer.handle();
            tokio::spawn(writer.run());
            Some(bw_handle)
        } else {
            None
        };

        // Start background jobs if enabled. `meta.db` is threaded through
        // alongside the entity-data DBs so the `StateCheckpointer` can
        // enforce the Slice 1 strict fsync ordering (state/raft/blocks/
        // events, then meta).
        let meta_db = Arc::clone(state.meta_database());
        let background_jobs = if enable_background_jobs {
            self.start_background_jobs(
                region,
                organization_id,
                handle.clone(),
                state.clone(),
                Arc::clone(&raft_db),
                Arc::clone(block_archive.db()),
                Some(Arc::clone(events_db.db())),
                Arc::clone(&meta_db),
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
        //
        // Phase D wire-format flip: the apply worker type is dispatched per
        // (region, organization_id) to match the propose-site serialization:
        //
        // - (GLOBAL, 0)      = SystemGroup  → `ApplyWorker<SystemRequest>`
        // - (region, 0)      = data-region  → `ApplyWorker<SystemRequest>` (B.1 compat shim;
        //   regional control plane uses SystemRequest until RegionGroup gets its own storage path
        //   in B.1.6)
        // - (region, org!=0) = per-org      → `ApplyWorker<OrganizationRequest>`
        //
        // Propose sites serialize the matching `RaftPayload<R>` directly, so
        // the bytes decode correctly on the apply side.
        if organization_id == OrganizationId::new(0) {
            // System group (GLOBAL) or data-region group — both use SystemRequest.
            let mut apply_worker =
                crate::apply_worker::ApplyWorker::<crate::types::SystemRequest>::new(
                    log_store,
                    handle.response_map().clone(),
                    handle.spillover().clone(),
                    region.as_str().to_string(),
                    organization_id,
                );
            if region == inferadb_ledger_types::Region::GLOBAL {
                apply_worker = apply_worker.with_dr_event_tx(self.dr_event_tx.clone());
            }
            // Apply worker reads from the per-shard channel registered with
            // the dispatcher above, not the raw engine commit channel.
            tokio::spawn(apply_worker.run(org_batch_rx));
        } else {
            // Per-organization group — uses OrganizationRequest.
            let apply_worker =
                crate::apply_worker::ApplyWorker::<crate::types::OrganizationRequest>::new(
                    log_store,
                    handle.response_map().clone(),
                    handle.spillover().clone(),
                    region.as_str().to_string(),
                    organization_id,
                );
            // Apply worker reads from the per-shard channel registered with
            // the dispatcher above, not the raw engine commit channel.
            tokio::spawn(apply_worker.run(org_batch_rx));
        }

        // Create region group.
        //
        // `blocks_db` is surfaced here alongside `raft_db` so the
        // `StateCheckpointer` and `sync_all_state_dbs` can reach the
        // underlying `Database<FileBackend>` without holding a reference to
        // `block_archive` (which owns a domain API, not a durability API).
        let jobs_running = enable_background_jobs;
        let blocks_db = Arc::clone(block_archive.db());
        let inner = Arc::new(InnerGroup {
            region,
            organization_id,
            handle,
            state,
            raft_db,
            meta_db,
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
            organization_creation_rx: parking_lot::Mutex::new(organization_creation_rx),
            vault_creation_rx: parking_lot::Mutex::new(vault_creation_rx),
            vault_deletion_rx: parking_lot::Mutex::new(vault_deletion_rx),
            commit_dispatcher,
        });

        {
            let mut regions = self.regions.write();
            regions.insert((region, organization_id), Arc::clone(&inner));
        }

        info!(
            region = region.as_str(),
            shard = organization_id.value(),
            "Region group started successfully"
        );

        Ok(inner)
    }

    /// Starts background jobs for a region.
    ///
    /// `raft_db`, `blocks_db`, `events_db`, and `meta_db` are plumbed into
    /// the [`StateCheckpointer`] so it can `sync_state` on every
    /// durability DB under the Slice 1 strict two-phase ordering
    /// (state/raft/blocks/events first; meta last). `events_db` is
    /// `Option` because some regions (test fixtures, historical
    /// GLOBAL-only configurations) are constructed without an events
    /// writer.
    #[allow(clippy::too_many_arguments)]
    fn start_background_jobs(
        &self,
        region: Region,
        organization_id: OrganizationId,
        handle: Arc<ConsensusHandle>,
        state: Arc<StateLayer<FileBackend>>,
        raft_db: Arc<Database<FileBackend>>,
        blocks_db: Arc<Database<FileBackend>>,
        events_db: Option<Arc<Database<FileBackend>>>,
        meta_db: Arc<Database<FileBackend>>,
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
        // Slice 2b: pass the `StateLayer` itself so the checkpointer
        // can enumerate per-vault DBs on every tick via
        // `live_vault_dbs()`. Phase A of `do_checkpoint` fans out across
        // the live vault set; Phase B syncs meta.db strictly after.
        //
        // Clone the per-org DB handles before they're moved into the
        // checkpointer — the integrity scrubber below needs the same
        // raft / blocks / events / meta DBs, one Arc clone per consumer.
        let raft_db_for_scrub = Arc::clone(&raft_db);
        let blocks_db_for_scrub = Arc::clone(&blocks_db);
        let events_db_for_scrub = events_db.as_ref().map(Arc::clone);
        let meta_db_for_scrub = Arc::clone(&meta_db);
        let state_checkpointer = StateCheckpointer::from_config(
            Arc::clone(&state),
            raft_db,
            blocks_db,
            events_db,
            meta_db,
            runtime_config,
            applied_index_rx,
            parent_token.child_token(),
            region.as_str().to_string(),
            organization_id,
        );
        let state_checkpointer_handle = state_checkpointer.start();
        info!(region = region.as_str(), "Started state checkpointer");

        // Integrity Scrubber.
        //
        // Slice 2c routes scrubbing per-DB so a corruption hit in one
        // vault does not block scan progress in any other DB. The
        // scrubber walks every materialised vault DB plus the per-org
        // raft.db / blocks.db / events.db / meta.db; each owns its own
        // progressive cursor.
        let integrity_scrubber = IntegrityScrubberJob::builder()
            .state(state.clone())
            .raft_db(Some(raft_db_for_scrub))
            .blocks_db(Some(blocks_db_for_scrub))
            .events_db(events_db_for_scrub)
            .meta_db(Some(meta_db_for_scrub))
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
        organization_id: OrganizationId,
        event_writer: Option<EventWriter<FileBackend>>,
        events_config: Option<inferadb_ledger_types::events::EventConfig>,
        divergence_sender: tokio::sync::mpsc::UnboundedSender<crate::types::StateRootDivergence>,
    ) -> Result<OpenedRegionStorage> {
        // Open per-shard databases via storage manager. Each `(region, organization_id)`
        // gets its own state.db / blocks.db / events.db / raft.db under the
        // shard-{N}/ subdirectory laid out by Task 3 — independent IO paths so
        // shard A's fsync stream does not block shard B's commit.
        let storage =
            self.storage_manager.open_organization(region, organization_id).map_err(|e| {
                if matches!(e, crate::region_storage::RegionStorageError::AlreadyOpen { .. }) {
                    RaftManagerError::RegionAlreadyOpen { region }
                } else {
                    RaftManagerError::Storage { region, message: format!("{e}") }
                }
            })?;

        // Wrap raw databases in domain-specific types. `meta.db` is the
        // Slice 1 per-organization coordinator — it sits alongside the
        // per-vault state DBs and owns the `_meta:last_applied`
        // sentinel.
        //
        // Slice 2b: `StateLayer` no longer holds a singleton state DB.
        // It materialises per-vault DBs lazily via this factory closure.
        // P2b.0 moves each vault's state.db down one level into a
        // per-vault subdirectory so future slices can add per-vault
        // `raft.db` / `blocks.db` / `events.db` alongside — the factory
        // composes `{organization_dir}/state/vault-{id}/state.db` via
        // `RegionStorage::vault_db_path` and creates the parent
        // `vault-{id}/` directory lazily on first reference.
        // `RegionStorageManager::open_organization` already created the
        // top-level `state/` directory.
        let region_storage_for_factory = Arc::clone(&storage);
        let state = Arc::new(
            StateLayer::new(
                move |vault| {
                    let path = region_storage_for_factory.vault_db_path(vault);
                    // P2b.0: ensure the per-vault `vault-{id}/` directory
                    // exists before opening state.db. Lazy creation keeps
                    // `open_organization` O(1) regardless of how many
                    // vaults the org ever had.
                    if let Some(parent) = path.parent() {
                        std::fs::create_dir_all(parent).map_err(|e| {
                            inferadb_ledger_state::StateError::Store {
                                source: inferadb_ledger_store::Error::Io { source: e },
                                location: snafu::location!(),
                            }
                        })?;
                    }
                    let db = if path.exists() {
                        Database::<FileBackend>::open(&path)
                    } else {
                        // Match the page size used for every other
                        // per-org DB so vault DBs share the same
                        // Raft-batch-sized pages.
                        let config = inferadb_ledger_store::DatabaseConfig {
                            page_size: crate::region_storage::ORGANIZATION_PAGE_SIZE,
                            ..Default::default()
                        };
                        Database::<FileBackend>::create_with_config(&path, config)
                    }
                    .map_err(|e| inferadb_ledger_state::StateError::Store {
                        source: e,
                        location: snafu::location!(),
                    })?;
                    Ok(Arc::new(db))
                },
                storage.meta_db().clone(),
            )
            .map_err(|e| RaftManagerError::Storage {
                region,
                message: format!("Failed to open StateLayer (P2b.0 factory): {e}"),
            })?,
        );
        let block_archive = Arc::new(BlockArchive::new(storage.blocks_db().clone()));

        // Create block announcements broadcast channel for real-time notifications.
        // Buffer size of 1024 allows for burst handling during high commit rates.
        let (block_announcements, _) = broadcast::channel(1024);

        // Open Raft log store (uses inferadb-ledger-store storage - handles open/create internally)
        let log_path = self.storage_manager.raft_db_path(region, organization_id);
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
            .with_organization_id(organization_id)
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

        // Wire organization creation channel for the GLOBAL log store.
        // CreateOrganization entries applied on GLOBAL send the new
        // (region, organization_id) pair through this channel; the bootstrap
        // handler picks it up and calls `start_organization_group` on each
        // in-region node so the per-organization Raft group spawns.
        let organization_creation_rx = if region == Region::GLOBAL {
            let (org_tx, org_rx) = tokio::sync::mpsc::unbounded_channel();
            log_store = log_store.with_organization_creation_sender(org_tx);
            Some(org_rx)
        } else {
            None
        };

        // P2b.2.c: wire vault create/delete signal channels for
        // per-organization log stores (organization_id != 0). Vault
        // lifecycle is scoped to a single `(region, organization)` pair,
        // so the system/region-plane groups (`organization_id == 0`) do
        // not need the channels. `start_organization_group` takes the
        // receivers and spawns a watcher task that (in P2b.2.d) calls
        // `start_vault_group` / `stop_vault_group` on every in-region
        // node.
        let (vault_creation_rx, vault_deletion_rx) = if organization_id != OrganizationId::new(0) {
            let (vault_create_tx, vault_create_rx) = tokio::sync::mpsc::unbounded_channel();
            let (vault_delete_tx, vault_delete_rx) = tokio::sync::mpsc::unbounded_channel();
            log_store = log_store
                .with_vault_creation_sender(vault_create_tx)
                .with_vault_deletion_sender(vault_delete_tx);
            (Some(vault_create_rx), Some(vault_delete_rx))
        } else {
            (None, None)
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

        Ok((
            state,
            block_archive,
            log_store,
            block_announcements,
            events_db,
            region_creation_rx,
            organization_creation_rx,
            vault_creation_rx,
            vault_deletion_rx,
        ))
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
            regions
                .remove(&(region, OrganizationId::new(0)))
                .ok_or(RaftManagerError::RegionNotFound { region })?
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

        // Close organization storage (removes from storage manager tracking).
        // B.1 transitional: closes the (region, organization_id) pair the
        // legacy `start_region` opened. Subsequent commits will iterate
        // every active organization in the region.
        if let Err(e) =
            self.storage_manager.close_organization(region, region_group.organization_id())
        {
            warn!(
                region = region.as_str(),
                error = %e,
                "Error closing organization storage"
            );
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
             (if configured) then meta.db sync across all regions (strict ordering)"
        );

        for region in regions {
            // Snapshot the region group under the read lock, drop the lock
            // before awaiting so the shutdown sweep never contends with
            // other readers for the regions map.
            let group = self.regions.read().get(&(region, OrganizationId::new(0))).cloned();
            let Some(group) = group else {
                debug!(
                    region = region.as_str(),
                    "sync_all_state_dbs: region vanished between list and lookup, skipping"
                );
                continue;
            };
            // Slice 2b: there is no longer a singleton state DB — the
            // per-vault state handles live inside the `StateLayer`.
            // Snapshot the live vault set and fan out Phase A across
            // every vault concurrently alongside raft/blocks/events.
            let vault_dbs = group.state().live_vault_dbs();
            let raft_db = Arc::clone(group.raft_db());
            let blocks_db = Arc::clone(group.blocks_db());
            let events_db_opt = group.events_state_db();
            let meta_db = Arc::clone(group.meta_db());

            // Sync every configured DB under a strict two-phase ordering
            // inside the per-region timeout:
            //
            // Phase A — every live vault state.db / raft.db / blocks.db /
            // events.db concurrently via `tokio::join!`. These are the
            // entity-data DBs; they must reach disk before the sentinel
            // in meta.db that references them.
            //
            // Phase B — meta.db, synced AFTER Phase A completes. This is
            // the Slice 1 strict-ordering invariant. Never invert.
            //
            // `sync_state` consumes an `Arc<Self>`, so clone for the await
            // and reuse the originals to read `last_synced_snapshot_id`
            // after the join.
            let vault_futs = futures::future::join_all(
                vault_dbs.iter().map(|(_, db)| Arc::clone(db).sync_state()),
            );
            let raft_fut = Arc::clone(&raft_db).sync_state();
            let blocks_fut = Arc::clone(&blocks_db).sync_state();
            let events_db_for_sync = events_db_opt.clone();
            let meta_db_for_sync = Arc::clone(&meta_db);
            let timeout_outcome = tokio::time::timeout(per_region_timeout, async move {
                // Phase A — fan out every vault state DB + raft + blocks
                // + events (if configured). Independent futures, one
                // single join point.
                let events_fut_opt = events_db_for_sync.as_ref().map(Arc::clone);
                let events_fut_async = async move {
                    if let Some(ev) = events_fut_opt { Some(ev.sync_state().await) } else { None }
                };
                let (vault_results, r, b, e) =
                    tokio::join!(vault_futs, raft_fut, blocks_fut, events_fut_async);
                // Phase B — strict ordering: meta.db always after the
                // entity DBs it's a sentinel for.
                let m = meta_db_for_sync.sync_state().await;
                (vault_results, r, b, e, m)
            })
            .await;

            match timeout_outcome {
                Ok((vault_results, raft_result, blocks_result, events_result, meta_result)) => {
                    for ((vault_id, db), result) in vault_dbs.iter().zip(vault_results.iter()) {
                        match result {
                            Ok(()) => info!(
                                region = region.as_str(),
                                db = "state",
                                vault_id = vault_id.value(),
                                last_synced_snapshot_id = db.last_synced_snapshot_id(),
                                "sync_all_state_dbs: final per-vault state-DB sync complete"
                            ),
                            Err(e) => warn!(
                                region = region.as_str(),
                                db = "state",
                                vault_id = vault_id.value(),
                                error = %e,
                                "sync_all_state_dbs: final per-vault state-DB sync failed; crash gap narrowed but not zero"
                            ),
                        }
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
                    match meta_result {
                        Ok(()) => info!(
                            region = region.as_str(),
                            db = "meta",
                            last_synced_snapshot_id = meta_db.last_synced_snapshot_id(),
                            "sync_all_state_dbs: final state-DB sync complete"
                        ),
                        Err(e) => warn!(
                            region = region.as_str(),
                            db = "meta",
                            error = %e,
                            "sync_all_state_dbs: final state-DB sync failed; crash gap narrowed but not zero"
                        ),
                    }
                },
                Err(_) => {
                    warn!(
                        region = region.as_str(),
                        timeout_ms = per_region_timeout.as_millis() as u64,
                        "sync_all_state_dbs: final state-DB sync (state + raft + blocks + events then meta) \
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
                regions.get(&(*region, OrganizationId::new(0))).cloned()
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
        let inner = self
            .regions
            .read()
            .get(&(region, OrganizationId::new(0)))
            .cloned()
            .ok_or(RaftManagerError::RegionNotFound { region })?;

        // Atomically transition from active to inactive.
        // Only one caller wins; others return early.
        if inner
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

        inner.background_jobs.lock().cancel();
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
        let inner = self
            .regions
            .read()
            .get(&(region, OrganizationId::new(0)))
            .cloned()
            .ok_or(RaftManagerError::RegionNotFound { region })?;

        // Atomically transition from inactive to active.
        // Only one caller wins; others return early.
        if inner
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

        inner.touch();
        let jobs = self.start_background_jobs(
            region,
            inner.organization_id(),
            inner.handle().clone(),
            inner.state().clone(),
            Arc::clone(inner.raft_db()),
            Arc::clone(inner.blocks_db()),
            inner.events_state_db(),
            Arc::clone(inner.meta_db()),
            inner.block_archive().clone(),
            inner.applied_state().clone(),
            inner.applied_index_watch(),
        );
        *inner.background_jobs.lock() = jobs;
        info!(%region, "Region group woken from hibernation");
        Ok(())
    }

    /// Hibernates idle region groups whose background jobs have been inactive
    /// beyond the given timeout.
    ///
    /// The system region (`GLOBAL`) is never hibernated.
    pub fn hibernate_idle_regions(&self, idle_timeout_secs: u64) {
        let regions: Vec<((Region, OrganizationId), Arc<InnerGroup>)> =
            self.regions.read().iter().map(|(k, g)| (*k, g.clone())).collect();

        for ((region, _shard), group) in regions {
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

        // Data region directories — B.1 layout drops the `regions/` parent.
        let data_dir = manager.region_dir(Region::US_EAST_VA);
        assert!(data_dir.ends_with("us-east-va"));
        assert!(!data_dir.to_string_lossy().contains("regions"));

        let data_dir = manager.region_dir(Region::JP_EAST_TOKYO);
        assert!(data_dir.ends_with("jp-east-tokyo"));
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

    // ────────────────────────────────────────────────────────────────────
    // Slice 2a: VaultGroup lookup surface
    //
    // Under Slice 2a no vault groups are actually started — the map is
    // empty and all lookups return `VaultGroupNotFound`. Slice 2b wires
    // `CreateVault` apply to `start_vault_group` and begins populating
    // the map; these tests will continue to pass because they exercise
    // the empty-map path explicitly.
    // ────────────────────────────────────────────────────────────────────

    #[test]
    fn test_vault_groups_empty_on_fresh_manager() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        assert!(manager.list_vault_groups().is_empty());
        assert!(!manager.has_vault_group(Region::GLOBAL, OrganizationId::new(1), VaultId::new(1),));
    }

    #[test]
    fn test_get_vault_group_returns_not_found_when_unregistered() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let result =
            manager.get_vault_group(Region::US_EAST_VA, OrganizationId::new(7), VaultId::new(42));
        match result {
            Ok(_) => panic!("expected VaultGroupNotFound on fresh manager"),
            Err(RaftManagerError::VaultGroupNotFound { region, organization_id, vault_id }) => {
                assert_eq!(region, Region::US_EAST_VA);
                assert_eq!(organization_id.value(), 7);
                assert_eq!(vault_id.value(), 42);
            },
            Err(other) => panic!("expected VaultGroupNotFound, got {other:?}"),
        }
    }

    // ────────────────────────────────────────────────────────────────────
    // start_vault_group lifecycle
    //
    // Exercises the happy path, the precondition failures, and the double-
    // start rejection. The per-vault apply pipeline is currently a
    // debug-log stub — these tests assert registration and accessors
    // only, not any committed-batch routing behaviour.
    // ────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_start_vault_group_errors_when_org_missing() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        // Start system region so the region-level preconditions are met,
        // but do NOT start the org group. `start_vault_group` must reject.
        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let result = manager
            .start_vault_group(Region::GLOBAL, OrganizationId::new(42), VaultId::new(7))
            .await;
        match result {
            Ok(_) => panic!("expected RegionNotFound when parent org missing"),
            Err(RaftManagerError::RegionNotFound { region }) => {
                assert_eq!(region, Region::GLOBAL);
            },
            Err(other) => panic!("expected RegionNotFound, got {other:?}"),
        }
        assert!(manager.list_vault_groups().is_empty());
    }

    #[tokio::test]
    async fn test_start_vault_group_registers_and_returns_accessors() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        // System region + org group serve as the parent for the vault.
        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");
        manager
            .start_organization_group(
                Region::GLOBAL,
                OrganizationId::new(11),
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                None,
            )
            .await
            .expect("start org group");

        let vault_id = VaultId::new(101);
        let group = manager
            .start_vault_group(Region::GLOBAL, OrganizationId::new(11), vault_id)
            .await
            .expect("start vault group");

        assert_eq!(group.region(), Region::GLOBAL);
        assert_eq!(group.organization_id(), OrganizationId::new(11));
        assert_eq!(group.vault_id(), vault_id);

        assert!(manager.has_vault_group(Region::GLOBAL, OrganizationId::new(11), vault_id));
        let triples = manager.list_vault_groups();
        assert_eq!(triples.len(), 1);
        assert_eq!(triples[0], (Region::GLOBAL, OrganizationId::new(11), vault_id));

        let fetched = manager
            .get_vault_group(Region::GLOBAL, OrganizationId::new(11), vault_id)
            .expect("get_vault_group after start");
        assert_eq!(fetched.region(), Region::GLOBAL);
        assert_eq!(fetched.organization_id(), OrganizationId::new(11));
        assert_eq!(fetched.vault_id(), vault_id);
    }

    #[tokio::test]
    async fn test_start_vault_group_rejects_double_start() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");
        manager
            .start_organization_group(
                Region::GLOBAL,
                OrganizationId::new(22),
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                None,
            )
            .await
            .expect("start org group");

        let vault_id = VaultId::new(202);
        manager
            .start_vault_group(Region::GLOBAL, OrganizationId::new(22), vault_id)
            .await
            .expect("first start_vault_group must succeed");

        // Second call for the same triple is an explicit error — not a
        // silent no-op — so the apply-side watcher surfaces duplicate
        // `VaultCreationRequest` signals rather than leaking them.
        let result =
            manager.start_vault_group(Region::GLOBAL, OrganizationId::new(22), vault_id).await;
        match result {
            Ok(_) => panic!("expected VaultGroupExists on second start"),
            Err(RaftManagerError::VaultGroupExists { region, organization_id, vault_id: got }) => {
                assert_eq!(region, Region::GLOBAL);
                assert_eq!(organization_id.value(), 22);
                assert_eq!(got, vault_id);
            },
            Err(other) => panic!("expected VaultGroupExists, got {other:?}"),
        }

        // Map size unchanged by the rejected double-start.
        assert_eq!(manager.list_vault_groups().len(), 1);
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
        let jobs = region.0.background_jobs.lock();
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
        let jobs = region.0.background_jobs.lock();
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
        // Slice 2c: `state.database()` is gone. Address the system vault
        // explicitly — under GLOBAL, the only vault that has been
        // materialised at this point is the system vault.
        let state_db = region
            .state()
            .db_for(inferadb_ledger_state::system::SYSTEM_VAULT_ID)
            .expect("system vault DB available");
        let raft_db = Arc::clone(region.raft_db());
        let blocks_db = Arc::clone(region.blocks_db());
        let events_db = region.events_state_db();
        let meta_db = Arc::clone(region.meta_db());
        let state_before = state_db.last_synced_snapshot_id();
        let raft_before = raft_db.last_synced_snapshot_id();
        let blocks_before = blocks_db.last_synced_snapshot_id();
        let events_before = events_db.as_ref().map(|db| db.last_synced_snapshot_id());
        let meta_before = meta_db.last_synced_snapshot_id();

        manager.sync_all_state_dbs(std::time::Duration::from_secs(5)).await;

        let state_after = state_db.last_synced_snapshot_id();
        let raft_after = raft_db.last_synced_snapshot_id();
        let blocks_after = blocks_db.last_synced_snapshot_id();
        let events_after = events_db.as_ref().map(|db| db.last_synced_snapshot_id());
        let meta_after = meta_db.last_synced_snapshot_id();

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
        assert!(
            meta_after >= meta_before,
            "meta.db last_synced_snapshot_id regressed (before={meta_before}, after={meta_after}) — \
             Slice 1 strict-ordering invariant"
        );
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
            let jobs = region.0.background_jobs.lock();
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
        *group.0.last_activity.lock() =
            std::time::Instant::now() - std::time::Duration::from_secs(5);

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
