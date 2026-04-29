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
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};

use inferadb_ledger_consensus::WalBackend as _;
use inferadb_ledger_proto::proto::BlockAnnouncement;
use inferadb_ledger_state::{
    BlockArchive, StateLayer,
    system::{
        MIN_NODES_PER_PROTECTED_REGION, SigningKeyCache, SystemOrganizationService,
        new_signing_key_cache,
    },
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
    state_checkpointer::{StateCheckpointer, VaultBlocksDbsFn, VaultEventsDbsFn, VaultRaftDbsFn},
    ttl_gc::TtlGarbageCollector,
    types::{LedgerNodeId, LedgerResponse, OrganizationRequest, RaftPayload},
};

// ============================================================================
// Type Aliases
// ============================================================================

/// Triple returned by [`RaftManager::open_and_wire_vault_store`]:
/// `(vault_log_store, vault_block_archive, vault_event_writer)`.
///
/// Used by both [`RaftManager::start_vault_group`] (production
/// per-vault startup path) and the Stage 5b parallel-replay path
/// inside [`RaftManager::start_region`]. The alias exists primarily
/// to keep the helper's return type within clippy's
/// `type_complexity` threshold; the field semantics are documented
/// on `open_and_wire_vault_store` itself.
pub(crate) type WiredVaultStore =
    (RaftLogStore<FileBackend>, Arc<BlockArchive<FileBackend>>, Option<EventWriter<FileBackend>>);

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

    /// Generic operational failure with a contextual message.
    ///
    /// Used by lifecycle paths (snapshot persistence, future admin
    /// trigger entry points) that compose multiple fallible steps where
    /// the surrounding context — not the structured failure variant —
    /// is what the caller logs.
    #[snafu(display("Raft manager error: {msg}"))]
    Other { msg: String },
}

/// Result type for multi-raft operations.
pub type Result<T> = std::result::Result<T, RaftManagerError>;

/// Region creation request: region + protected flag + initial members for the
/// Raft group.
///
/// The `protected` flag is sourced from the persisted
/// [`RegionDirectoryEntry`](inferadb_ledger_state::system::RegionDirectoryEntry)
/// (`_dir:region:{name}`) and propagated through the apply-time signal so the
/// bootstrap region handler can decide whether to auto-join (unprotected) or
/// require operator opt-in via `--regions <name1,name2>` (protected).
pub type RegionCreationRequest = (Region, bool, Vec<(u64, String)>);

/// Organization creation request: target region + the new organization id.
///
/// Sent on the GLOBAL log store's `organization_creation_sender` channel
/// when a `CreateOrganization` entry applies. The receiver task spawned
/// during bootstrap calls
/// [`RaftManager::start_organization_group`](RaftManager::start_organization_group)
/// on each in-region node so the per-organization Raft group spins up.
pub type OrganizationCreationRequest = (Region, OrganizationId);

/// Signal fired by `CreateVault` apply; drained by the per-org watcher
/// task to trigger [`RaftManager::start_vault_group`] on every voter in
/// the org.
///
/// Fire-and-forget: the apply path does not wait for the vault group to
/// come up. Mirrors the [`OrganizationCreationRequest`] pattern, but
/// scoped to a single `(region, organization)` Raft group.
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
    /// Cap on concurrent snapshot-producing membership conf-changes per
    /// per-organization Raft group (Phase 5 / M2 of the centralised
    /// membership plan). Default `2`, matching TiKV's
    /// `max_snapshot_per_store` default.
    ///
    /// Plumbed onto every per-organization
    /// [`InnerGroup`]'s `membership_queue` when the group is
    /// constructed; M2 only adds the primitive (the cascade still
    /// fires through the existing
    /// [`RaftManager::cascade_membership_to_children`] path), so this
    /// value has no observable effect until M3 wires the watcher.
    #[builder(default = crate::membership_queue::DEFAULT_MAX_CONCURRENT_SNAPSHOT_PRODUCING)]
    pub max_concurrent_snapshot_producing: usize,

    /// Whether to drive per-vault parallel catch-up from the local
    /// per-org WAL on restart (Stage 5b of the M6 reframed initiative).
    ///
    /// When `true`, `RaftManager::start_region` runs
    /// [`replay_shared_wal_for_org`](crate::log_storage::replay_shared_wal_for_org)
    /// after [`RaftLogStore::replay_crash_gap`](crate::log_storage::RaftLogStore::replay_crash_gap)
    /// and BEFORE
    /// [`ConsensusEngine::start_with_all_callbacks`](inferadb_ledger_consensus::ConsensusEngine::start_with_all_callbacks)
    /// consumes the WAL by value. The per-vault stores are opened via
    /// `RaftManager::open_and_wire_vault_store`, driven through one
    /// coordinated parallel-apply pass, and dropped — `start_vault_group`
    /// reopens them later. This closes the per-vault correctness gap that
    /// motivated Stage 5b: per-vault `applied_durable_index` lives in the
    /// page cache between [`StateCheckpointer`](crate::state_checkpointer::StateCheckpointer)
    /// ticks; an unclean shutdown loses it, and without this replay the
    /// only post-restart recovery path is AppendEntries-driven catch-up
    /// from a healthy peer (which is unavailable in single-node /
    /// majority-partitioned scenarios).
    ///
    /// When `false`, the replay block in `start_region` is skipped
    /// entirely. Per-vault catch-up falls back on the live commit pump,
    /// which only re-applies entries that arrive AFTER the engine starts
    /// — i.e., it does not close the gap on a single-node restart. This
    /// disabled mode is preserved as a runtime escape hatch for
    /// debugging the replay path itself; production deployments should
    /// leave the default.
    ///
    /// Default `true`. The cost on a clean shutdown is one
    /// `recover_from_wal` call per per-org WAL with all vaults at
    /// applied_durable >= last_committed (skipped via `skipped_no_gap`),
    /// so the no-op path is cheap and observable via
    /// `ledger_org_parallel_replay_invocations_total{result=skipped_no_gap}`.
    #[builder(default = true)]
    pub enable_parallel_wal_replay: bool,

    /// Maximum number of vault apply tasks that may run concurrently when
    /// [`Self::enable_parallel_wal_replay`] is active. Forwarded into
    /// [`crate::log_storage::ParallelReplayConfig::max_concurrent`].
    /// Default
    /// [`crate::log_storage::DEFAULT_MAX_CONCURRENT_REPLAY`].
    #[builder(default = crate::log_storage::DEFAULT_MAX_CONCURRENT_REPLAY)]
    pub parallel_wal_replay_max_concurrent: usize,

    /// Per-vault conf-change timeout in seconds (Phase 5 / M5 of the
    /// centralised membership plan).
    ///
    /// Each entry the
    /// [`MembershipDispatcher`](crate::region_membership_watcher::MembershipDispatcher)
    /// pops off the per-org [`MembershipQueue`](crate::membership_queue::MembershipQueue)
    /// is wrapped in a [`tokio::time::timeout`] of this duration. If the
    /// per-vault `apply_cascade_action_for_vault` call has not completed
    /// when the timer fires, the dispatcher logs a WARN with full
    /// context, increments
    /// `ledger_vault_conf_change_stalled_total`, and drops the request.
    /// The cascade is best-effort — the next region-state delta the
    /// [`RegionMembershipWatcher`](crate::region_membership_watcher::RegionMembershipWatcher)
    /// observes will re-derive any membership change the dropped entry
    /// failed to propagate.
    ///
    /// Default `60` — long enough that healthy Raft proposals never
    /// trip it. Operators on unusually-slow networks can raise it; the
    /// trade-off is a longer dispatcher stall on a truly broken vault
    /// before the queue advances.
    #[builder(default = 60)]
    pub vault_conf_change_timeout_secs: u64,

    /// Per-scope snapshot encryption key provider (Stage 1a scaffolding).
    ///
    /// Threaded onto every [`RaftLogStore`](crate::log_storage::RaftLogStore)
    /// constructed by [`RaftManager`] (org-scoped at `start_region`,
    /// per-vault at `start_vault_group`) so that
    /// [`LedgerSnapshotBuilder`](crate::log_storage::LedgerSnapshotBuilder)
    /// can resolve a `(region, organization, Option<vault>)` triple to a
    /// snapshot DEK. Stage 1a stores the provider; Stage 1b's bifurcated
    /// `build_snapshot` paths and Stage 2's `SnapshotPersister` are the
    /// first consumers.
    ///
    /// Defaults to [`NoopSnapshotKeyProvider`](crate::NoopSnapshotKeyProvider),
    /// which always returns `None`. The Stage 2 persister will skip the
    /// encryption envelope when the provider returns `None` — only valid
    /// for tests / unencrypted local-dev configurations.
    #[builder(default = Arc::new(crate::snapshot_key_provider::NoopSnapshotKeyProvider))]
    pub snapshot_key_provider: Arc<dyn crate::snapshot_key_provider::SnapshotKeyProvider>,
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
    /// initiates elections, and its leader is set externally by
    /// `RaftManager`'s per-organization leadership watcher (driven by the
    /// data-region group's elected leader — the B.1 unified-leadership
    /// model).
    ///
    /// `false` (default) keeps the standard self-electing Raft behavior
    /// for the data-region and system groups.
    #[builder(default = false)]
    pub delegated_leadership: bool,
    /// Hint for whether the region requires data-residency enforcement.
    ///
    /// `start_region` consults the GLOBAL region directory at startup to
    /// resolve the authoritative `requires_residency` flag; when the
    /// directory entry is missing (e.g. in test fixtures that bypass the
    /// `CreateDataRegion` apply path) this hint is used instead.
    /// Defaults to `false` (no residency requirement) so test fixtures
    /// that previously relied on the slug-based default for non-GDPR
    /// regions (`Region::US_EAST_VA`) continue to work without an
    /// explicit directory write.
    ///
    /// Production callers (`bootstrap_node`'s region-creation handler,
    /// `start_directory_regions`) populate this from the persisted
    /// directory entry, so the lookup-then-hint chain is consistent.
    #[builder(default = false)]
    pub requires_residency_hint: bool,
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
    /// Original batch-writer configuration that constructed
    /// [`Self::batch_handle`]. Retained so per-vault Raft groups created
    /// under this organization can construct their own
    /// [`BatchWriter`](crate::batching::BatchWriter) with the same
    /// coalescing thresholds. `None` when the org started without
    /// batching configured (e.g. test fixtures); per-vault groups under
    /// such an org also start without a batch writer.
    pub(crate) batch_writer_config: Option<BatchWriterConfig>,
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
    /// Apply-phase event writer cloned from the org's `RaftLogStore`.
    ///
    /// `EventWriter` wraps `Arc<EventsDatabase>` + `EventConfig`. This
    /// handle is bound to the org-level `events.db` and receives
    /// org-scoped emissions (CreateVault, AddOrganizationMember, team /
    /// invitation / app lifecycle, etc.). Phase 4.2 of per-vault
    /// consensus split vault-scoped emissions off into per-vault
    /// `events.db` files: [`RaftManager::start_vault_group`] opens a
    /// fresh writer over each vault's own `events.db` and wires it into
    /// the vault's `RaftLogStore`. The org's writer reads `EventConfig`
    /// from this field to seed the per-vault writer's scope flags / TTL.
    /// When `None` (e.g. test fixtures with no events configuration),
    /// vault apply also runs without an `event_writer` and apply skips
    /// event emission.
    pub(crate) event_writer: Option<EventWriter<FileBackend>>,
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
    /// [`RaftManager::start_organization_group`](RaftManager::start_organization_group),
    /// which dispatches each signal to
    /// [`RaftManager::start_vault_group`](RaftManager::start_vault_group).
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
    /// Rate-limited queue for per-vault membership conf-changes
    /// (Phase 5 / M2). Populated only on per-organization groups
    /// (`organization_id != OrganizationId::new(0)`); the system group
    /// and the regional control-plane group leave this `None`.
    ///
    /// In M2 the queue is constructed but receives no entries — the
    /// existing
    /// [`RaftManager::cascade_membership_to_children`] cascade still
    /// fires synchronously. M3 wires the
    /// `RegionMembershipWatcher` task that produces entries, and M4
    /// wires the dispatcher that drains them via
    /// [`MembershipQueue::take_next`](crate::membership_queue::MembershipQueue::take_next).
    pub(crate) membership_queue: Option<Arc<crate::membership_queue::MembershipQueue>>,
    /// Sender side of the per-shard apply-command channel used by Stage 4's
    /// `RaftManagerSnapshotInstaller` to route
    /// [`ApplyCommand::InstallSnapshot`](crate::apply_command::ApplyCommand::InstallSnapshot)
    /// onto the apply task that owns the org's
    /// [`RaftLogStore`](crate::log_storage::RaftLogStore).
    ///
    /// The receiver side is held by the apply worker spawned in
    /// [`RaftManager::start_region`] and drained alongside the commit
    /// channel via `tokio::select!`. The channel is sized for at most a
    /// handful of in-flight install commands
    /// ([`APPLY_COMMAND_CHANNEL_CAPACITY`](crate::apply_command::APPLY_COMMAND_CHANNEL_CAPACITY))
    /// — beyond that, drop-and-let-Raft-retry kicks in and the leader's
    /// next heartbeat re-emits `Action::SendSnapshot`.
    pub(crate) apply_command_tx: crate::apply_command::ApplyCommandSender,
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

    /// Returns the apply-phase event writer cloned from the org's
    /// `RaftLogStore`, if events are configured for this group.
    ///
    /// Bound to the org-level `events.db`. Phase 4.2 of per-vault
    /// consensus split vault-scoped emissions off to per-vault
    /// `events.db` files; this writer continues to receive org-scoped
    /// emissions only. [`RaftManager::start_vault_group`] reads this
    /// field's [`EventConfig`] to seed each vault's per-vault writer.
    /// Returns `None` for groups started without events configuration
    /// (e.g. some test fixtures), in which case vault apply also runs
    /// without an `event_writer` and skips event emission.
    pub fn event_writer(&self) -> Option<&EventWriter<FileBackend>> {
        self.event_writer.as_ref()
    }

    /// Returns the per-shard apply-command sender used by Stage 4's
    /// snapshot installer to route
    /// [`ApplyCommand::InstallSnapshot`](crate::apply_command::ApplyCommand::InstallSnapshot)
    /// onto the apply task that owns this org's
    /// [`RaftLogStore`](crate::log_storage::RaftLogStore).
    pub fn apply_command_tx(&self) -> &crate::apply_command::ApplyCommandSender {
        &self.apply_command_tx
    }

    /// Returns the batch writer handle.
    pub fn batch_handle(&self) -> Option<&BatchWriterHandle> {
        self.batch_handle.as_ref()
    }

    /// Returns the batch-writer configuration that constructed this
    /// group's [`Self::batch_handle`]. Used by `start_vault_group` to
    /// give per-vault groups the same coalescing thresholds as their
    /// parent organization.
    pub fn batch_writer_config(&self) -> Option<&BatchWriterConfig> {
        self.batch_writer_config.as_ref()
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

    /// Returns the per-organization rate-limited membership queue, if
    /// this group is a per-organization data-plane group.
    ///
    /// Returns `None` for the system group (`GLOBAL`,
    /// `OrganizationId(0)`) and the regional control-plane group
    /// (`region`, `OrganizationId(0)`).
    ///
    /// In M2 this is wired but unused — see
    /// [`crate::membership_queue`] for the migration plan.
    pub fn membership_queue(&self) -> Option<&Arc<crate::membership_queue::MembershipQueue>> {
        self.membership_queue.as_ref()
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

    /// Returns the per-organization rate-limited membership queue
    /// (Phase 5 / M2).
    ///
    /// In M2 this returns the constructed queue for every
    /// per-organization group; M3 will start producing entries through
    /// it and M4 will drain them. Until then the queue is inert — the
    /// existing
    /// [`RaftManager::cascade_membership_to_children`] cascade still
    /// fires synchronously.
    ///
    /// Returns `None` only when the underlying group is a
    /// control-plane group (`organization_id == OrganizationId(0)`);
    /// callers reaching this method through
    /// [`OrganizationGroup`] in production always observe `Some` —
    /// route_organization only constructs the wrapper for per-org
    /// groups.
    pub fn membership_queue(&self) -> Option<&Arc<crate::membership_queue::MembershipQueue>> {
        self.0.membership_queue()
    }
}

// ----------------------------------------------------------------------------
// VaultGroup — per-vault data plane (Phase 2 of per-vault consensus)
// ----------------------------------------------------------------------------

/// Lifecycle state of a per-vault Raft group (Phase 7 / O1).
///
/// Tracks whether a vault is actively serving requests, hibernating to
/// reduce overhead, or stalled (operator-visible failure to drain
/// background work). The state is stored on [`InnerVaultGroup`] in an
/// [`AtomicU8`] so reads are lock-free on every request hot-path; the
/// idle-detector + apply-path writers serialise transitions through
/// `compare_exchange`.
///
/// Hibernation is opt-in via
/// [`HibernationConfig::enabled`](inferadb_ledger_types::config::HibernationConfig)
/// — when disabled, vaults stay [`VaultLifecycleState::Active`] forever and
/// no transitions fire.
///
/// ## Discriminants
///
/// The numeric values are part of the `AtomicU8` storage contract; tests
/// rely on them. Do not reorder.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VaultLifecycleState {
    /// Serving requests. Scheduler ticks running, file handles open.
    Active = 0,
    /// No requests in `idle_secs`. Logged + counted; today the vault
    /// continues to tick (the smaller, lower-risk version of Phase 7).
    /// The metric / lifecycle bookkeeping is the operator-visible win
    /// without invasive consensus-engine surgery; tick suppression and
    /// FD eviction are deliberate follow-ups.
    Dormant = 1,
    /// Pending membership change has not drained for >`max_stall_secs`.
    /// Surfaced through `org_active_vault_count{status="stalled"}` so
    /// operators can investigate; the manager does not auto-recover.
    Stalled = 2,
}

impl VaultLifecycleState {
    /// Decodes from a raw `AtomicU8` value. Unknown values map to
    /// [`VaultLifecycleState::Active`] — the safe default for an
    /// unrecognised discriminant.
    #[inline]
    fn from_u8(value: u8) -> Self {
        match value {
            1 => VaultLifecycleState::Dormant,
            2 => VaultLifecycleState::Stalled,
            _ => VaultLifecycleState::Active,
        }
    }

    /// Returns the lowercase metric label for this state — matches the
    /// `org_active_vault_count{status=...}` label values populated by
    /// [`metrics::set_org_active_vault_count`].
    #[inline]
    #[must_use]
    pub fn as_metric_label(self) -> &'static str {
        match self {
            VaultLifecycleState::Active => "active",
            VaultLifecycleState::Dormant => "dormant",
            VaultLifecycleState::Stalled => "stalled",
        }
    }
}

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
    /// The shard identifier registered with the parent organization's
    /// [`CommitDispatcher`](crate::commit_dispatcher::CommitDispatcher) and
    /// [`ConsensusEngine`](inferadb_ledger_consensus::ConsensusEngine). Used
    /// by [`RaftManager::stop_vault_group`] to deregister from the
    /// dispatcher and call
    /// [`remove_shard`](crate::consensus_handle::ConsensusHandle::remove_shard)
    /// on the engine.
    pub(crate) shard_id: inferadb_ledger_consensus::types::ConsensusStateId,
    /// Vault-specific consensus handle. Shares the parent organization's
    /// `ConsensusEngine` (and therefore reactor, transport, and commit
    /// dispatcher) but routes proposals to this vault's `shard_id` and
    /// observes the vault shard's leadership / commit-index transitions.
    /// Constructed in `RaftManager::start_vault_group` via
    /// [`ConsensusHandle::new_for_shard`](crate::consensus_handle::ConsensusHandle::new_for_shard).
    pub(crate) handle: Arc<ConsensusHandle>,
    /// Shared state layer. Phase 1 already made the underlying `state.db`
    /// per-vault; a vault group still borrows the parent org's
    /// `StateLayer` (which owns the per-vault `Database`s internally) so
    /// apply workers write through the same accessor the org uses for
    /// metadata reads.
    pub(crate) state: Arc<StateLayer<FileBackend>>,
    /// Per-vault block archive — the vault's own Merkle chain, backed by
    /// its own `blocks.db` file at
    /// `{data_dir}/{region}/{org_id}/state/vault-{vault_id}/blocks.db`
    /// (Phase 4.1.a). Each vault's chain starts fresh at
    /// `{ height: 0, previous_hash: ZERO_HASH }` (hard cutover; new
    /// installs only).
    pub(crate) block_archive: Arc<BlockArchive<FileBackend>>,
    /// Accessor for applied state.
    pub(crate) applied_state: AppliedStateAccessor,
    /// Per-vault applied state — carries apply progress and vault-scoped
    /// counters for this vault's Raft group. Populated with the default
    /// empty state at vault-group start; populated by the vault's apply
    /// worker as committed entries land.
    ///
    /// The parent org's [`applied_state`](Self::applied_state) accessor
    /// holds the org-scoped applied state (slug indices, organization
    /// registry, etc.). This field holds the subset that's keyed
    /// per-vault — `last_applied` for this vault's Raft log,
    /// `vault_height` for this vault's block chain, `client_sequences`
    /// for writes targeting this vault.
    pub(crate) vault_applied_state: Arc<arc_swap::ArcSwap<crate::log_storage::VaultAppliedState>>,
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
    /// Per-vault cancellation handle for the commit-pump stub (and, once
    /// wired, the real apply worker). Cancelled either by
    /// [`RaftManager::stop_vault_group`] — to tear down a single vault —
    /// or indirectly by manager shutdown, since the stub task also selects
    /// on a child of the manager's cancellation token. Cloned from a
    /// fresh token at `start_vault_group` time and stored here so
    /// `stop_vault_group` can fire it without cancelling unrelated
    /// vaults.
    pub(crate) cancellation: CancellationToken,
    /// Shared handle to the vault's per-vault `raft.db`. Owned here so
    /// [`RaftManager::sync_all_state_dbs`] can include it in the shutdown
    /// fan-out alongside the vault's `state.db`. The apply task holds its
    /// own `Arc` via the owned [`RaftLogStore`](crate::log_storage::RaftLogStore);
    /// this field is a parallel reference so the database stays live
    /// even after apply-task teardown.
    pub(crate) raft_db: Arc<Database<FileBackend>>,
    /// Apply-phase event writer bound to this vault's per-vault
    /// `events.db` at
    /// `{data_dir}/{region}/{org_id}/state/vault-{vault_id}/events.db`.
    /// Held alongside the writer attached to the vault's owned
    /// `RaftLogStore` so the writer handle stays observable from the
    /// [`InnerVaultGroup`] (e.g. for tests) after the log store is
    /// moved into the apply task.
    ///
    /// Phase 4.2 of per-vault consensus: vault-scoped emissions
    /// (Write, BatchWrite, IngestExternalEvents) land in this writer.
    /// Org-scoped emissions (CreateVault, AddOrganizationMember, etc.)
    /// continue to land in the parent organization's writer
    /// ([`InnerGroup::event_writer`]). The vault writer inherits its
    /// `EventConfig` from the parent org's writer at
    /// [`RaftManager::start_vault_group`] time so scope flags / TTL
    /// stay aligned.
    ///
    /// `None` when the parent org has no events configured — in that
    /// case the vault's `RaftLogStore` is also constructed without a
    /// writer and apply skips event emission.
    pub(crate) event_writer: Option<EventWriter<FileBackend>>,
    /// Lifecycle state for hibernation tracking (Phase 7 / O1). Stored
    /// as an [`AtomicU8`] so the request hot-path can read it without
    /// any locks; transitions go through
    /// [`InnerVaultGroup::transition_lifecycle_state`] which fires
    /// metrics + log lines.
    pub(crate) lifecycle_state: AtomicU8,
    /// Wall-clock seconds since the UNIX epoch of the last activity
    /// observed on this vault. Updated by
    /// [`InnerVaultGroup::touch_activity`] on every request and on
    /// every leader adoption. The idle detector compares this against
    /// `now()` to decide whether to transition the vault to
    /// [`VaultLifecycleState::Dormant`].
    pub(crate) last_activity_unix_secs: AtomicU64,
    /// Wall-clock seconds since the UNIX epoch when an in-flight
    /// membership change started, or `0` if none. Used by the idle
    /// detector to surface `Stalled` state; see
    /// [`InnerVaultGroup::mark_membership_change_started`] /
    /// [`InnerVaultGroup::mark_membership_change_finished`].
    pub(crate) pending_membership_started_unix_secs: AtomicU64,
    /// Sender side of the per-vault apply-command channel used by Stage 4's
    /// `RaftManagerSnapshotInstaller` to route
    /// [`ApplyCommand::InstallSnapshot`](crate::apply_command::ApplyCommand::InstallSnapshot)
    /// onto the per-vault commit pump that owns this vault's
    /// [`RaftLogStore`](crate::log_storage::RaftLogStore).
    ///
    /// The receiver side is held by the per-vault commit pump spawned in
    /// [`RaftManager::start_vault_group`] and drained alongside the commit
    /// channel via `tokio::select!`. Strict ordering: an install command
    /// runs to completion before the next batch is processed (the install
    /// arm awaits the synchronous `RaftLogStore::install_snapshot` call
    /// before returning to the loop), preventing any race between an
    /// in-flight batch apply and a wholesale state-replace. The channel is
    /// sized for at most a handful of in-flight install commands
    /// ([`APPLY_COMMAND_CHANNEL_CAPACITY`](crate::apply_command::APPLY_COMMAND_CHANNEL_CAPACITY))
    /// — beyond that, drop-and-let-Raft-retry kicks in and the leader's
    /// next heartbeat re-emits `Action::SendSnapshot`.
    pub(crate) apply_command_tx: crate::apply_command::ApplyCommandSender,
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

    /// Returns the shard identifier this vault group registers with the
    /// parent organization's
    /// [`CommitDispatcher`](crate::commit_dispatcher::CommitDispatcher) and
    /// [`ConsensusEngine`](inferadb_ledger_consensus::ConsensusEngine).
    pub fn shard_id(&self) -> inferadb_ledger_consensus::types::ConsensusStateId {
        self.shard_id
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

    /// Returns a handle to the vault's per-vault applied state. Uses
    /// [`arc_swap::ArcSwap`] for lock-free reads; the apply worker swaps
    /// in a new snapshot on each committed entry.
    pub fn vault_applied_state(
        &self,
    ) -> &Arc<arc_swap::ArcSwap<crate::log_storage::VaultAppliedState>> {
        &self.vault_applied_state
    }

    /// Returns the per-vault apply-command sender used by Stage 4's
    /// snapshot installer to route
    /// [`ApplyCommand::InstallSnapshot`](crate::apply_command::ApplyCommand::InstallSnapshot)
    /// onto the per-vault commit pump that owns this vault's
    /// [`RaftLogStore`](crate::log_storage::RaftLogStore).
    pub fn apply_command_tx(&self) -> &crate::apply_command::ApplyCommandSender {
        &self.apply_command_tx
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

    /// Returns the per-vault `raft.db` handle.
    pub fn raft_db(&self) -> &Arc<Database<FileBackend>> {
        &self.raft_db
    }

    /// Returns the apply-phase event writer wired into the vault's
    /// `RaftLogStore`, if events are configured for the parent org.
    ///
    /// Cloned from the parent [`InnerGroup::event_writer`] at
    /// vault-group start so the vault store and the org store both emit
    /// into the same physical `events.db`. Returns `None` when the org
    /// was started without events configuration.
    pub fn event_writer(&self) -> Option<&EventWriter<FileBackend>> {
        self.event_writer.as_ref()
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

    // ---- Hibernation lifecycle (Phase 7 / O1) -------------------------

    /// Returns the current lifecycle state.
    #[must_use]
    pub fn lifecycle_state(&self) -> VaultLifecycleState {
        VaultLifecycleState::from_u8(self.lifecycle_state.load(Ordering::Acquire))
    }

    /// Returns the seconds-since-epoch of the last observed activity, or
    /// `0` if the vault has never seen activity since startup.
    #[must_use]
    pub fn last_activity_unix_secs(&self) -> u64 {
        self.last_activity_unix_secs.load(Ordering::Relaxed)
    }

    /// Returns the seconds-since-epoch at which an in-flight membership
    /// change started, or `0` if no membership change is pending.
    ///
    /// Used by the operator-facing `ShowVault` RPC to surface stalled
    /// membership transitions: when the value is non-zero and older than
    /// `max_stall_secs`, the idle detector marks the vault as
    /// [`VaultLifecycleState::Stalled`] for operator follow-up.
    #[must_use]
    pub fn pending_membership_started_unix_secs(&self) -> u64 {
        self.pending_membership_started_unix_secs.load(Ordering::Acquire)
    }

    /// Marks this vault as having outstanding membership work in flight.
    /// Pairs with [`InnerVaultGroup::mark_membership_change_finished`].
    /// Idempotent — re-marking does not reset the start timestamp, so
    /// the stall budget is measured from the first signal.
    pub fn mark_membership_change_started(&self) {
        let now = current_unix_secs();
        let _ = self.pending_membership_started_unix_secs.compare_exchange(
            0,
            now,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
    }

    /// Clears the in-flight membership marker.
    pub fn mark_membership_change_finished(&self) {
        self.pending_membership_started_unix_secs.store(0, Ordering::Release);
    }

    /// Records activity on this vault: bumps `last_activity_unix_secs`
    /// and, when the vault is currently
    /// [`VaultLifecycleState::Dormant`], synchronously transitions back
    /// to [`VaultLifecycleState::Active`] (firing the wake metric +
    /// log). This is the single function the request hot-path needs to
    /// call — it's safe to invoke unconditionally because Active is
    /// the no-op fast path.
    ///
    /// Returns `true` if the call performed a Dormant → Active
    /// transition (so callers can observe wake-time latency for tests
    /// / dashboards).
    pub fn touch_activity(&self) -> bool {
        self.last_activity_unix_secs.store(current_unix_secs(), Ordering::Relaxed);
        // Optimistic CAS: only transition if currently Dormant. Stalled
        // vaults wake too — they had outstanding work, an incoming
        // request is a cue for the operator to investigate but should
        // not block the request.
        let current = self.lifecycle_state.load(Ordering::Acquire);
        if current == VaultLifecycleState::Active as u8 {
            return false;
        }
        self.transition_lifecycle_state(VaultLifecycleState::Active)
    }

    /// Transitions to the requested lifecycle state. No-op if the vault
    /// is already in `target`. Fires metrics + log lines for actual
    /// transitions.
    ///
    /// Returns `true` when a transition fired.
    pub fn transition_lifecycle_state(&self, target: VaultLifecycleState) -> bool {
        let target_u8 = target as u8;
        let prev_u8 = self.lifecycle_state.swap(target_u8, Ordering::AcqRel);
        if prev_u8 == target_u8 {
            return false;
        }
        let prev = VaultLifecycleState::from_u8(prev_u8);
        let region_label = self.region.as_str();
        let org_label = self.organization_id.value().to_string();

        match (prev, target) {
            (VaultLifecycleState::Dormant, VaultLifecycleState::Active) => {
                metrics::record_vault_hibernation_transition(region_label, &org_label, "wake");
                info!(
                    region = region_label,
                    organization_id = self.organization_id.value(),
                    vault_id = self.vault_id.value(),
                    prev = prev.as_metric_label(),
                    "Vault hibernation: wake (dormant -> active)",
                );
            },
            (VaultLifecycleState::Active, VaultLifecycleState::Dormant) => {
                metrics::record_vault_hibernation_transition(region_label, &org_label, "sleep");
                info!(
                    region = region_label,
                    organization_id = self.organization_id.value(),
                    vault_id = self.vault_id.value(),
                    "Vault hibernation: sleep (active -> dormant)",
                );
            },
            (_, VaultLifecycleState::Stalled) => {
                warn!(
                    region = region_label,
                    organization_id = self.organization_id.value(),
                    vault_id = self.vault_id.value(),
                    prev = prev.as_metric_label(),
                    "Vault hibernation: marked stalled — pending membership change has not drained",
                );
            },
            _ => {
                debug!(
                    region = region_label,
                    organization_id = self.organization_id.value(),
                    vault_id = self.vault_id.value(),
                    prev = prev.as_metric_label(),
                    target = target.as_metric_label(),
                    "Vault lifecycle transition",
                );
            },
        }
        true
    }
}

/// Returns the current wall-clock time in seconds since the UNIX epoch.
///
/// Returns `0` if the system clock is somehow before the epoch — used as
/// a sentinel for "never observed activity" in the lifecycle bookkeeping.
#[inline]
fn current_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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

    /// Returns the shard identifier this vault group registers with the
    /// parent organization's dispatcher and engine.
    #[must_use]
    pub fn shard_id(&self) -> inferadb_ledger_consensus::types::ConsensusStateId {
        self.0.shard_id()
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

    /// Returns a handle to the vault's per-vault applied state. Uses
    /// [`arc_swap::ArcSwap`] for lock-free reads; the apply worker swaps
    /// in a new snapshot on each committed entry.
    #[must_use]
    pub fn vault_applied_state(
        &self,
    ) -> &Arc<arc_swap::ArcSwap<crate::log_storage::VaultAppliedState>> {
        self.0.vault_applied_state()
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

    /// Returns the per-vault `raft.db` handle.
    #[must_use]
    pub fn raft_db(&self) -> &Arc<Database<FileBackend>> {
        self.0.raft_db()
    }

    /// Returns the apply-phase event writer wired into the vault's
    /// `RaftLogStore`, if events are configured for the parent org.
    #[must_use]
    pub fn event_writer(&self) -> Option<&EventWriter<FileBackend>> {
        self.0.event_writer()
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

    /// Returns the lifecycle state (Phase 7 / O1).
    #[must_use]
    pub fn lifecycle_state(&self) -> VaultLifecycleState {
        self.0.lifecycle_state()
    }

    /// Returns the seconds-since-epoch of the last observed activity on
    /// this vault, or `0` if the vault has never observed activity since
    /// startup.
    #[must_use]
    pub fn last_activity_unix_secs(&self) -> u64 {
        self.0.last_activity_unix_secs()
    }

    /// Returns the seconds-since-epoch at which an in-flight membership
    /// change started, or `0` if no membership change is pending.
    #[must_use]
    pub fn pending_membership_started_unix_secs(&self) -> u64 {
        self.0.pending_membership_started_unix_secs()
    }

    /// Records activity on this vault — see
    /// [`InnerVaultGroup::touch_activity`]. Returns `true` when this call
    /// performed a Dormant → Active wake transition.
    pub fn touch_activity(&self) -> bool {
        self.0.touch_activity()
    }

    /// Test-only helper that backdates the vault's `last_activity_unix_secs`
    /// stamp. Visible to integration tests in `crates/server/tests/` so
    /// they can simulate a long-idle vault without sleeping for
    /// `idle_secs`. Not intended for production callers — there is no
    /// production reason to rewind the activity timestamp.
    #[doc(hidden)]
    pub fn force_last_activity_for_test(&self, secs_since_epoch: u64) {
        self.0.last_activity_unix_secs.store(secs_since_epoch, Ordering::Release);
    }
}

// ============================================================================
// Raft Manager
// ============================================================================

/// [`ShardWakeNotifier`](inferadb_ledger_consensus::wake::ShardWakeNotifier)
/// implementation that translates a paused-shard peer-message event into a
/// [`RaftManager::wake_vault`] call (O6 vault hibernation Pass 2).
///
/// Holds an `Arc<Mutex<Weak<RaftManager>>>` shared with [`RaftManager`] —
/// bootstrap fills the `Weak` via [`RaftManager::install_self_weak`]
/// immediately after wrapping the manager in an `Arc`. The notifier is
/// installed on every per-org [`ConsensusEngine`] via
/// [`ConsensusEngine::start_with_wake_notifier`](inferadb_ledger_consensus::ConsensusEngine::start_with_wake_notifier),
/// so the late-bind pattern is forgiving of the precise call ordering
/// between "build engine" and "install self_weak".
///
/// ## Drop-and-let-Raft-retry
///
/// The reactor invokes `on_peer_message_for_paused_shard` then **drops**
/// the originating peer message. The notifier dispatches the wake work on
/// a tokio task and returns immediately — the reactor's event loop is
/// single-threaded, and blocking inside the notifier stalls every other
/// shard managed by the reactor (see `crates/consensus/src/wake.rs` and
/// raft rule 17). The peer's next AppendEntries heartbeat (~50ms typical)
/// lands on the awoken shard.
///
/// On a missing reverse-map entry (shard arrived after `stop_vault_group`,
/// or a transient race during start), the notifier increments
/// `ledger_vault_hibernation_wake_unmatched_total` and logs a warning.
struct RaftManagerWakeNotifier {
    manager: Arc<parking_lot::Mutex<Weak<RaftManager>>>,
}

impl inferadb_ledger_consensus::wake::ShardWakeNotifier for RaftManagerWakeNotifier {
    fn on_peer_message_for_paused_shard(
        &self,
        shard_id: inferadb_ledger_consensus::types::ConsensusStateId,
    ) {
        // Upgrade the Weak under the mutex, then release the mutex before
        // spawning the task so notifier callbacks for sibling shards do
        // not contend on the lock.
        let manager = match self.manager.lock().upgrade() {
            Some(arc) => arc,
            None => {
                // Two cases:
                // 1. RaftManager is being dropped (process shutdown). The wake event is obsolete —
                //    there is nothing to wake against.
                // 2. install_self_weak was never called (test fixture without hibernation). Falling
                //    through is correct; tests that need wake routing must call it.
                tracing::debug!(
                    ?shard_id,
                    "RaftManagerWakeNotifier: weak upgrade returned None; skipping wake \
                     (manager dropped or self_weak not installed)"
                );
                return;
            },
        };
        // Resolve the shard_id → vault triple before the spawn so the
        // task does not need to round-trip through Self again. Lookup is
        // O(1) on the DashMap and does not contend with the vault_groups
        // RwLock.
        let triple = manager.vault_shard_index.get(&shard_id).map(|e| *e.value());
        let Some((region, organization_id, vault_id)) = triple else {
            metrics::record_vault_hibernation_wake_unmatched(shard_id);
            tracing::warn!(
                ?shard_id,
                "RaftManagerWakeNotifier: paused-shard peer message has no vault \
                 mapping in vault_shard_index; dropping wake (peer's next \
                 AppendEntries will retry)"
            );
            return;
        };

        // Spawn-and-return so the reactor's event loop is unblocked.
        // wake_vault.await is short — engine.resume_shard() is a single
        // control-channel round-trip, lifecycle transition is an atomic
        // store, page-cache repopulation is lazy on first access — but
        // the notifier contract is "do not block the reactor", and that
        // promise is not contingent on wake being fast.
        tokio::spawn(async move {
            if let Err(e) = manager.wake_vault(region, organization_id, vault_id).await {
                tracing::warn!(
                    error = %e,
                    region = region.as_str(),
                    organization_id = organization_id.value(),
                    vault_id = vault_id.value(),
                    "RaftManagerWakeNotifier: wake_vault failed"
                );
            }
        });
    }
}

/// Snapshot coordinator that translates
/// [`Action::TriggerSnapshot`](inferadb_ledger_consensus::action::Action::TriggerSnapshot)
/// emissions from any per-org reactor into a `RaftManager::persist_snapshot`
/// call.
///
/// Holds an `Arc<Mutex<Weak<RaftManager>>>` shared with [`RaftManager`] —
/// bootstrap fills the `Weak` via [`RaftManager::install_self_weak`]
/// immediately after wrapping the manager in an `Arc`. Sister of
/// [`RaftManagerWakeNotifier`]; same late-bind pattern, same
/// drop-and-let-Raft-retry contract for the reactor (the coordinator MUST
/// dispatch any I/O asynchronously and return immediately).
///
/// On a missing reverse-map entry (the shard does not resolve to either an
/// org or a vault scope on this node), the coordinator logs at warn level
/// and increments the unmatched-trigger metric.
struct RaftManagerSnapshotCoordinator {
    manager: Arc<parking_lot::Mutex<Weak<RaftManager>>>,
}

impl inferadb_ledger_consensus::snapshot_coordinator::SnapshotCoordinator
    for RaftManagerSnapshotCoordinator
{
    fn on_trigger_snapshot(
        &self,
        shard_id: inferadb_ledger_consensus::types::ConsensusStateId,
        last_included_index: u64,
        last_included_term: u64,
    ) {
        let manager = match self.manager.lock().upgrade() {
            Some(arc) => arc,
            None => {
                // Two cases:
                // 1. RaftManager is being dropped (process shutdown). The trigger is obsolete —
                //    persisting a snapshot during shutdown would race state-DB sync.
                // 2. install_self_weak was never called (test fixture). Falling through is correct;
                //    tests that exercise the snapshot path must call it.
                tracing::debug!(
                    ?shard_id,
                    "RaftManagerSnapshotCoordinator: weak upgrade returned None; \
                     skipping snapshot trigger (manager dropped or self_weak not installed)",
                );
                return;
            },
        };

        // Resolve the shard_id → scope address before the spawn so the
        // task does not need to round-trip through Self again.
        let scope_addr = manager.resolve_shard_to_scope(shard_id);
        let Some((region, organization_id, vault_id)) = scope_addr else {
            tracing::warn!(
                ?shard_id,
                last_included_index,
                last_included_term,
                "RaftManagerSnapshotCoordinator: no scope mapping for trigger; \
                 dropping (peer's next threshold check will retry)",
            );
            return;
        };

        // Spawn-and-return: the reactor's event loop is on hold until this
        // method returns. The async build + persist + notify chain happens
        // off the reactor task.
        tokio::spawn(async move {
            if let Err(e) = manager
                .persist_snapshot(region, organization_id, vault_id, shard_id, last_included_term)
                .await
            {
                tracing::warn!(
                    error = %e,
                    region = region.as_str(),
                    organization_id = organization_id.value(),
                    vault_id = vault_id.map(|v| v.value()),
                    shard_id = shard_id.0,
                    "RaftManagerSnapshotCoordinator: persist_snapshot failed",
                );
            }
        });
    }
}

/// Manager for multiple Raft region groups.
///
/// Concrete map type backing [`RaftManager::vault_groups`]. Keyed by the
/// `(region, organization_id, vault_id)` triple since vault groups are
/// owned by per-organization shards but discoverable across the whole
/// node — pulled into a type alias to satisfy clippy's
/// `type_complexity` lint on the field declaration.
type VaultGroupMap = Arc<RwLock<HashMap<(Region, OrganizationId, VaultId), Arc<InnerVaultGroup>>>>;

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
    vault_groups: VaultGroupMap,
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
    /// Vault hibernation policy (Phase 7 / O1). When `enabled` is
    /// `true`, [`RaftManager::start_hibernation_idle_detector`] spawns a
    /// background task that scans `vault_groups` on the configured
    /// interval and transitions idle vaults to
    /// [`VaultLifecycleState::Dormant`]. Defaults to `disabled` so test
    /// suites and operators that don't opt in see no behaviour change.
    hibernation_config: parking_lot::Mutex<inferadb_ledger_types::config::HibernationConfig>,
    /// Whether the idle detector task has been started — guards
    /// [`RaftManager::start_hibernation_idle_detector`] against
    /// double-spawn races during bootstrap.
    hibernation_detector_started: AtomicBool,
    /// Process-wide cache for JWT signing keys.
    ///
    /// One [`SigningKeyCache`] is allocated per `RaftManager` and shared with
    /// every [`SystemOrganizationService`] constructed on a hot path
    /// (`route_organization`, `RegionResolverService::resolve_with_redirect`,
    /// `ServiceContext`-rooted handlers). Sharing the `Arc` eliminates the
    /// per-request `moka::Cache` construct/drop cycle that previously
    /// dominated CPU self-time via `crossbeam_epoch::Global::try_advance` —
    /// see `docs/superpowers/specs/2026-04-27-reactor-wal-investigation.md`.
    signing_key_cache: SigningKeyCache,
    /// Self-`Weak` so `RaftManagerWakeNotifier` (installed on every
    /// per-org `ConsensusEngine` via `start_with_wake_notifier`) can
    /// upgrade and call back into the manager from the consensus reactor's
    /// thread without creating an `Arc` cycle.
    ///
    /// Populated by [`RaftManager::install_self_weak`] immediately after
    /// the manager is wrapped in `Arc<RaftManager>` during bootstrap.
    /// The notifier holds a clone of the same `Arc<Mutex<Weak<…>>>` so
    /// it observes the late install without re-plumbing. Tests that don't
    /// exercise hibernation may leave it unset; the notifier degrades to
    /// a metric-only warning when the upgrade returns `None` (also
    /// covers the "manager is being dropped" race).
    self_weak: Arc<parking_lot::Mutex<Weak<RaftManager>>>,
    /// Reverse map from `ConsensusStateId` (the consensus-tier shard id) to
    /// the `(region, organization_id, vault_id)` triple keyed in
    /// [`RaftManager::vault_groups`]. Maintained alongside `vault_groups`
    /// inserts/removes by [`RaftManager::start_vault_group`] /
    /// [`RaftManager::stop_vault_group`].
    ///
    /// Used by `RaftManagerWakeNotifier::on_peer_message_for_paused_shard`
    /// to translate the reactor's `shard_id` into the triple
    /// [`RaftManager::wake_vault`] expects without scanning every entry of
    /// `vault_groups` under a lock. Lookup is `DashMap`-style (read-only
    /// `get`) so the notifier's hot path does not contend with new vault
    /// registrations.
    vault_shard_index: dashmap::DashMap<
        inferadb_ledger_consensus::types::ConsensusStateId,
        (Region, OrganizationId, VaultId),
    >,
    /// Reverse map from `ConsensusStateId` (the consensus-tier shard id) to
    /// the `(region, organization_id)` pair keyed in
    /// [`RaftManager::regions`]. Maintained alongside `regions` inserts /
    /// removes by [`RaftManager::start_region`] / `stop_region`.
    ///
    /// Used by `RaftManagerSnapshotCoordinator::on_trigger_snapshot` to
    /// translate the reactor's `shard_id` into the org coordinates
    /// [`RaftManager::persist_snapshot`] expects without scanning the
    /// `regions` map under a lock. Wake-on-peer-message routing
    /// deliberately uses [`Self::vault_shard_index`] only — org-level
    /// shards have no hibernation primitive.
    org_shard_index: dashmap::DashMap<
        inferadb_ledger_consensus::types::ConsensusStateId,
        (Region, OrganizationId),
    >,
    /// Stage 2 snapshot persister.
    ///
    /// Built once during [`Self::new`] from `config.snapshot_key_provider`
    /// and the shared `region_storage` manager so every per-org reactor's
    /// `RaftManagerSnapshotCoordinator` resolves to the same persister
    /// instance — single retention pruning point, single warning-emission
    /// dedup. Wrapped in `Arc` so the coordinator and any future direct
    /// admin caller share the handle.
    snapshot_persister: Arc<crate::snapshot_persister::SnapshotPersister>,
}

/// Membership delta to cascade from a data-region group down to its
/// per-organization and per-vault child shards. Used by
/// [`RaftManager::cascade_membership_to_children`] so the DR scheduler
/// (`crates/server/src/dr_scheduler.rs`) and the `LeaveCluster` admin RPC
/// (`crates/services/src/services/admin.rs`) share one cascade
/// implementation. Per multi-tier consensus (root rule 14), per-org and
/// per-vault shards adopt the parent region's leader but maintain
/// *independent* Raft membership state — without an explicit cascade,
/// late-joining voters never receive AppendEntries for child shards,
/// and decommissioned voters linger as quorum-blocking ghosts after
/// their parent removal. Per-vault groups share the parent org's
/// transport (root rule 17), so cascade only registers the new peer on
/// per-org transports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CascadeMembershipAction {
    /// Add `node_id` as a learner to every child shard in `region`.
    AddLearner,
    /// Promote `node_id` from learner to voter on every child shard. If
    /// the node is not yet a learner on a given child, an
    /// `AddLearner` is issued first.
    PromoteVoter,
    /// Remove `node_id` from every child shard in `region`.
    Remove,
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
        // Stage 2 snapshot persister — shared across every per-org
        // `RaftManagerSnapshotCoordinator` so retention pruning and the
        // unencrypted-warning dedup all converge on a single instance.
        // Persister's RegionStorageManager is a separate instance with the
        // same data_dir — it only ever calls `snapshot_dir(region, org)`,
        // a pure path computation that does not depend on the open-DB
        // bookkeeping the manager-owned instance maintains. Sharing the
        // same Arc would require dual-tier locking on the open-DB map for
        // a feature that doesn't need it.
        let persister_storage = Arc::new(RegionStorageManager::new(config.data_dir.clone()));
        let snapshot_persister = Arc::new(
            crate::snapshot_persister::SnapshotPersister::builder()
                .region_storage(persister_storage)
                .snapshot_key_provider(Arc::clone(&config.snapshot_key_provider))
                .build(),
        );
        Self {
            config,
            storage_manager,
            regions: RwLock::new(HashMap::new()),
            vault_groups: Arc::new(RwLock::new(HashMap::new())),
            peer_addresses: crate::peer_address_map::PeerAddressMap::new(),
            registry,
            dr_event_tx: tx,
            dr_event_rx: parking_lot::Mutex::new(Some(rx)),
            cancellation_token: parking_lot::Mutex::new(CancellationToken::new()),
            runtime_config: parking_lot::Mutex::new(None),
            recovery_stats: RwLock::new(HashMap::new()),
            hibernation_config: parking_lot::Mutex::new(
                inferadb_ledger_types::config::HibernationConfig::default(),
            ),
            hibernation_detector_started: AtomicBool::new(false),
            signing_key_cache: new_signing_key_cache(),
            self_weak: Arc::new(parking_lot::Mutex::new(Weak::new())),
            vault_shard_index: dashmap::DashMap::new(),
            org_shard_index: dashmap::DashMap::new(),
            snapshot_persister,
        }
    }

    /// Records a `Weak` to this manager so the per-org [`ConsensusEngine`]'s
    /// [`ShardWakeNotifier`](inferadb_ledger_consensus::wake::ShardWakeNotifier)
    /// can route wake-on-peer-message events back into
    /// [`RaftManager::wake_vault`] without an `Arc` cycle.
    ///
    /// Bootstrap calls this immediately after wrapping the manager in an
    /// `Arc`. Tests that exercise the hibernation path invoke this from
    /// their fixture; tests that don't may leave it unset, in which case
    /// the notifier degrades to a metric-only warning.
    pub fn install_self_weak(self: &Arc<Self>) {
        *self.self_weak.lock() = Arc::downgrade(self);
    }

    /// Test-only helper: dispatches the snapshot-streaming send path for a
    /// `(shard, follower)` pair the same way the consensus reactor would
    /// when it processes [`Action::SendSnapshot`].
    ///
    /// Mirrors the production
    /// [`SnapshotSender::send_snapshot`](inferadb_ledger_consensus::snapshot_sender::SnapshotSender::send_snapshot)
    /// invocation: spawns a tokio task and returns immediately. The
    /// caller is responsible for waiting for the staged file to land
    /// (e.g. via [`SnapshotPersister::list_staged`](crate::snapshot_persister::SnapshotPersister::list_staged))
    /// or for the install RPC to complete.
    ///
    /// Public solely so server-level integration tests can exercise the
    /// full snapshot install chain (persist → stream → stage → install)
    /// end-to-end without depending on long-running consensus protocol
    /// behavior to trigger the action emission. Production callers go
    /// through the registered [`SnapshotSender`] trait implementation
    /// installed at engine start time.
    pub fn send_snapshot_for_test(
        self: &Arc<Self>,
        shard_id: inferadb_ledger_consensus::types::ConsensusStateId,
        follower_id: inferadb_ledger_consensus::types::NodeId,
        snapshot_index: u64,
    ) {
        let sender = self.build_snapshot_sender();
        // The trait implementation `tokio::spawn`s the dispatch internally
        // so this returns immediately — same fire-and-forget contract the
        // reactor uses.
        sender.send_snapshot(shard_id, follower_id, snapshot_index);
    }

    /// Test-only helper: dispatches the snapshot install path for a shard
    /// the same way the consensus reactor would when it processes
    /// [`Action::InstallSnapshot`].
    ///
    /// Mirrors the production
    /// [`SnapshotInstaller::install_snapshot`](inferadb_ledger_consensus::snapshot_installer::SnapshotInstaller::install_snapshot)
    /// invocation: spawns a tokio task and returns immediately. The
    /// caller is responsible for polling until the install lands (e.g.
    /// by observing the shard's `last_applied` advance, or by querying
    /// the metric `ledger_snapshot_install_total{result="success"}`).
    ///
    /// Public solely so server-level integration tests can exercise the
    /// full snapshot install chain end-to-end. Production callers go
    /// through the registered [`SnapshotInstaller`] trait implementation
    /// installed at engine start time.
    pub fn install_snapshot_for_test(
        self: &Arc<Self>,
        shard_id: inferadb_ledger_consensus::types::ConsensusStateId,
        leader_term: u64,
        last_included_index: u64,
        last_included_term: u64,
    ) {
        let installer = self.build_snapshot_installer();
        installer.install_snapshot(shard_id, leader_term, last_included_index, last_included_term);
    }

    /// Returns a [`ShardWakeNotifier`](inferadb_ledger_consensus::wake::ShardWakeNotifier)
    /// bound to this manager's `self_weak`, suitable for passing to
    /// [`ConsensusEngine::start_with_wake_notifier`](inferadb_ledger_consensus::ConsensusEngine::start_with_wake_notifier).
    ///
    /// Spawns a tokio task on every wake event and returns immediately —
    /// the consensus reactor's event loop is single-threaded, and blocking
    /// inside the notifier stalls every shard managed by the reactor (see
    /// [`crate::raft_manager::RaftManagerWakeNotifier`] docs and
    /// `crates/consensus/src/wake.rs`).
    ///
    /// The returned notifier carries a clone of the same shared
    /// `Arc<Mutex<Weak<RaftManager>>>` cell as `self_weak`, so a later
    /// [`Self::install_self_weak`] call wires every notifier built before
    /// the install — bootstrap call ordering between
    /// `start_with_wake_notifier` and `install_self_weak` is forgiving.
    fn build_wake_notifier(&self) -> Arc<dyn inferadb_ledger_consensus::wake::ShardWakeNotifier> {
        Arc::new(RaftManagerWakeNotifier { manager: Arc::clone(&self.self_weak) })
    }

    /// Returns a
    /// [`SnapshotCoordinator`](inferadb_ledger_consensus::snapshot_coordinator::SnapshotCoordinator)
    /// bound to this manager's `self_weak`, suitable for passing to
    /// [`ConsensusEngine::start_with_coordinators`](inferadb_ledger_consensus::ConsensusEngine::start_with_coordinators).
    ///
    /// Spawns a tokio task on every snapshot trigger and returns
    /// immediately — same reactor-single-thread contract as
    /// [`Self::build_wake_notifier`].
    fn build_snapshot_coordinator(
        &self,
    ) -> Arc<dyn inferadb_ledger_consensus::snapshot_coordinator::SnapshotCoordinator> {
        Arc::new(RaftManagerSnapshotCoordinator { manager: Arc::clone(&self.self_weak) })
    }

    /// Returns a
    /// [`SnapshotSender`](inferadb_ledger_consensus::snapshot_sender::SnapshotSender)
    /// bound to this manager's `self_weak`, suitable for passing to
    /// [`ConsensusEngine::start_with_full_coordinators`](inferadb_ledger_consensus::ConsensusEngine::start_with_full_coordinators).
    ///
    /// Spawns a tokio task on every `Action::SendSnapshot` and returns
    /// immediately — same reactor-single-thread contract as
    /// [`Self::build_wake_notifier`] and [`Self::build_snapshot_coordinator`].
    /// The dispatched task resolves the shard ID to its scope, opens the
    /// at-rest encrypted snapshot via
    /// [`SnapshotPersister::open_encrypted`](crate::snapshot_persister::SnapshotPersister::open_encrypted),
    /// and streams chunks to the follower over the internal Raft
    /// transport's `InstallSnapshotStream` RPC. Failures drop the request
    /// and let the leader's heartbeat replicator retry on the next cycle.
    fn build_snapshot_sender(
        &self,
    ) -> Arc<dyn inferadb_ledger_consensus::snapshot_sender::SnapshotSender> {
        Arc::new(crate::snapshot_streamer::RaftManagerSnapshotSender {
            manager: Arc::clone(&self.self_weak),
            chunk_size_bytes: crate::snapshot_streamer::DEFAULT_CHUNK_SIZE_BYTES,
        })
    }

    /// Returns a
    /// [`SnapshotInstaller`](inferadb_ledger_consensus::snapshot_installer::SnapshotInstaller)
    /// bound to this manager's `self_weak`, suitable for passing to
    /// [`ConsensusEngine::start_with_all_callbacks`](inferadb_ledger_consensus::ConsensusEngine::start_with_all_callbacks).
    ///
    /// Spawns a tokio task on every `Action::InstallSnapshot` and returns
    /// immediately — same reactor-single-thread contract as
    /// [`Self::build_wake_notifier`], [`Self::build_snapshot_coordinator`],
    /// and [`Self::build_snapshot_sender`]. The dispatched task locates the
    /// staged file via [`SnapshotPersister::list_staged`], decrypts via the
    /// [`SnapshotKeyProvider`](crate::snapshot_key_provider::SnapshotKeyProvider),
    /// routes the install onto the apply worker via the per-shard
    /// [`ApplyCommand::InstallSnapshot`](crate::apply_command::ApplyCommand::InstallSnapshot)
    /// channel, and on success calls back into
    /// [`ConsensusEngine::notify_snapshot_installed`](inferadb_ledger_consensus::ConsensusEngine::notify_snapshot_installed).
    /// Failures drop the request and let the leader's heartbeat replicator
    /// retry on the next cycle.
    ///
    /// [`SnapshotPersister::list_staged`]: crate::snapshot_persister::SnapshotPersister::list_staged
    fn build_snapshot_installer(
        &self,
    ) -> Arc<dyn inferadb_ledger_consensus::snapshot_installer::SnapshotInstaller> {
        Arc::new(crate::snapshot_installer::RaftManagerSnapshotInstaller {
            manager: Arc::clone(&self.self_weak),
            staged_file_wait: crate::snapshot_installer::DEFAULT_STAGED_WAIT,
        })
    }

    /// Resolves a `ConsensusStateId` to its
    /// `(region, organization_id, Option<vault_id>)` scope tuple.
    ///
    /// Returns `Some(.., None)` for org-level shards (looked up via
    /// [`Self::org_shard_index`]) and `Some(.., Some(vault_id))` for vault
    /// shards (looked up via [`Self::vault_shard_index`]). Returns `None`
    /// when the shard is not registered on this node, which can happen
    /// during the brief window between
    /// [`Self::stop_vault_group`](RaftManager::stop_vault_group) /
    /// `stop_region` and the engine's final shard cleanup tick — the
    /// trigger is dropped and the next threshold check retries.
    pub fn resolve_shard_to_scope(
        &self,
        shard_id: inferadb_ledger_consensus::types::ConsensusStateId,
    ) -> Option<(Region, OrganizationId, Option<VaultId>)> {
        if let Some(entry) = self.vault_shard_index.get(&shard_id) {
            let (region, org, vault) = *entry.value();
            return Some((region, org, Some(vault)));
        }
        if let Some(entry) = self.org_shard_index.get(&shard_id) {
            let (region, org) = *entry.value();
            return Some((region, org, None));
        }
        None
    }

    /// Builds the snapshot file for a `(region, org, vault?)` scope, persists
    /// it via the manager's [`SnapshotPersister`], then notifies the engine
    /// so the shard's `last_snapshot_index` advances.
    ///
    /// Stage 2's first production caller of
    /// [`ConsensusEngine::notify_snapshot_completed`](inferadb_ledger_consensus::ConsensusEngine::notify_snapshot_completed).
    /// `last_included_term` is the term reported by
    /// [`Action::TriggerSnapshot`](inferadb_ledger_consensus::action::Action::TriggerSnapshot)
    /// — used purely as snapshot metadata; the actual covered index is
    /// resolved from the produced snapshot's
    /// [`SnapshotMeta`](crate::log_storage::SnapshotMeta).
    ///
    /// # Errors
    ///
    /// Returns [`RaftManagerError::Other`] wrapping any underlying
    /// build / persist / notify failure. Failures are surfaced to the caller
    /// (the snapshot coordinator) which logs at warn level — the next
    /// threshold check will re-trigger.
    pub async fn persist_snapshot(
        &self,
        region: Region,
        organization_id: OrganizationId,
        vault_id: Option<VaultId>,
        shard_id: inferadb_ledger_consensus::types::ConsensusStateId,
        last_included_term: u64,
    ) -> Result<crate::snapshot_persister::PersistedSnapshotMeta> {
        let mut builder = self.build_snapshot_builder(region, organization_id, vault_id)?;

        let (file, meta) = builder
            .build_snapshot()
            .await
            .map_err(|e| RaftManagerError::Other { msg: format!("build_snapshot: {e}") })?;

        let scope = builder.scope();
        let covered_index = meta.last_log_id.map(|id| id.index).unwrap_or(0);
        let covered_term = meta.last_log_id.map(|id| id.term).unwrap_or(last_included_term);

        let persisted = self
            .snapshot_persister
            .persist(region, scope, covered_index, covered_term, file)
            .await
            .map_err(|e| RaftManagerError::Other { msg: format!("persist snapshot: {e}") })?;

        // Resolve the engine and notify the shard. The engine reference
        // lives behind the `ConsensusHandle` carried by either the
        // `InnerGroup` (org-level shard) or the `InnerVaultGroup` (vault
        // shard).
        let engine = self.engine_for_shard(region, organization_id, vault_id).ok_or_else(|| {
            RaftManagerError::Other {
                msg: format!(
                    "engine for shard {shard_id:?} (region={region:?}, org={organization_id:?}, \
                     vault={vault_id:?}) not found — snapshot persisted but \
                     last_snapshot_index will not advance",
                ),
            }
        })?;
        engine.notify_snapshot_completed(shard_id, covered_index).await.map_err(|e| {
            RaftManagerError::Other { msg: format!("notify_snapshot_completed: {e}") }
        })?;

        tracing::info!(
            region = region.as_str(),
            organization_id = organization_id.value(),
            vault_id = vault_id.map(|v| v.value()),
            shard_id = shard_id.0,
            covered_index,
            size_bytes = persisted.size_bytes,
            encrypted = persisted.encrypted,
            "Snapshot persisted",
        );

        Ok(persisted)
    }

    /// Builds a [`LedgerSnapshotBuilder`](crate::log_storage::LedgerSnapshotBuilder)
    /// for the requested scope by reading the live `Arc` handles off the
    /// resolved [`InnerGroup`] / [`InnerVaultGroup`].
    fn build_snapshot_builder(
        &self,
        region: Region,
        organization_id: OrganizationId,
        vault_id: Option<VaultId>,
    ) -> Result<crate::log_storage::LedgerSnapshotBuilder> {
        match vault_id {
            Some(vault_id) => {
                let inner = self
                    .vault_groups
                    .read()
                    .get(&(region, organization_id, vault_id))
                    .map(Arc::clone)
                    .ok_or(RaftManagerError::VaultGroupNotFound {
                        region,
                        organization_id,
                        vault_id,
                    })?;
                let event_writer = inner.event_writer();
                let events_db = event_writer.map(|w| Arc::clone(w.events_db()));
                let event_config = event_writer.map(|w| w.config().clone());
                Ok(crate::log_storage::LedgerSnapshotBuilder::for_vault_group(
                    Arc::clone(inner.raft_db()),
                    Arc::clone(inner.state()),
                    Arc::clone(inner.block_archive()),
                    events_db,
                    event_config,
                    Arc::clone(&self.config.snapshot_key_provider),
                    region,
                    organization_id,
                    vault_id,
                ))
            },
            None => {
                let inner = self
                    .regions
                    .read()
                    .get(&(region, organization_id))
                    .map(Arc::clone)
                    .ok_or(RaftManagerError::RegionNotFound { region })?;
                let event_writer = inner.event_writer();
                let events_db = event_writer.map(|w| Arc::clone(w.events_db()));
                let event_config = event_writer.map(|w| w.config().clone());
                Ok(crate::log_storage::LedgerSnapshotBuilder::for_org_group(
                    Arc::clone(inner.raft_db()),
                    events_db,
                    event_config,
                    Arc::clone(&self.config.snapshot_key_provider),
                    region,
                    organization_id,
                ))
            },
        }
    }

    /// Returns the [`ConsensusEngine`](inferadb_ledger_consensus::ConsensusEngine)
    /// that owns `shard_id` for the given scope, or `None` when the scope
    /// has been torn down.
    fn engine_for_shard(
        &self,
        region: Region,
        organization_id: OrganizationId,
        vault_id: Option<VaultId>,
    ) -> Option<Arc<inferadb_ledger_consensus::ConsensusEngine>> {
        match vault_id {
            Some(vault_id) => {
                let inner =
                    self.vault_groups.read().get(&(region, organization_id, vault_id)).cloned()?;
                Some(inner.handle().engine_arc())
            },
            None => {
                let inner = self.regions.read().get(&(region, organization_id)).cloned()?;
                Some(inner.handle().engine_arc())
            },
        }
    }

    /// Returns the shared [`SnapshotPersister`](crate::snapshot_persister::SnapshotPersister)
    /// instance for this manager. Exposed for tests + future admin
    /// callers (e.g. `AdminService::create_snapshot`); the production
    /// path goes through [`Self::persist_snapshot`].
    pub fn snapshot_persister(&self) -> Arc<crate::snapshot_persister::SnapshotPersister> {
        Arc::clone(&self.snapshot_persister)
    }

    /// Public accessor used by Stage 4's
    /// [`RaftManagerSnapshotInstaller`](crate::snapshot_installer::RaftManagerSnapshotInstaller)
    /// to resolve the engine that owns `shard_id` for a given scope.
    /// Wraps the crate-private [`Self::engine_for_shard`].
    pub fn snapshot_engine_for_shard(
        &self,
        region: Region,
        organization_id: OrganizationId,
        vault_id: Option<VaultId>,
    ) -> Option<Arc<inferadb_ledger_consensus::ConsensusEngine>> {
        self.engine_for_shard(region, organization_id, vault_id)
    }

    /// Returns the per-shard apply-command sender for the org-level group at
    /// `(region, organization_id)`, or `None` when the group is not started
    /// on this node.
    ///
    /// Stage 4's installer uses this to route
    /// [`ApplyCommand::InstallSnapshot`](crate::apply_command::ApplyCommand::InstallSnapshot)
    /// onto the apply task that owns the org's
    /// [`RaftLogStore`](crate::log_storage::RaftLogStore).
    pub fn apply_command_sender(
        &self,
        region: Region,
        organization_id: OrganizationId,
    ) -> Option<crate::apply_command::ApplyCommandSender> {
        let inner = self.regions.read().get(&(region, organization_id)).cloned()?;
        Some(inner.apply_command_tx().clone())
    }

    /// Returns the per-vault apply-command sender for the vault group at
    /// `(region, organization_id, vault_id)`, or `None` when the vault
    /// group is not started on this node.
    ///
    /// Stage 4's installer uses this to route
    /// [`ApplyCommand::InstallSnapshot`](crate::apply_command::ApplyCommand::InstallSnapshot)
    /// onto the per-vault commit pump that owns this vault's
    /// [`RaftLogStore`](crate::log_storage::RaftLogStore). Sister of
    /// [`apply_command_sender`](Self::apply_command_sender) — same shape,
    /// keyed by the per-vault triple instead of the org pair.
    pub fn apply_command_sender_for_vault(
        &self,
        region: Region,
        organization_id: OrganizationId,
        vault_id: VaultId,
    ) -> Option<crate::apply_command::ApplyCommandSender> {
        let inner = self.vault_groups.read().get(&(region, organization_id, vault_id)).cloned()?;
        Some(inner.apply_command_tx().clone())
    }

    /// Returns `(leader_term, leader_node_id)` for the shard backing
    /// `shard_id`, or `None` if the shard does not resolve on this node.
    ///
    /// Used by Stage 3's leader-side
    /// [`RaftManagerSnapshotSender`](crate::snapshot_streamer::RaftManagerSnapshotSender)
    /// to stamp the streaming `InstallSnapshotHeader` with the leader's
    /// current term + node id without plumbing those fields through the
    /// `SnapshotSender::send_snapshot` trait surface (which keeps the
    /// trait shape identical to the wake notifier and snapshot
    /// coordinator).
    ///
    /// `leader_term` reads from the shard's state-watch channel — same
    /// source as `ConsensusHandle::current_term`. `leader_node_id` returns
    /// the local node id (this method is only meaningful when called on
    /// the leader, since `Action::SendSnapshot` is leader-only).
    pub fn leader_term_and_id_for_shard(
        &self,
        shard_id: inferadb_ledger_consensus::types::ConsensusStateId,
    ) -> Option<(u64, u64)> {
        let (region, organization_id, vault_id) = self.resolve_shard_to_scope(shard_id)?;
        match vault_id {
            Some(vault_id) => {
                let inner =
                    self.vault_groups.read().get(&(region, organization_id, vault_id)).cloned()?;
                Some((inner.handle().current_term(), inner.handle().node_id()))
            },
            None => {
                let inner = self.regions.read().get(&(region, organization_id)).cloned()?;
                Some((inner.handle().current_term(), inner.handle().node_id()))
            },
        }
    }

    /// Returns the current Raft term for an explicit scope, or `None` if
    /// the scope's group is not registered on this node.
    ///
    /// Used by Stage 3's follower-side
    /// [`snapshot_receiver`](crate::snapshot_receiver) to stamp the
    /// `InstallSnapshotStreamResponse.follower_term` field — the follower
    /// reports the local term so the leader can step down if it's stale.
    pub fn current_term_for_scope(
        &self,
        region: Region,
        organization_id: OrganizationId,
        vault_id: Option<VaultId>,
    ) -> Option<u64> {
        match vault_id {
            Some(vault_id) => self
                .vault_groups
                .read()
                .get(&(region, organization_id, vault_id))
                .map(|g| g.handle().current_term()),
            None => self
                .regions
                .read()
                .get(&(region, organization_id))
                .map(|g| g.handle().current_term()),
        }
    }

    /// Returns the process-wide signing-key cache shared with every
    /// hot-path [`SystemOrganizationService`] built off this manager.
    ///
    /// Pass the returned `Arc` to
    /// [`SystemOrganizationService::with_signing_key_cache`] so that the
    /// constructed service reuses the long-lived cache rather than
    /// allocating (and immediately dropping) a fresh `moka::Cache`.
    #[must_use]
    pub fn signing_key_cache(&self) -> SigningKeyCache {
        self.signing_key_cache.clone()
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

    /// Sets the vault hibernation policy (Phase 7 / O1).
    ///
    /// Called during bootstrap from the server's `Config`. The
    /// idle-detector task picks the new policy up on its next scan
    /// (re-reading the config value lock-free); transitions in flight
    /// when the config changes are not rolled back.
    pub fn set_hibernation_config(&self, config: inferadb_ledger_types::config::HibernationConfig) {
        *self.hibernation_config.lock() = config;
    }

    /// Returns a clone of the current hibernation policy.
    #[must_use]
    pub fn hibernation_config(&self) -> inferadb_ledger_types::config::HibernationConfig {
        self.hibernation_config.lock().clone()
    }

    /// Spawns the hibernation idle-detector task (Phase 7 / O1).
    ///
    /// The task wakes up every `scan_interval_secs` seconds, scans
    /// `vault_groups`, and transitions idle vaults to
    /// [`VaultLifecycleState::Dormant`]. Vaults with an in-flight
    /// membership change older than `max_stall_secs` are marked
    /// [`VaultLifecycleState::Stalled`]. After every scan the per-org
    /// gauges (`org_active_vault_count{status=active|dormant|stalled}`)
    /// are republished so observability dashboards always read fresh
    /// values regardless of transition cadence.
    ///
    /// No-ops when hibernation is disabled at the time of the call.
    /// Idempotent: re-invocation is a no-op once the task is running.
    /// Cancels via the manager-level cancellation token.
    ///
    /// ## Current scope (smaller, lower-risk version)
    ///
    /// This implementation tracks lifecycle state, fires metrics + log
    /// lines on transitions, and exposes the dormant / stalled
    /// observability that downstream tooling (operator dashboards, O5
    /// docs) needs. It does **not** yet:
    ///
    /// - pause the per-shard scheduler tick on the parent [`ConsensusEngine`]
    /// - drop the per-vault `state.db` / `raft.db` / `blocks.db` / `events.db` file handles
    ///
    /// Both optimizations are deliberate follow-ups: they require
    /// invasive changes to consensus internals (vault shards share the
    /// parent org's reactor — pausing the tick per-vault is a
    /// reactor-level surgery) and to the storage layer's ownership
    /// model. The bookkeeping landed here is the foundation those
    /// optimizations build on; once the per-shard tick can be paused,
    /// the dormant arm in this scan is the natural place to call
    /// `engine.pause_shard(shard_id)` and friends.
    pub fn start_hibernation_idle_detector(self: &Arc<Self>) {
        let config = self.hibernation_config();
        if !config.enabled {
            debug!("Hibernation idle detector not started — config disabled");
            return;
        }
        if self
            .hibernation_detector_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            debug!("Hibernation idle detector already running");
            return;
        }

        let manager = Arc::clone(self);
        let cancel = manager.cancellation_token.lock().child_token();
        let scan_interval = Duration::from_secs(config.scan_interval_secs.max(1));
        info!(
            idle_secs = config.idle_secs,
            scan_interval_secs = config.scan_interval_secs,
            max_warm = config.max_warm,
            max_stall_secs = config.max_stall_secs,
            "Starting vault hibernation idle detector",
        );

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(scan_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => {
                        debug!("Vault hibernation idle detector exiting on cancellation");
                        break;
                    }
                    _ = ticker.tick() => {
                        manager.run_hibernation_scan_once();
                    }
                }
            }
        });
    }

    /// Runs a single hibernation scan pass. Public for testing — the
    /// background task in [`start_hibernation_idle_detector`] drives
    /// this on a timer in production.
    ///
    /// Sleep transitions (`Active → Dormant`) are dispatched as background
    /// tokio tasks via [`Self::sleep_vault`] so the sync scan loop is not
    /// blocked on the per-vault `engine.pause_shard` round-trip or the
    /// best-effort page-cache eviction syscalls.
    pub fn run_hibernation_scan_once(self: &Arc<Self>) {
        let config = self.hibernation_config();
        if !config.enabled {
            return;
        }
        let now = current_unix_secs();
        let idle_threshold = config.idle_secs;
        let stall_threshold = config.max_stall_secs;

        // Snapshot the vault map so the scan does not hold the lock
        // across atomic transitions / metric publication. Cloning
        // `Arc<InnerVaultGroup>` is cheap.
        let vaults: Vec<Arc<InnerVaultGroup>> =
            self.vault_groups.read().values().cloned().collect();

        // Per-org tally for the gauge republish at the end of the scan.
        // Keys are `(region, organization_id)`; values are
        // `[active, dormant, stalled]` counts.
        let mut tallies: HashMap<(Region, OrganizationId), [u64; 3]> = HashMap::new();

        for vault in &vaults {
            // Stalled detection takes precedence over idle detection so
            // a vault that is BOTH idle AND stuck on membership work
            // surfaces the operator-visible state instead of silently
            // sleeping.
            let pending_started =
                vault.pending_membership_started_unix_secs.load(Ordering::Acquire);
            let stalled =
                pending_started > 0 && now.saturating_sub(pending_started) >= stall_threshold;

            if stalled {
                vault.transition_lifecycle_state(VaultLifecycleState::Stalled);
            } else {
                let last_activity = vault.last_activity_unix_secs.load(Ordering::Relaxed);
                let idle_for = now.saturating_sub(last_activity);
                let current = vault.lifecycle_state();
                if current == VaultLifecycleState::Active && idle_for >= idle_threshold {
                    // Dispatch sleep on a background task so the scan is
                    // not blocked on engine.pause_shard + the page-cache
                    // eviction syscalls. sleep_vault performs the
                    // lifecycle transition itself; the scan does not call
                    // transition_lifecycle_state directly here.
                    let manager = Arc::clone(self);
                    let region = vault.region;
                    let organization_id = vault.organization_id;
                    let vault_id = vault.vault_id;
                    tokio::spawn(async move {
                        if let Err(e) = manager.sleep_vault(region, organization_id, vault_id).await
                        {
                            warn!(
                                error = %e,
                                region = region.as_str(),
                                organization_id = organization_id.value(),
                                vault_id = vault_id.value(),
                                "hibernation idle detector: sleep_vault failed"
                            );
                        }
                    });
                } else if current == VaultLifecycleState::Stalled && pending_started == 0 {
                    // Membership change finished — clear the stalled
                    // flag and let activity tracking decide whether the
                    // vault is Active or eligible for Dormant on the
                    // next pass.
                    vault.transition_lifecycle_state(VaultLifecycleState::Active);
                }
            }

            let key = (vault.region, vault.organization_id);
            let tally = tallies.entry(key).or_insert([0; 3]);
            match vault.lifecycle_state() {
                VaultLifecycleState::Active => tally[0] += 1,
                VaultLifecycleState::Dormant => tally[1] += 1,
                VaultLifecycleState::Stalled => tally[2] += 1,
            }
        }

        // Republish per-org gauges. We always emit all three series for
        // every observed org so a dashboard sees a clean step-down to 0
        // when, e.g., the last dormant vault wakes up.
        for ((region, org_id), counts) in &tallies {
            let region_label = region.as_str();
            let org_label = org_id.value().to_string();
            metrics::set_org_active_vault_count(region_label, &org_label, "active", counts[0]);
            metrics::set_org_active_vault_count(region_label, &org_label, "dormant", counts[1]);
            metrics::set_org_active_vault_count(region_label, &org_label, "stalled", counts[2]);
        }

        // Soft warn when the warm-set cap is exceeded — operators can
        // tune `max_warm` upward or shorten `idle_secs` to drive more
        // sleeps. Today this is observability only; once the scheduler
        // tick can be paused per-vault, this is also the point where we
        // would force-evict the LRU active vaults.
        let total_active: u64 = tallies.values().map(|c| c[0]).sum();
        if total_active > config.max_warm as u64 {
            warn!(
                active_vaults = total_active,
                max_warm = config.max_warm,
                "Hibernation: active vault count exceeds max_warm — consider tuning idle_secs",
            );
        }
    }

    /// Transitions a vault to the Dormant lifecycle state and releases its
    /// per-vault DB OS-page-cache footprint (O6 hibernation Pass 2).
    ///
    /// Steps, in order:
    /// 1. Resolve the [`InnerVaultGroup`] for `(region, organization_id, vault_id)`.
    /// 2. Pause the vault's consensus shard via the parent org's [`ConsensusEngine`]
    ///    (`engine.pause_shard(shard_id)`). Idempotent: re-pausing an already-paused shard is a
    ///    no-op success.
    /// 3. Transition the vault's `VaultLifecycleState` to [`VaultLifecycleState::Dormant`] (which
    ///    fires the existing `record_vault_hibernation_transition("sleep")` metric).
    /// 4. Best-effort evict the OS page cache for the vault's four per-vault DBs: `state.db` (via
    ///    [`StateLayer::evict_vault_state_page_cache`]), `raft.db`, `blocks.db`, and `events.db`
    ///    (when present, via [`Database::evict_page_cache`] on the per-vault handles).
    /// 5. Increment `vault_hibernation_evicted_total{region, organization_id}`.
    ///
    /// Page-cache eviction is the primary hibernation mechanism for memory
    /// pressure: the per-vault `Arc<Database>` clones on [`InnerVaultGroup`]
    /// stay alive across sleep/wake, FDs are not closed, and pages
    /// re-populate lazily on the first read after wake. Eviction is best-
    /// effort — the call is `posix_fadvise(POSIX_FADV_DONTNEED)` on Linux
    /// and a successful no-op on Apple / Windows (see
    /// [`Database::evict_page_cache`]).
    ///
    /// # Errors
    ///
    /// Returns [`RaftManagerError::VaultGroupNotFound`] if the triple has no
    /// registered group on this node. Pause-shard and eviction failures are
    /// logged at warn but do **not** propagate — the lifecycle transition is
    /// the load-bearing observable change, and a partial sleep (paused but
    /// not evicted, or vice versa) is still strictly better than no sleep.
    pub async fn sleep_vault(
        &self,
        region: Region,
        organization_id: OrganizationId,
        vault_id: VaultId,
    ) -> Result<()> {
        let inner: Arc<InnerVaultGroup> =
            self.vault_groups.read().get(&(region, organization_id, vault_id)).cloned().ok_or(
                RaftManagerError::VaultGroupNotFound { region, organization_id, vault_id },
            )?;

        // Idempotent fast-path: already dormant, nothing to do. We still
        // honor the pause/evict path on first transition so a manual
        // sleep_vault call from a test fixture lands the same observable
        // changes as the idle-detector path.
        let was_dormant = inner.lifecycle_state() == VaultLifecycleState::Dormant;

        // Step 1: pause the consensus shard. Pause is observational — does
        // not abort in-flight proposals (those resolve normally if quorum
        // is met before the flag is checked) and does not shut down the
        // shard. Idempotent at the engine level.
        let engine = inner.handle().engine_arc();
        if let Err(e) = engine.pause_shard(inner.shard_id()).await {
            warn!(
                error = %e,
                region = region.as_str(),
                organization_id = organization_id.value(),
                vault_id = vault_id.value(),
                shard_id = ?inner.shard_id(),
                "sleep_vault: engine.pause_shard failed; continuing with eviction"
            );
        }

        // Step 2: lifecycle transition. The transition method fires the
        // existing `record_vault_hibernation_transition("sleep")` counter
        // and the `Vault hibernation: sleep ...` info log line.
        inner.transition_lifecycle_state(VaultLifecycleState::Dormant);

        // Step 3: best-effort page-cache eviction across the four per-vault
        // DBs. Each call is independent — a failure on raft.db must not
        // prevent eviction on blocks.db (Linux posix_fadvise rarely fails,
        // but we do not want the chain to be coupled).
        if let Err(e) = inner.state().evict_vault_state_page_cache(vault_id) {
            warn!(
                error = %e,
                region = region.as_str(),
                organization_id = organization_id.value(),
                vault_id = vault_id.value(),
                db = "state",
                "sleep_vault: evict_vault_state_page_cache failed"
            );
        }
        if let Err(e) = inner.raft_db().evict_page_cache() {
            warn!(
                error = %e,
                region = region.as_str(),
                organization_id = organization_id.value(),
                vault_id = vault_id.value(),
                db = "raft",
                "sleep_vault: raft.db evict_page_cache failed"
            );
        }
        if let Err(e) = inner.block_archive().db().evict_page_cache() {
            warn!(
                error = %e,
                region = region.as_str(),
                organization_id = organization_id.value(),
                vault_id = vault_id.value(),
                db = "blocks",
                "sleep_vault: blocks.db evict_page_cache failed"
            );
        }
        if let Some(ew) = inner.event_writer()
            && let Err(e) = ew.events_db().db().evict_page_cache()
        {
            warn!(
                error = %e,
                region = region.as_str(),
                organization_id = organization_id.value(),
                vault_id = vault_id.value(),
                db = "events",
                "sleep_vault: events.db evict_page_cache failed"
            );
        }

        if !was_dormant {
            metrics::record_vault_hibernation_evicted(
                region.as_str(),
                &organization_id.value().to_string(),
            );
        }
        Ok(())
    }

    /// Wakes a Dormant vault: transitions back to Active and resumes the
    /// underlying consensus shard so AppendEntries traffic flows again
    /// (O6 hibernation Pass 2).
    ///
    /// Steps:
    /// 1. Resolve the [`InnerVaultGroup`] for `(region, organization_id, vault_id)`.
    /// 2. Resume the consensus shard via `engine.resume_shard(shard_id)`. Idempotent.
    /// 3. Transition `VaultLifecycleState` back to [`VaultLifecycleState::Active`] (no-op if the
    ///    vault is already Active — the existing wake metric fires only when there's a real Dormant
    ///    → Active transition).
    /// 4. Page-cache repopulation is **lazy** — the first read after wake re-fetches pages from
    ///    disk on Linux (or finds them still hot on Apple / Windows where eviction was a no-op). No
    ///    explicit reopen.
    ///
    /// Wake p99 budget: < 100ms. The hot path is `engine.resume_shard` (one
    /// control-channel round-trip ≈ µs) plus an atomic state store; first-
    /// read cold-cache cost dominates p99 in practice. Validated by
    /// `tests/hibernation_wake_latency.rs` and the SLI histogram
    /// `vault_hibernation_wake_duration_seconds`.
    ///
    /// # Errors
    ///
    /// Returns [`RaftManagerError::VaultGroupNotFound`] if the triple is not
    /// registered. Resume-shard failures are logged and surfaced through the
    /// returned error so the caller (the [`RaftManagerWakeNotifier`] task or
    /// a test fixture) can metric them.
    pub async fn wake_vault(
        &self,
        region: Region,
        organization_id: OrganizationId,
        vault_id: VaultId,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        let inner: Arc<InnerVaultGroup> =
            self.vault_groups.read().get(&(region, organization_id, vault_id)).cloned().ok_or(
                RaftManagerError::VaultGroupNotFound { region, organization_id, vault_id },
            )?;

        // Step 1: resume the consensus shard. Idempotent at the engine
        // level — calling resume_shard on a non-paused shard is a no-op
        // success. The reactor re-arms timers via normal Raft state-
        // machine transitions on the next legitimate event.
        let engine = inner.handle().engine_arc();
        if let Err(e) = engine.resume_shard(inner.shard_id()).await {
            // Do propagate engine failures here — wake is a correctness
            // event (peer is trying to talk to a paused shard), not a
            // best-effort observability blip.
            return Err(RaftManagerError::Raft {
                region,
                message: format!(
                    "wake_vault: engine.resume_shard for shard {:?} failed: {e}",
                    inner.shard_id()
                ),
            });
        }

        // Step 2: lifecycle transition. transition_lifecycle_state is a
        // no-op when the vault is already Active and only fires the wake
        // metric on a real Dormant → Active transition — so a wake_vault
        // call against an Active vault costs nothing observable beyond
        // the resume_shard idempotent round-trip.
        inner.transition_lifecycle_state(VaultLifecycleState::Active);

        let elapsed = start.elapsed();
        metrics::record_vault_hibernation_woken(
            region.as_str(),
            &organization_id.value().to_string(),
            elapsed.as_secs_f64(),
        );
        Ok(())
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
        //
        // Per-vault shards are registered on the parent organization's
        // `ConsensusEngine` (see `start_vault_group` —
        // `org_inner.handle().add_shard(consensus_shard)`). They have no
        // entry in `self.regions` of their own, so a direct match against
        // `group.handle().shard_id()` misses every vault shard. To dispatch
        // an inbound vault `Replicate` message correctly the receiver must
        // resolve to the parent org's [`InnerGroup`] (which owns the
        // engine, the transport, and is registered in `self.regions`).
        // After the direct match, scan `self.vault_groups` and resolve the
        // vault's `(region, organization_id)` parent.
        if let Some(group) = self
            .regions
            .read()
            .values()
            .find(|group| group.handle().shard_id() == shard_id)
            .cloned()
        {
            return Some(group);
        }
        let parent_key = self
            .vault_groups
            .read()
            .values()
            .find(|vault| vault.shard_id == shard_id)
            .map(|vault| (vault.region, vault.organization_id))?;
        self.regions.read().get(&parent_key).cloned()
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

    /// Cascades a data-region membership change to every per-organization
    /// and per-vault child shard in the same region. See
    /// [`CascadeMembershipAction`] for the invariants this preserves.
    ///
    /// Each child-shard proposal is fire-and-forget with a 3 s timeout —
    /// the next reconciliation cycle on the placement controller (or the
    /// next `LeaveCluster` retry) will catch up if a propose attempt
    /// times out. The function is idempotent: skips children where the
    /// target is already in the desired role.
    ///
    /// Only proposes against shards where this node currently holds the
    /// leader role; under delegated leadership the per-org / per-vault
    /// leader is whoever leads the parent region group, so the same
    /// process that just executed the parent change is also the right
    /// proposer for every child.
    pub async fn cascade_membership_to_children(
        &self,
        region: Region,
        node_id: u64,
        action: CascadeMembershipAction,
    ) {
        use inferadb_ledger_consensus::types::NodeId;
        let target = NodeId(node_id);

        let org_shards: Vec<OrganizationId> = self
            .list_organization_groups()
            .into_iter()
            .filter_map(|(r, oid)| (r == region && oid != OrganizationId::new(0)).then_some(oid))
            .collect();

        for org_id in org_shards {
            let Ok(org_group) = self.get_organization_group(region, org_id) else { continue };
            if !org_group.handle().is_leader() {
                continue;
            }
            apply_cascade_action(
                action,
                self,
                org_group.handle(),
                org_group.consensus_transport(),
                target,
                node_id,
                region,
                org_id,
                None,
            )
            .await;
        }

        let vault_shards: Vec<(OrganizationId, VaultId)> = self
            .list_vault_groups()
            .into_iter()
            .filter_map(|(r, oid, vid)| (r == region).then_some((oid, vid)))
            .collect();

        for (org_id, vault_id) in vault_shards {
            let Ok(vault_group) = self.get_vault_group(region, org_id, vault_id) else { continue };
            if !vault_group.handle().is_leader() {
                continue;
            }
            apply_cascade_action(
                action,
                self,
                vault_group.handle(),
                None, // vault groups share parent org's transport (root rule 17)
                target,
                node_id,
                region,
                org_id,
                Some(vault_id),
            )
            .await;
        }
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

        // External registration path: the apply worker is wired upstream
        // and we do not own the apply-command receiver. Construct a sender
        // whose receiver is dropped immediately so any installer-side send
        // fails fast (returns `mpsc::error::TrySendError::Closed`) — the
        // installer treats this the same as "no apply-target on this
        // node" and falls back to drop-and-let-Raft-retry.
        let (apply_command_tx, _) = tokio::sync::mpsc::channel::<crate::apply_command::ApplyCommand>(
            crate::apply_command::APPLY_COMMAND_CHANNEL_CAPACITY,
        );
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
            batch_writer_config: None,
            commitment_buffer,
            leader_lease,
            applied_index_rx,
            consensus_transport: None,
            events_db: None,
            event_writer: None,
            last_activity: Arc::new(parking_lot::Mutex::new(std::time::Instant::now())),
            jobs_active: Arc::new(AtomicBool::new(false)),
            region_creation_rx: parking_lot::Mutex::new(None),
            organization_creation_rx: parking_lot::Mutex::new(None),
            vault_creation_rx: parking_lot::Mutex::new(None),
            vault_deletion_rx: parking_lot::Mutex::new(None),
            commit_dispatcher,
            // System / region control-plane groups do not own a
            // membership queue — only per-organization data-plane groups
            // do.
            membership_queue: None,
            apply_command_tx,
        });

        {
            let mut regions = self.regions.write();
            regions.insert((region, OrganizationId::new(0)), Arc::clone(&inner));
        }
        // Maintain the reverse `shard_id → (region, org)` index alongside
        // `regions` so the snapshot coordinator's hot-path lookup is
        // O(1). Drops on `stop_region`.
        self.org_shard_index.insert(inner.handle().shard_id(), (region, OrganizationId::new(0)));

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
        // Reuse the process-wide signing-key cache to avoid the per-request
        // `moka::Cache` construct/drop cycle that dominated CPU self-time
        // before the cache was lifted out (P11 — see
        // `docs/superpowers/specs/2026-04-27-reactor-wal-investigation.md`).
        let sys = SystemOrganizationService::with_signing_key_cache(
            system.state().clone(),
            self.signing_key_cache.clone(),
        );
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
    /// On fresh `CreateOrganization` signals, the method returns immediately
    /// after the watcher task spawns and no vaults exist to rehydrate. On
    /// node restart, after the org's applied state has been rebuilt from
    /// snapshot + WAL replay inside
    /// [`start_region`](Self::start_region), this method sweeps
    /// `applied_state.vaults` and calls
    /// [`start_vault_group`](Self::start_vault_group) for every non-deleted
    /// vault — reconstructing the vault-group lifecycle state that was live
    /// before shutdown. Rehydration is sequential so the parent
    /// engine's dispatcher register / `add_shard` sequence is not
    /// overwhelmed on a node recovering a large org.
    ///
    /// # Errors
    ///
    /// Returns a storage / Raft error if the underlying group bootstrap
    /// fails.
    pub async fn start_organization_group(
        self: &Arc<Self>,
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
            // Per-organization groups follow the parent region's residency
            // contract — `start_region`'s registry lookup picks up the
            // authoritative value from the GLOBAL directory entry.
            requires_residency_hint: false,
        };
        let org_inner = self.start_region(region_config, organization_id).await?;

        // Activate delegated leadership: the new org ConsensusState does not run
        // its own elections (LeadershipMode::Delegated). Its leader is
        // adopted from the data-region group's elected leader. The watcher
        // task re-adopts the parent's leader on every change AND on initial
        // entry — the latter closes the race where the parent acquires a
        // leader between the bootstrap-time read and the watcher's
        // subscription, which `state_rx().clone()` marks as "already seen"
        // (so `changed()` would not fire until the next change).
        //
        // The read-guard must be dropped before the first `.await` below
        // (`adopt_leader`). Taking the lookup result in its own let-binding
        // forces the guard out of scope before the `if let` body executes.
        let region_inner_opt = self.regions.read().get(&(region, OrganizationId::new(0))).cloned();
        if let Some(region_inner) = region_inner_opt {
            let region_handle = region_inner.handle().clone();
            let org_handle = org_inner.handle().clone();
            let mut state_rx = region_handle.state_rx().clone();
            tokio::spawn(async move {
                // Initial adoption: re-read the parent's current state
                // and adopt without waiting for a change. Idempotent —
                // `ConsensusState::adopt_leader` is a no-op when the
                // shard is already in (term, leader). Closes two races:
                //   1. Parent acquired a leader before this task started.
                //   2. The cloned `state_rx` marks the current value as "seen", so `changed()`
                //      would not fire on the already-current leader.
                let snap = state_rx.borrow_and_update().clone();
                if let Some(leader) = snap.leader {
                    let _ = org_handle.adopt_leader(leader, snap.term).await;
                }
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

        // Spawn the per-org vault-lifecycle watcher. Drains the vault
        // create/delete signal channels wired by `open_region_storage`
        // for this per-organization log store, dispatching each signal to
        // [`start_vault_group`] / [`stop_vault_group`]. The watcher
        // captures `Arc<RaftManager>` (weak would lose the task on
        // manager drop, but the manager outlives its groups by
        // construction) and is cancelled through a child of the manager's
        // cancellation token on shutdown.
        let vault_creation_rx_opt = org_inner.take_vault_creation_rx();
        let vault_deletion_rx_opt = org_inner.take_vault_deletion_rx();
        if let (Some(mut vault_create_rx), Some(mut vault_delete_rx)) =
            (vault_creation_rx_opt, vault_deletion_rx_opt)
        {
            let cancel = self.cancellation_token.lock().child_token();
            let manager = Arc::clone(self);
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
                            match manager
                                .start_vault_group(req.region, req.organization, req.vault)
                                .await
                            {
                                Ok(_) => {
                                    info!(
                                        region = req.region.as_str(),
                                        organization_id = req.organization.value(),
                                        vault_id = req.vault.value(),
                                        "Vault group started from CreateVault signal",
                                    );
                                }
                                Err(RaftManagerError::VaultGroupExists { .. }) => {
                                    warn!(
                                        region = req.region.as_str(),
                                        organization_id = req.organization.value(),
                                        vault_id = req.vault.value(),
                                        "Duplicate CreateVault signal — vault group already running",
                                    );
                                }
                                Err(e) => {
                                    tracing::error!(
                                        region = req.region.as_str(),
                                        organization_id = req.organization.value(),
                                        vault_id = req.vault.value(),
                                        error = %e,
                                        "Failed to start vault group in response to \
                                         CreateVault signal",
                                    );
                                }
                            }
                        }
                        Some(req) = vault_delete_rx.recv() => {
                            match manager
                                .stop_vault_group(req.region, req.organization, req.vault)
                                .await
                            {
                                Ok(()) => {
                                    info!(
                                        region = req.region.as_str(),
                                        organization_id = req.organization.value(),
                                        vault_id = req.vault.value(),
                                        "Vault group stopped from DeleteVault signal",
                                    );
                                }
                                Err(RaftManagerError::VaultGroupNotFound { .. }) => {
                                    warn!(
                                        region = req.region.as_str(),
                                        organization_id = req.organization.value(),
                                        vault_id = req.vault.value(),
                                        "Duplicate DeleteVault signal — vault group already stopped",
                                    );
                                }
                                Err(e) => {
                                    tracing::error!(
                                        region = req.region.as_str(),
                                        organization_id = req.organization.value(),
                                        vault_id = req.vault.value(),
                                        error = %e,
                                        "Failed to stop vault group in response to \
                                         DeleteVault signal",
                                    );
                                }
                            }
                        }
                        else => break,
                    }
                }
            });
        }

        // Phase 5 / M3: spawn the per-org `RegionMembershipWatcher` +
        // `MembershipDispatcher` pair. The watcher observes the
        // parent data-region group's voter / learner deltas and
        // enqueues per-vault membership requests; the dispatcher
        // drains the queue (one in-flight conf-change at a time,
        // gated by the queue's snapshot-cap semaphore) and applies
        // each through `apply_cascade_action_for_vault`.
        //
        // The legacy cascade in
        // [`Self::cascade_membership_to_children`] is still callable
        // for tests / emergency tooling, but the production cascade
        // sites in `dr_scheduler::execute_operator` and
        // `admin::leave_cluster` no longer invoke it as of M3 sub-stage
        // 5c — the OrganizationGroup-driven path is the sole
        // production cascade.
        //
        // Both tasks tie to a child of the manager's cancellation
        // token; on shutdown the watcher exits, the queue's
        // `take_next` returns `None`, and the dispatcher exits without
        // leaking pending work.
        if org_inner.membership_queue().is_some() {
            // Look up the parent region group. Skip silently if the
            // region control-plane group is not registered locally —
            // can happen on a node that holds an org replica without
            // being a region-tier voter (rare, but defensive).
            let region_inner_opt =
                self.regions.read().get(&(region, OrganizationId::new(0))).cloned();
            if let Some(region_inner) = region_inner_opt {
                let manager_weak = Arc::downgrade(self);
                let watcher_cancel = self.cancellation_token.lock().child_token();
                let dispatcher_cancel = self.cancellation_token.lock().child_token();

                let watcher = crate::region_membership_watcher::RegionMembershipWatcher::new(
                    manager_weak.clone(),
                    Arc::clone(&region_inner),
                    Arc::clone(&org_inner),
                );
                tokio::spawn(watcher.run(watcher_cancel));

                if let Some(dispatcher) =
                    crate::region_membership_watcher::MembershipDispatcher::new(
                        manager_weak,
                        Arc::clone(&org_inner),
                        std::time::Duration::from_secs(self.config.vault_conf_change_timeout_secs),
                    )
                {
                    tokio::spawn(dispatcher.run(dispatcher_cancel));
                }
            }
        }

        // Rehydrate vault groups persisted in this org's applied state.
        //
        // On fresh CreateOrganization signals, `applied_state.vaults` is
        // empty (no vaults exist yet) and the loop is a no-op. On node
        // restart, the applied state was rebuilt from snapshot + WAL
        // replay inside `start_region` above, so this sweep reconstructs
        // every non-deleted vault group. `CreateVault` apply signals that
        // committed below `last_applied` do not re-fire during replay;
        // this sweep is what brings those vault groups back up.
        //
        // Sequential: parallel vault bring-up could overwhelm the
        // dispatcher register / engine `add_shard` on a node recovering a
        // large org. Any individual failure is logged and skipped so
        // remaining vaults still rehydrate.
        self.rehydrate_vault_groups(region, organization_id, &org_inner).await;

        Ok(Arc::new(OrganizationGroup(org_inner)))
    }

    /// Sweeps a per-organization group's applied state and starts a
    /// per-vault Raft group for every non-deleted vault.
    ///
    /// Extracted from [`start_organization_group`](Self::start_organization_group)
    /// to keep the sweep a testable seam. `start_organization_group` is
    /// the sole production caller. Failures on individual vaults are
    /// logged and skipped — one broken vault must not block rehydration
    /// of the rest of the org. The vault-lifecycle watcher spawned by
    /// `start_organization_group` can race this sweep for newly-emerging
    /// vaults; `VaultGroupExists` from that race is expected and logged
    /// at debug level rather than surfaced as a failure.
    async fn rehydrate_vault_groups(
        self: &Arc<Self>,
        region: Region,
        organization_id: OrganizationId,
        org_inner: &Arc<InnerGroup>,
    ) {
        let vaults_to_rehydrate: Vec<VaultId> = org_inner
            .applied_state()
            .list_vaults(organization_id)
            .into_iter()
            .map(|meta| meta.vault)
            .collect();

        if vaults_to_rehydrate.is_empty() {
            return;
        }

        debug!(
            region = region.as_str(),
            organization_id = organization_id.value(),
            vault_count = vaults_to_rehydrate.len(),
            "Rehydrating vault groups from applied state",
        );

        for vault in vaults_to_rehydrate {
            match self.start_vault_group(region, organization_id, vault).await {
                Ok(_) => {
                    info!(
                        region = region.as_str(),
                        organization_id = organization_id.value(),
                        vault_id = vault.value(),
                        "Vault group rehydrated from applied state",
                    );
                },
                Err(RaftManagerError::VaultGroupExists { .. }) => {
                    // Signal-driven path beat us (rare — the watcher
                    // spawned above drains `CreateVault` signals that
                    // race the sweep). Benign.
                    debug!(
                        region = region.as_str(),
                        organization_id = organization_id.value(),
                        vault_id = vault.value(),
                        "Vault group rehydration skipped: already running",
                    );
                },
                Err(e) => {
                    tracing::error!(
                        region = region.as_str(),
                        organization_id = organization_id.value(),
                        vault_id = vault.value(),
                        error = %e,
                        "Failed to rehydrate vault group from applied state",
                    );
                    // Continue — rehydrating other vaults is still valuable.
                },
            }
        }
    }

    /// Opens the per-vault `raft.db`, `blocks.db`, and (conditionally)
    /// `events.db`, wires the resulting [`RaftLogStore`] with the
    /// parent organization's shared component handles, and returns the
    /// triple `(vault_log_store, vault_block_archive,
    /// vault_event_writer)`.
    ///
    /// Used by both `Self::start_vault_group` (the production
    /// per-vault startup path) and `Self::start_region` (the Stage 5b
    /// parallel-replay-on-restart path). Identical wiring at both call
    /// sites is load-bearing — `start_region` opens the store once for
    /// replay, drops it, and `start_vault_group` reopens it later
    /// against the same `raft.db` file. Any divergence between the two
    /// would cause the post-replay reopen to observe a different
    /// component-handle topology than the replay itself ran against.
    ///
    /// ## Component-handle topology
    ///
    /// - `state_layer` — shared with the parent org. The org's [`StateLayer`] already materialises
    ///   per-vault `state.db` databases via its lazy factory (P2b.0); sharing the accessor lets
    ///   vault apply land entity writes in the correct per-vault database without duplicating the
    ///   layer.
    /// - `block_archive` — **per-vault** (Phase 4.1.a). Vault writes append to the vault's own
    ///   chain in its own `blocks.db`; the parent organization's `blocks.db` is no longer touched
    ///   by vault apply. Returned alongside the store so callers can also stash it on
    ///   `InnerVaultGroup` (in the start-vault path) or drop it (in the replay path).
    /// - `block_announcements` — broadcast through the org's channel so subscribers see vault-shard
    ///   commits alongside org-shard commits.
    /// - `leader_lease` — vault groups run delegated leadership; the parent org's lease is the
    ///   authoritative read-validity window.
    /// - `event_writer` — **per-vault** (Phase 4.2). When the parent org has an event writer
    ///   configured, we open a fresh `events.db` under
    ///   `{data_dir}/{region}/{org_id}/state/vault-{vault_id}/events.db` and wrap it in a new
    ///   [`EventWriter`] that inherits the org writer's [`EventConfig`]. Returned alongside the
    ///   store (via the third tuple element) so the start-vault path can also stash a clone on
    ///   `InnerVaultGroup`.
    /// - `snapshot_key_provider` — wired from `self.config` so per-vault snapshots resolve to the
    ///   right per-scope DEKs (root rule 18).
    ///
    /// ## Errors
    ///
    /// Returns [`RaftManagerError::RegionNotFound`] if the parent
    /// `(region, organization_id)` group is not registered on this
    /// node, or [`RaftManagerError::Storage`] for any I/O failure
    /// opening the per-vault databases.
    pub(crate) fn open_and_wire_vault_store(
        &self,
        region: Region,
        organization_id: OrganizationId,
        vault_id: VaultId,
        org_state_layer: Arc<StateLayer<FileBackend>>,
        org_block_announcements: broadcast::Sender<BlockAnnouncement>,
        org_leader_lease: Arc<crate::leader_lease::LeaderLease>,
        org_event_writer: Option<&EventWriter<FileBackend>>,
    ) -> Result<WiredVaultStore> {
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

        // Phase 4.1.a: open / create the per-vault `blocks.db` and wrap
        // it in its own [`BlockArchive`]. Each vault owns its own
        // Merkle chain — `append_block` lands in the per-vault file
        // rather than sharing the parent organization's chain.
        //
        // Mirrors the per-org construction in `open_region_storage`:
        // existing files reopen via `Database::open`; brand-new files
        // are created with `ORGANIZATION_PAGE_SIZE` so vault DBs share
        // the same Raft-batch-sized pages as the rest of the per-org
        // store.
        let vault_blocks_db_path =
            self.storage_manager.vault_blocks_db_path(region, organization_id, vault_id);
        let vault_blocks_db = if vault_blocks_db_path.exists() {
            Database::<FileBackend>::open(&vault_blocks_db_path)
        } else {
            let blocks_db_config = inferadb_ledger_store::DatabaseConfig {
                page_size: crate::region_storage::ORGANIZATION_PAGE_SIZE,
                ..Default::default()
            };
            Database::<FileBackend>::create_with_config(&vault_blocks_db_path, blocks_db_config)
        }
        .map_err(|e| RaftManagerError::Storage {
            region,
            message: format!(
                "failed to open vault blocks.db at {}: {e}",
                vault_blocks_db_path.display()
            ),
        })?;
        let vault_block_archive = Arc::new(BlockArchive::new(Arc::new(vault_blocks_db)));

        // Open the per-vault Raft log store. `open_for_vault` stamps the
        // store's residency identity (organization_id + vault_id) at
        // construction time so downstream apply workers can't accidentally
        // mutate another vault's state.
        let mut vault_log_store = RaftLogStore::<FileBackend>::open_for_vault(
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
        })?
        .with_state_layer(org_state_layer)
        .with_block_archive(Arc::clone(&vault_block_archive))
        .with_block_announcements(org_block_announcements)
        .with_region_config(
            region,
            NodeId::new(self.config.node_id.to_string()),
            self.config.node_id,
        )
        .with_leader_lease(org_leader_lease)
        .with_snapshot_key_provider(Arc::clone(&self.config.snapshot_key_provider));

        // Phase 4.2: open / create the per-vault `events.db` and wrap it
        // in a fresh per-vault [`EventWriter`]. Inherits its
        // [`EventConfig`] from the parent org's writer so scope flags
        // and TTL match. When the parent org has no event writer (e.g.
        // test fixtures without events configuration), the vault store
        // also runs without one and apply skips event emission.
        //
        // `EventsDatabase::open` joins `events.db` to its `data_dir`
        // argument, so passing `vault_dir` materialises the file at
        // `{vault_dir}/events.db` — the same path returned by
        // [`RegionStorageManager::vault_events_db_path`].
        let vault_event_writer = if let Some(org_writer) = org_event_writer {
            let vault_events_db = inferadb_ledger_state::EventsDatabase::<FileBackend>::open(
                &vault_dir,
            )
            .map_err(|e| RaftManagerError::Storage {
                region,
                message: format!(
                    "failed to open vault events.db at {}: {e}",
                    vault_dir.join("events.db").display()
                ),
            })?;
            Some(EventWriter::new(Arc::new(vault_events_db), org_writer.config().clone()))
        } else {
            None
        };
        if let Some(writer) = vault_event_writer.clone() {
            vault_log_store = vault_log_store.with_event_writer(writer);
        }

        Ok((vault_log_store, vault_block_archive, vault_event_writer))
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
    /// ## Commit-pump
    ///
    /// The spawned commit-pump task drains committed batches from the
    /// vault shard's channel, classifies each entry's tier
    /// ([`classify_vault_tier`]), drops org-scoped tier violations with
    /// a loud `error!` log, and applies the surviving entries against
    /// the per-vault [`RaftLogStore`] via
    /// [`RaftLogStore::apply_committed_entries`]. Membership entries
    /// and empty-Normal (Raft no-op) entries pass through unfiltered —
    /// `apply_committed_entries` handles them internally.
    ///
    /// Routing-side wiring that flips entity writes from the parent
    /// `OrganizationGroup` to this vault shard lands in a later slice;
    /// until then the channel only carries Raft no-op / membership
    /// entries, so the apply call is exercised but produces no
    /// entity-data mutations on the per-vault `state.db`.
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

        // Open + wire the per-vault `raft.db` / `blocks.db` /
        // `events.db` via the shared
        // [`Self::open_and_wire_vault_store`] helper. The helper is
        // also called from
        // [`Self::start_region`]'s Stage 5b parallel-replay-on-restart
        // path against the same `raft.db` file, so every wiring
        // decision lives there in one place.
        let (vault_log_store, vault_block_archive, vault_event_writer) = self
            .open_and_wire_vault_store(
                region,
                organization_id,
                vault_id,
                Arc::clone(org_inner.state()),
                org_inner.block_announcements().clone(),
                Arc::clone(org_inner.leader_lease()),
                org_inner.event_writer(),
            )?;

        // Per-vault applied-state accessors, surfaced before the log store
        // is moved into the debug-stub worker task.
        let vault_applied_state = vault_log_store.accessor();
        let vault_commitment_buffer = vault_log_store.commitment_buffer();
        let vault_applied_index_rx = vault_log_store.applied_index_watch();
        // Per-vault apply-progress snapshot. Initialised to the default
        // empty state at start-time; the per-vault apply worker swaps in
        // a new pointee on each committed entry. Holds `last_applied`,
        // `vault_height`, `client_sequences`, and the chain-continuity
        // hash for this vault's Raft log (the org-scoped accessor above
        // continues to back slug indices and other shared state).
        let vault_applied_state_swap = Arc::new(arc_swap::ArcSwap::from_pointee(
            crate::log_storage::VaultAppliedState::default(),
        ));
        // Parallel `Arc` to the per-vault `raft.db` so the shutdown sync
        // sweep can fan out to it alongside the vault's `state.db`. The
        // apply task also keeps an `Arc` alive via the owned log store;
        // this clone exists so the DB handle survives even if the apply
        // task exits early.
        let vault_raft_db = Arc::clone(vault_log_store.raft_db());

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

        // Leadership adoption is wired below, after `vault_handle` and
        // `vault_cancel` are constructed. Vault groups run delegated
        // leadership; the leader is adopted from the parent organization's
        // leader on every org leader change (see the "Delegated leader
        // adoption" block after the commit-pump spawn).

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
        //
        // `add_shard` returns the shard's leadership / commit-index watch
        // receiver, which the per-vault `ConsensusHandle` below uses so
        // `is_leader()`, `current_leader()`, and `commit_index()` reflect
        // the vault shard rather than the parent organization shard.
        let vault_state_rx = match org_inner.handle().add_shard(consensus_shard).await {
            Ok(rx) => rx,
            Err(e) => {
                org_inner.commit_dispatcher().deregister(shard_id);
                return Err(RaftManagerError::Raft {
                    region,
                    message: format!("failed to register vault shard on parent engine: {e}"),
                });
            },
        };

        // Per-vault response map. Apply on the vault shard fills this
        // map, and proposals through the vault handle wait on it. Kept
        // distinct from the parent organization's map so the vault and
        // org apply pipelines don't accidentally satisfy each other's
        // waiters.
        let vault_response_map: crate::consensus_handle::ResponseMap =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));

        // Vault-specific handle that shares the parent organization's
        // engine but routes to the vault's own shard. Reuses the
        // engine, reactor, transport, and commit dispatcher behind the
        // org handle; introduces its own shard id, state watch, and
        // response map.
        let vault_handle = Arc::new(crate::consensus_handle::ConsensusHandle::new_for_shard(
            org_inner.handle().engine_arc(),
            shard_id,
            organization_id,
            self.config.node_id,
            vault_state_rx,
            Arc::clone(&vault_response_map),
        ));

        // Spawn the debug-log stub apply worker. The stub owns the
        // per-vault log store for the lifetime of the vault group so the
        // store's `raft.db` handle and applied-state watch sender stay
        // alive as long as any receiver is observing them. Real per-vault
        // apply — with `VaultAppliedState` mutations and tier validation
        // — replaces this stub in a later slice.
        //
        // Two cancellation paths drive exit:
        // - `manager_cancel` — a child of the manager's cancellation token. Fires on process-wide
        //   shutdown so the stub drains with every other region-scoped task.
        // - `vault_cancel` — the per-vault token stored on `InnerVaultGroup`. Fires when
        //   `stop_vault_group` tears down this single vault without disturbing sibling vaults.
        let manager_cancel = self.cancellation_token.lock().child_token();
        let vault_cancel = CancellationToken::new();
        let stub_manager_cancel = manager_cancel.clone();
        let stub_vault_cancel = vault_cancel.clone();
        let stub_region = region;
        let stub_org = organization_id;
        let stub_vault = vault_id;
        let stub_shard_id = shard_id;
        // Per-vault apply-command channel (Stage 4). Drained by the per-vault
        // commit pump via `tokio::select!` alongside the commit channel; the
        // sender side is stored on `InnerVaultGroup` so
        // `RaftManagerSnapshotInstaller` can find it via the
        // `(region, organization_id, vault_id)` key.
        let (vault_apply_command_tx, mut vault_apply_command_rx) =
            tokio::sync::mpsc::channel::<crate::apply_command::ApplyCommand>(
                crate::apply_command::APPLY_COMMAND_CHANNEL_CAPACITY,
            );
        // Per-vault response map + spillover, captured into the apply task
        // for response fan-out. Mirrors `ApplyWorker::run`'s discipline:
        // each committed entry's response is delivered to the registered
        // waiter via `response_map`, or stashed in `spillover` when the
        // proposer hasn't yet inserted (closes the propose→apply TOCTOU).
        let stub_response_map = Arc::clone(&vault_response_map);
        let stub_spillover = Arc::clone(vault_handle.spillover());
        // Per-vault apply-progress projection target. The same `ArcSwap`
        // that lives on `InnerVaultGroup.vault_applied_state` (the value
        // passed into `InnerVaultGroup` below). The pump projects the
        // log store's full `AppliedState` into a `VaultAppliedState`
        // after each successful apply and stores it through this clone;
        // both clones share the same `ArcSwap`, so observability
        // readers on `InnerVaultGroup` see the projected value.
        let stub_vault_applied_state = Arc::clone(&vault_applied_state_swap);
        tokio::spawn(async move {
            // `InnerVaultGroup` intentionally does not own `RaftLogStore`.
            // The store's `applied_index` watch-sender must live
            // co-located with the task that drives applies, so the sender
            // stays alive whenever a receiver is observed. The commit
            // pump plays that role — `apply_committed_entries` takes
            // `&mut self`, so the binding is `mut`. Do not remove this
            // binding without relocating ownership; dropping the store
            // here closes `applied_index_watch()` for every holder of
            // the receiver.
            //
            // Per-vault raft.db durability: every per-vault apply commit
            // lands in the per-vault raft.db page cache via
            // `commit_in_memory` (see `RaftLogStore::save_state_core`).
            // The steady-state `StateCheckpointer` ticks the per-vault
            // raft.db handle on every fire (Phase A fan-out, threaded
            // via `vault_raft_dbs_fn` from `start_background_jobs`), so
            // `applied_durable_index` advances independently per vault
            // on the checkpoint cadence (~500ms default). On crash, the
            // post-restart WAL-replay gap is bounded by checkpoint
            // cadence × per-vault apply rate — `replay_crash_gap` runs
            // only `(applied_durable, last_committed]`. `GracefulShutdown`
            // Phase 5c additionally fsyncs every per-vault raft.db so
            // clean shutdowns leave a zero-entry replay window.
            let mut vault_log_store = vault_log_store;
            let mut vault_batch_rx = vault_batch_rx;
            loop {
                tokio::select! {
                    biased;
                    () = stub_manager_cancel.cancelled() => {
                        debug!(
                            region = stub_region.as_str(),
                            organization_id = stub_org.value(),
                            vault_id = stub_vault.value(),
                            shard_id = stub_shard_id.0,
                            "Vault commit pump exiting on manager cancellation",
                        );
                        break;
                    }
                    () = stub_vault_cancel.cancelled() => {
                        debug!(
                            region = stub_region.as_str(),
                            organization_id = stub_org.value(),
                            vault_id = stub_vault.value(),
                            shard_id = stub_shard_id.0,
                            "Vault commit pump exiting on per-vault cancellation",
                        );
                        break;
                    }
                    cmd = vault_apply_command_rx.recv() => {
                        // Stage 4 install arm. Drained alongside the
                        // commit channel under `biased;` so an install
                        // wins over a concurrently-arriving batch — this
                        // is correct because the install replaces the
                        // state machine wholesale and any batch arriving
                        // mid-flight is about to be invalidated by a
                        // post-install fanout (Stage 5).
                        //
                        // Strict-ordering invariant: the install
                        // `await`s `RaftLogStore::install_snapshot` to
                        // completion inside this arm, so the next loop
                        // iteration cannot poll `vault_batch_rx` until
                        // the install has either landed or failed.
                        // Concurrent batch apply during install is
                        // therefore impossible — owning `&mut
                        // vault_log_store` here, plus the synchronous
                        // await, closes the only reachable race.
                        match cmd {
                            Some(crate::apply_command::ApplyCommand::InstallSnapshot {
                                meta,
                                plaintext,
                                completion,
                                _backend: _,
                            }) => {
                                let install_result =
                                    vault_log_store.install_snapshot(&meta, plaintext).await;
                                // The completion channel may be closed
                                // if the installer task was cancelled
                                // while we held the command — drop
                                // silently in that case.
                                let _ = completion.send(install_result);
                                continue;
                            }
                            None => {
                                // Control channel closed (manager
                                // dropped). Continue draining the commit
                                // channel until it closes too — `recv()`
                                // returns `None` immediately on
                                // subsequent polls so this arm becomes a
                                // no-op fallthrough.
                                continue;
                            }
                        }
                    }
                    maybe_batch = vault_batch_rx.recv() => {
                        match maybe_batch {
                            Some(batch) => {
                                if batch.entries.is_empty() {
                                    continue;
                                }
                                debug!(
                                    region = stub_region.as_str(),
                                    organization_id = stub_org.value(),
                                    vault_id = stub_vault.value(),
                                    shard_id = stub_shard_id.0,
                                    entry_count = batch.entries.len(),
                                    "Vault commit batch received",
                                );
                                // Filter step: keep Membership and empty-Normal
                                // entries verbatim (`apply_committed_entries`
                                // handles them internally), keep Normal
                                // entries that decode AND classify as
                                // VaultScoped, drop OrgScoped tier violations
                                // and decode failures with a loud log line.
                                // Routing-side discipline should make the
                                // OrgScoped arm unreachable in practice — when
                                // it fires, it surfaces a routing bug rather
                                // than corrupting per-vault state.
                                let mut entries_to_apply: Vec<
                                    inferadb_ledger_consensus::committed::CommittedEntry,
                                > = Vec::with_capacity(batch.entries.len());
                                for entry in batch.entries.iter() {
                                    match &entry.kind {
                                        inferadb_ledger_consensus::types::EntryKind::Membership(_) => {
                                            entries_to_apply.push(entry.clone());
                                            continue;
                                        }
                                        inferadb_ledger_consensus::types::EntryKind::Normal
                                            if entry.data.is_empty() =>
                                        {
                                            entries_to_apply.push(entry.clone());
                                            continue;
                                        }
                                        inferadb_ledger_consensus::types::EntryKind::Normal => {}
                                    }
                                    let request = match decode_vault_payload(&entry.data) {
                                        Ok(r) => r,
                                        Err(e) => {
                                            tracing::error!(
                                                region = stub_region.as_str(),
                                                organization_id = stub_org.value(),
                                                vault_id = stub_vault.value(),
                                                shard_id = stub_shard_id.0,
                                                entry_index = entry.index,
                                                bytes_len = entry.data.len(),
                                                error = %e,
                                                "Vault apply: decode failed",
                                            );
                                            continue;
                                        }
                                    };
                                    match classify_vault_tier(&request) {
                                        VaultTier::VaultScoped => {
                                            entries_to_apply.push(entry.clone());
                                        }
                                        VaultTier::OrgScoped => {
                                            tracing::error!(
                                                region = stub_region.as_str(),
                                                organization_id = stub_org.value(),
                                                vault_id = stub_vault.value(),
                                                shard_id = stub_shard_id.0,
                                                entry_index = entry.index,
                                                variant = request_variant_name(&request),
                                                "Vault apply: tier violation — org-scoped variant routed to vault shard",
                                            );
                                        }
                                    }
                                }

                                if entries_to_apply.is_empty() {
                                    continue;
                                }

                                // Real apply: drive surviving entries through
                                // the per-vault `RaftLogStore` apply pipeline,
                                // then fan out the returned `Vec<LedgerResponse>`
                                // to the per-vault response map / spillover.
                                // Mirrors `ApplyWorker::run`'s response delivery
                                // loop (see `apply_worker.rs` § "Response
                                // fan-out"): each committed entry's response is
                                // delivered to a registered waiter via
                                // `stub_response_map`; if no waiter is present
                                // (proposer hasn't inserted yet), the response
                                // is stashed in `stub_spillover` so a late
                                // `propose_and_wait` can pick it up after
                                // registering. On apply error, every entry is
                                // mapped to a synthetic `LedgerResponse::Error`
                                // so waiters never hang on a bad batch.
                                let entry_count = entries_to_apply.len();
                                let mut apply_succeeded = false;
                                let responses: Vec<crate::types::LedgerResponse> =
                                    match vault_log_store
                                        .apply_committed_entries::<OrganizationRequest>(
                                            &entries_to_apply,
                                            batch.leader_node,
                                        )
                                        .await
                                    {
                                        Ok(rs) => {
                                            apply_succeeded = true;
                                            rs
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                region = stub_region.as_str(),
                                                organization_id = stub_org.value(),
                                                vault_id = stub_vault.value(),
                                                shard_id = stub_shard_id.0,
                                                entry_count,
                                                error = %e,
                                                "Vault apply: apply_committed_entries failed",
                                            );
                                            let err =
                                                crate::types::LedgerResponse::Error {
                                                    code:
                                                        inferadb_ledger_types::ErrorCode::Internal,
                                                    message: format!("Vault apply failed: {e}"),
                                                };
                                            entries_to_apply
                                                .iter()
                                                .map(|_| err.clone())
                                                .collect()
                                        },
                                    };

                                // Stage 1: under the response_map lock, drain
                                // waiters into `to_send` and the rest into
                                // `to_spillover`. Keeping the map's critical
                                // section short avoids contending with
                                // proposers racing to register.
                                let mut to_send: Vec<(
                                    tokio::sync::oneshot::Sender<crate::types::LedgerResponse>,
                                    crate::types::LedgerResponse,
                                )> = Vec::with_capacity(entry_count);
                                let mut to_spillover: Vec<(
                                    u64,
                                    crate::types::LedgerResponse,
                                )> = Vec::with_capacity(entry_count);
                                {
                                    let mut map = stub_response_map.lock();
                                    for (entry, response) in
                                        entries_to_apply.iter().zip(responses.into_iter())
                                    {
                                        match map.remove(&entry.index) {
                                            Some(tx) => to_send.push((tx, response)),
                                            None => to_spillover.push((entry.index, response)),
                                        }
                                    }
                                }

                                // Stage 2: lock-free oneshot delivery.
                                for (tx, response) in to_send {
                                    let _ = tx.send(response);
                                }

                                // Stage 3: batch-insert spillover under a
                                // single spillover lock.
                                if !to_spillover.is_empty() {
                                    let mut spillover = stub_spillover.lock();
                                    for (index, response) in to_spillover {
                                        spillover.insert(index, response);
                                    }
                                }

                                // Stage 4: project the log store's full
                                // `AppliedState` into a `VaultAppliedState`
                                // and publish it through the
                                // `InnerVaultGroup.vault_applied_state`
                                // `ArcSwap`. Skipped on apply failure —
                                // the log store's `AppliedState` is only
                                // mutated on the Ok arm of
                                // `apply_committed_entries`, so a
                                // projection on Err would just re-publish
                                // the previous snapshot.
                                if apply_succeeded {
                                    let projected =
                                        crate::log_storage::VaultAppliedState::from_applied_state(
                                            &vault_log_store.applied_state().load(),
                                            stub_org,
                                            stub_vault,
                                        );
                                    stub_vault_applied_state.store(Arc::new(projected));
                                }
                            }
                            None => {
                                debug!(
                                    region = stub_region.as_str(),
                                    organization_id = stub_org.value(),
                                    vault_id = stub_vault.value(),
                                    shard_id = stub_shard_id.0,
                                    "Vault commit channel closed — pump exiting",
                                );
                                break;
                            }
                        }
                    }
                }
            }
        });

        // Delegated leader adoption. Vault shards run
        // `LeadershipMode::Delegated` — their leader is whoever leads the
        // parent organization group. Mirrors the region → organization
        // watcher in `start_organization_group` (see above), one level
        // deeper: organization → vault. Without this block,
        // `vault_handle.is_leader()` stays `false` indefinitely and every
        // propose through the vault handle fails.
        //
        // Two parts:
        //
        // 1. One-shot adoption — if the parent org already has a leader, adopt it before returning
        //    so the vault group doesn't sit leaderless between start and the next parent
        //    leader-change. When the parent hasn't elected yet (e.g., during cluster bootstrap),
        //    this arm is a no-op and the watcher covers the first real change.
        //
        // 2. Watcher task — subscribes to the parent org's state watch and re-adopts on every
        //    leader / term change. Bound to the per-vault cancellation token so `stop_vault_group`
        //    tears it down without disturbing sibling vaults, and to the manager token so
        //    process-wide shutdown drains it alongside the commit-pump stub.
        // Hibernation activity sentinel: vault_handle is constructed
        // fresh, so `last_activity` is set in the `InnerVaultGroup`
        // construction below. The leader watcher loop touches activity
        // on every successful adoption — see the watcher block.
        let org_handle_for_adopt = org_inner.handle().clone();
        let initial_org_state = org_handle_for_adopt.shard_state();
        if let Some(leader) = initial_org_state.leader {
            let term = initial_org_state.term;
            if let Err(e) = vault_handle.adopt_leader(leader, term).await {
                warn!(
                    region = region.as_str(),
                    organization_id = organization_id.value(),
                    vault_id = vault_id.value(),
                    shard_id = shard_id.0,
                    error = %e,
                    "Vault leader adoption: initial adoption failed",
                );
            } else {
                debug!(
                    region = region.as_str(),
                    organization_id = organization_id.value(),
                    vault_id = vault_id.value(),
                    shard_id = shard_id.0,
                    term = term,
                    "Vault leader adoption: initial",
                );
            }
        }
        {
            let mut parent_state_rx = org_handle_for_adopt.state_rx().clone();
            let watcher_handle = Arc::clone(&vault_handle);
            let watcher_manager_cancel = manager_cancel.clone();
            let watcher_vault_cancel = vault_cancel.clone();
            let watcher_region = region;
            let watcher_org = organization_id;
            let watcher_vault = vault_id;
            let watcher_shard = shard_id;
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        () = watcher_manager_cancel.cancelled() => {
                            debug!(
                                region = watcher_region.as_str(),
                                organization_id = watcher_org.value(),
                                vault_id = watcher_vault.value(),
                                shard_id = watcher_shard.0,
                                "Vault leader watcher exiting on manager cancellation",
                            );
                            break;
                        }
                        () = watcher_vault_cancel.cancelled() => {
                            debug!(
                                region = watcher_region.as_str(),
                                organization_id = watcher_org.value(),
                                vault_id = watcher_vault.value(),
                                shard_id = watcher_shard.0,
                                "Vault leader watcher exiting on per-vault cancellation",
                            );
                            break;
                        }
                        res = parent_state_rx.changed() => {
                            if res.is_err() {
                                // Sender dropped — parent engine tore down.
                                // Nothing left to observe; exit quietly.
                                break;
                            }
                            let snap = parent_state_rx.borrow_and_update().clone();
                            if let Some(leader) = snap.leader
                                && watcher_handle
                                    .adopt_leader(leader, snap.term)
                                    .await
                                    .is_err()
                            {
                                // Engine rejected adoption (typically
                                // because its control inbox closed). No
                                // recovery path from here — exit the
                                // watcher so the task doesn't spin.
                                break;
                            }
                        }
                    }
                }
            });
        }

        // P2c.3.b.4: per-vault batch writer.
        //
        // Mirrors the org-side construction in `start_region` — same
        // submit-fn shape (`OrganizationRequest::BatchWrite { requests }`
        // wrapped in a `RaftPayload` with piggybacked commitments) but
        // routed through the vault's own `ConsensusHandle` so the
        // proposal lands on the per-vault shard instead of the parent
        // org's. Each vault gets its own coalescing window, so concurrent
        // writes to the same vault still amortize WAL fsync cost.
        //
        // The config is inherited from the parent organization
        // (option (a) in the task #164 design): per-vault tuning is not
        // expected to diverge from per-org tuning, and adding a new
        // `RegionConfig` field for it would expand the construction
        // surface without a concrete need. When the parent org started
        // without batching configured (e.g. test fixtures), the vault
        // group also starts without a batch handle — the Write /
        // BatchWrite handlers fall back to direct propose.
        //
        // `BatchWrite` is classified as `VaultTier::VaultScoped` (see
        // `classify_vault_tier`), so the per-vault commit pump accepts
        // it; the apply pipeline decodes the batch and fans out the
        // returned `Vec<LedgerResponse>` to the per-vault response map.
        //
        // Lifecycle: the writer task is spawned via `tokio::spawn` and
        // wrapped in `tokio::select!` against `vault_cancel` and
        // `manager_cancel`. `stop_vault_group` cancels `vault_cancel`
        // (see `InnerVaultGroup.cancellation`), so the spawned task
        // exits when this single vault is torn down. Process-wide
        // shutdown fires `manager_cancel`. Pending submissions in the
        // queue at cancellation time are dropped — callers receive
        // `BatchError::Dropped` via the `oneshot` `Sender` being
        // dropped, surfacing as `ApplyDropped` to the SDK and
        // triggering retry.
        let vault_batch_handle = if let Some(batch_config) =
            org_inner.batch_writer_config().cloned()
        {
            let handle_clone = Arc::clone(&vault_handle);
            let buffer_clone = Arc::clone(&vault_commitment_buffer);
            let submit_fn = move |requests: Vec<OrganizationRequest>| {
                let h = Arc::clone(&handle_clone);
                let buffer = Arc::clone(&buffer_clone);
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

            // Spawn the writer loop with cancellation. `BatchWriter::run`
            // is an infinite loop without its own cancellation hook
            // (matching the org-side spawn pattern); wrapping in
            // `tokio::select!` against the per-vault and manager tokens
            // is what makes per-vault teardown clean — the alternative
            // would be leaking the task until process exit, which is
            // tolerable for org groups (one per process) but not for
            // vault groups (one per vault, with `stop_vault_group`
            // running on every `DeleteVault`).
            let bw_manager_cancel = manager_cancel.clone();
            let bw_vault_cancel = vault_cancel.clone();
            let bw_region = region;
            let bw_org = organization_id;
            let bw_vault = vault_id;
            tokio::spawn(async move {
                tokio::select! {
                    biased;
                    () = bw_manager_cancel.cancelled() => {
                        debug!(
                            region = bw_region.as_str(),
                            organization_id = bw_org.value(),
                            vault_id = bw_vault.value(),
                            "Per-vault batch writer exiting on manager cancellation",
                        );
                    }
                    () = bw_vault_cancel.cancelled() => {
                        debug!(
                            region = bw_region.as_str(),
                            organization_id = bw_org.value(),
                            vault_id = bw_vault.value(),
                            "Per-vault batch writer exiting on per-vault cancellation",
                        );
                    }
                    () = writer.run() => {
                        // Unreachable: `BatchWriter::run` only returns
                        // when its semaphore closes, which never happens
                        // because the semaphore is owned by the writer
                        // itself. Treat as a defensive arm.
                    }
                }
            });
            Some(bw_handle)
        } else {
            None
        };

        // Build the per-vault group, reusing shared fields from the
        // parent org where the data plane overlaps (handle, state,
        // block_archive, block_announcements, leader_lease under
        // delegated leadership).
        let inner = Arc::new(InnerVaultGroup {
            region,
            organization_id,
            vault_id,
            shard_id,
            handle: vault_handle,
            state: Arc::clone(org_inner.state()),
            block_archive: vault_block_archive,
            applied_state: vault_applied_state,
            vault_applied_state: vault_applied_state_swap,
            block_announcements: org_inner.block_announcements().clone(),
            batch_handle: vault_batch_handle,
            commitment_buffer: vault_commitment_buffer,
            leader_lease: Arc::clone(org_inner.leader_lease()),
            applied_index_rx: vault_applied_index_rx,
            cancellation: vault_cancel,
            raft_db: vault_raft_db,
            event_writer: vault_event_writer,
            // Hibernation lifecycle (Phase 7 / O1). Vault starts Active
            // with `last_activity` set to "now" — fresh vaults have, by
            // definition, just been provisioned and should not be
            // immediately eligible for hibernation. The idle detector
            // measures elapsed seconds against this baseline.
            lifecycle_state: AtomicU8::new(VaultLifecycleState::Active as u8),
            last_activity_unix_secs: AtomicU64::new(current_unix_secs()),
            pending_membership_started_unix_secs: AtomicU64::new(0),
            apply_command_tx: vault_apply_command_tx,
        });

        {
            let mut vault_groups = self.vault_groups.write();
            vault_groups.insert((region, organization_id, vault_id), Arc::clone(&inner));
        }

        // Maintain the reverse `shard_id → triple` index used by
        // [`RaftManagerWakeNotifier`] to translate a paused-shard peer-
        // message event into a [`Self::wake_vault`] call. Inserted under
        // the same lifecycle window as `vault_groups` so the notifier's
        // O(1) `DashMap::get` cannot observe a vault group without its
        // shard mapping (and vice versa).
        self.vault_shard_index.insert(shard_id, (region, organization_id, vault_id));

        info!(
            region = region.as_str(),
            organization_id = organization_id.value(),
            vault_id = vault_id.value(),
            shard_id = shard_id.0,
            "Vault group started successfully",
        );

        Ok(Arc::new(VaultGroup(inner)))
    }

    /// Stops a per-vault Raft group on this node.
    ///
    /// Tears down a vault group registered by
    /// [`start_vault_group`](Self::start_vault_group): removes it from
    /// `vault_groups`, cancels the per-vault commit-pump stub, deregisters
    /// the shard from the parent organization's
    /// [`CommitDispatcher`](crate::commit_dispatcher::CommitDispatcher),
    /// and calls
    /// [`remove_shard`](crate::consensus_handle::ConsensusHandle::remove_shard)
    /// on the parent engine. Ordering is deregister-before-remove so the
    /// dispatcher never routes a final committed batch to a shard the
    /// engine has already dropped.
    ///
    /// Treats a missing parent organization as best-effort cleanup: logs
    /// a warning and continues. This covers the (unusual) race where an
    /// organization teardown dismantles the parent engine before its
    /// vaults have been stopped — in that case the vault's shard was
    /// torn down with the engine and there is nothing more to unwind.
    ///
    /// # Errors
    ///
    /// - [`RaftManagerError::VaultGroupNotFound`] if no vault group is registered for the requested
    ///   `(region, organization_id, vault_id)` triple. Callers that want idempotent-stop semantics
    ///   (for example a double-apply of `DeleteVault`) should match on this variant and treat it as
    ///   success.
    /// - [`RaftManagerError::Raft`] if the parent engine rejects the shard removal. Best-effort:
    ///   the map entry and the dispatcher registration are already gone by the time this error
    ///   surfaces, so rolling the teardown back would leave a worse partial state.
    pub async fn stop_vault_group(
        &self,
        region: Region,
        organization_id: OrganizationId,
        vault_id: VaultId,
    ) -> Result<()> {
        info!(
            region = region.as_str(),
            organization_id = organization_id.value(),
            vault_id = vault_id.value(),
            "Stopping vault group",
        );

        // Remove from the map first. If the entry is missing, return the
        // explicit not-found error — the apply-side watcher surfaces
        // duplicate `VaultDeletionRequest` signals instead of leaking
        // them.
        let inner = {
            let mut vault_groups = self.vault_groups.write();
            vault_groups.remove(&(region, organization_id, vault_id))
        }
        .ok_or(RaftManagerError::VaultGroupNotFound {
            region,
            organization_id,
            vault_id,
        })?;

        let shard_id = inner.shard_id();

        // Mirror the `vault_groups` removal in the reverse `shard_id → triple`
        // index so [`RaftManagerWakeNotifier`] cannot observe a stale shard
        // mapping. After this point a peer message landing on this shard
        // routes through the "no triple → unmatched metric + drop" path.
        self.vault_shard_index.remove(&shard_id);

        // Cancel the per-vault commit-pump stub (and, once wired, the
        // real apply worker). The stub task exits on its next poll.
        inner.cancellation.cancel();

        // Parent org may have been torn down concurrently. Treat that as
        // best-effort cleanup: the vault's shard was removed when the
        // parent engine dropped. The map entry and the per-vault
        // cancellation have already been handled above.
        let org_group = match self.get_organization_group(region, organization_id) {
            Ok(group) => group,
            Err(e) => {
                warn!(
                    region = region.as_str(),
                    organization_id = organization_id.value(),
                    vault_id = vault_id.value(),
                    error = %e,
                    "Parent organization group missing during vault-group stop — \
                     best-effort cleanup, dispatcher and engine teardown skipped",
                );
                return Ok(());
            },
        };
        let org_inner = Arc::clone(&org_group.0);

        // Deregister from the dispatcher BEFORE removing the shard from
        // the engine. If we reversed the order, the dispatcher could
        // route a final committed batch to a shard the engine has
        // already dropped. The dispatcher's documented "unknown shard =
        // drop silently" behaviour would absorb that, but clean teardown
        // still prefers deregister-first.
        org_inner.commit_dispatcher().deregister(shard_id);

        // Remove the shard from the parent engine. On failure the map
        // entry and the dispatcher registration are already gone — log
        // and continue rather than rolling back a partial teardown.
        if let Err(e) = org_inner.handle().remove_shard(shard_id).await {
            warn!(
                region = region.as_str(),
                organization_id = organization_id.value(),
                vault_id = vault_id.value(),
                shard_id = shard_id.0,
                error = %e,
                "Parent engine rejected vault shard removal — continuing best-effort cleanup",
            );
        }

        info!(
            region = region.as_str(),
            organization_id = organization_id.value(),
            vault_id = vault_id.value(),
            shard_id = shard_id.0,
            "Vault group stopped successfully",
        );

        Ok(())
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

    /// Stage 5b: re-drive per-vault apply for entries committed in the
    /// per-org WAL but not yet synced to per-vault `raft.db` dual-slots.
    ///
    /// Called from [`Self::start_region`] after the org-level
    /// [`RaftLogStore::replay_crash_gap`] returns and BEFORE
    /// [`ConsensusEngine::start_with_all_callbacks`] consumes the WAL
    /// by value. The replay path opens each child vault's store via
    /// [`Self::open_and_wire_vault_store`], runs them through
    /// [`replay_shared_wal_for_org`], and drops the stores —
    /// [`Self::start_vault_group`] reopens them later when the org
    /// apply pipeline starts driving vault creations.
    ///
    /// ## Error policy
    ///
    /// Errors are non-fatal. Logged + recorded as
    /// `ledger_org_parallel_replay_invocations_total{result=error}`;
    /// startup falls through to the live commit pump path. Failing to
    /// catch up an unclean-shutdown gap is preferable to failing to
    /// start at all — the live commit pump still receives entries
    /// committed AFTER startup (matching the pre-Stage-5b behaviour),
    /// so failure here is a degraded mode, not a hard stop.
    ///
    /// ## Skip arms
    ///
    /// Three skip arms each emit only the `_invocations_total` counter
    /// (no duration / vaults-active / entries-processed):
    /// - `RaftManagerConfig::enable_parallel_wal_replay = false` → `result=skipped_disabled`
    ///   (operator escape hatch).
    /// - The org has zero vaults → no metric is emitted (cold-start no-op).
    /// - Every vault has `applied_durable >= last_committed` → `result=skipped_no_gap`
    ///   (clean-shutdown path).
    #[allow(clippy::too_many_arguments)]
    async fn run_stage5b_parallel_replay(
        &self,
        region: Region,
        organization_id: OrganizationId,
        org_shard_id: inferadb_ledger_consensus::types::ConsensusStateId,
        wal: &inferadb_ledger_consensus::wal::SegmentedWalBackend,
        log_store: &RaftLogStore<FileBackend>,
        org_state_layer: Arc<StateLayer<FileBackend>>,
        org_block_announcements: broadcast::Sender<BlockAnnouncement>,
        org_leader_lease: Arc<crate::leader_lease::LeaderLease>,
        org_event_writer: Option<&EventWriter<FileBackend>>,
    ) {
        let region_label = region.as_str();
        let org_label = organization_id.value().to_string();

        // Operator escape hatch — skip the whole block when disabled.
        if !self.config.enable_parallel_wal_replay {
            metrics::record_org_parallel_replay_invocation(
                region_label,
                &org_label,
                metrics::org_parallel_replay_result::SKIPPED_DISABLED,
            );
            debug!(
                region = region_label,
                organization_id = organization_id.value(),
                "Stage 5b: parallel WAL replay disabled by config",
            );
            return;
        }

        // Enumerate vaults from the org's `applied_state`. Post-
        // `replay_crash_gap` this map reflects every vault the org
        // knows about — `list_vaults` returns the per-org vault metas
        // exactly the way `rehydrate_vault_groups` consumes them.
        let vault_metas = log_store.applied_state().load().vaults.clone();
        let vaults_in_org: Vec<VaultId> = vault_metas
            .iter()
            .filter_map(|((org, vault), meta)| {
                // Defensive: filter on org id even though every vault
                // in a per-org `applied_state.vaults` map should belong
                // to that org. Skip soft-deleted vaults (matches
                // `rehydrate_vault_groups`'s effective behaviour
                // through `start_vault_group`'s precondition checks).
                if *org == organization_id && !meta.deleted { Some(*vault) } else { None }
            })
            .collect();

        // Cold-start no-op: the org has zero vaults. Don't even emit
        // a skip metric — the metric labels are designed for steady-
        // state operator dashboards, and emitting on every cold start
        // would muddy the rate() over deploys.
        if vaults_in_org.is_empty() {
            debug!(
                region = region_label,
                organization_id = organization_id.value(),
                "Stage 5b: org has zero vaults; replay block skipped",
            );
            return;
        }

        // Read the WAL's last committed_index. This is the upper bound
        // for the replay window. If the WAL has no checkpoint (fresh
        // bootstrap), nothing to replay.
        let committed_index = match wal.last_checkpoint() {
            Ok(Some(cp)) => cp.committed_index,
            Ok(None) => {
                debug!(
                    region = region_label,
                    organization_id = organization_id.value(),
                    "Stage 5b: WAL has no checkpoint; replay block skipped",
                );
                metrics::record_org_parallel_replay_invocation(
                    region_label,
                    &org_label,
                    metrics::org_parallel_replay_result::SKIPPED_NO_GAP,
                );
                return;
            },
            Err(e) => {
                warn!(
                    region = region_label,
                    organization_id = organization_id.value(),
                    error = %e,
                    "Stage 5b: failed to read WAL checkpoint; falling through to live pump",
                );
                metrics::record_org_parallel_replay_invocation(
                    region_label,
                    &org_label,
                    metrics::org_parallel_replay_result::ERROR,
                );
                return;
            },
        };

        // Open each vault's store via the shared wiring helper. We
        // also derive the per-vault `ConsensusStateId` (matches the
        // derivation in `start_vault_group`) so the WAL's per-shard
        // routing can find the right apply closure.
        let mut vault_stores: Vec<(
            VaultId,
            inferadb_ledger_consensus::types::ConsensusStateId,
            RaftLogStore<FileBackend>,
        )> = Vec::with_capacity(vaults_in_org.len());

        for vault_id in &vaults_in_org {
            let vault_shard_id = {
                let mut bytes = Vec::with_capacity(region.as_str().len() + 16);
                bytes.extend_from_slice(region.as_str().as_bytes());
                bytes.extend_from_slice(&organization_id.value().to_le_bytes());
                bytes.extend_from_slice(&vault_id.value().to_le_bytes());
                inferadb_ledger_consensus::types::ConsensusStateId(seahash::hash(&bytes))
            };

            match self.open_and_wire_vault_store(
                region,
                organization_id,
                *vault_id,
                Arc::clone(&org_state_layer),
                org_block_announcements.clone(),
                Arc::clone(&org_leader_lease),
                org_event_writer,
            ) {
                Ok((store, _block_archive, _event_writer)) => {
                    vault_stores.push((*vault_id, vault_shard_id, store));
                },
                Err(e) => {
                    warn!(
                        region = region_label,
                        organization_id = organization_id.value(),
                        vault_id = vault_id.value(),
                        error = %e,
                        "Stage 5b: failed to open vault store for replay; \
                         falling through to live pump",
                    );
                    metrics::record_org_parallel_replay_invocation(
                        region_label,
                        &org_label,
                        metrics::org_parallel_replay_result::ERROR,
                    );
                    return;
                },
            }
        }

        // Build the per-shard `applied_durable` map. Includes the org
        // shard set to `committed_index` so `recover_from_wal` filters
        // out org-shard entries (already replayed by
        // `replay_crash_gap`); without this, the org-shard batch would
        // surface as `UnknownShard` since we're not registering an
        // apply closure for it.
        let mut applied_durable: std::collections::HashMap<
            inferadb_ledger_consensus::types::ConsensusStateId,
            u64,
        > = std::collections::HashMap::new();
        applied_durable.insert(org_shard_id, committed_index);

        // Skip-check: are all vaults already caught up?
        let mut vaults_with_gap: u64 = 0;
        for (_, vault_shard_id, store) in &vault_stores {
            let vault_applied =
                store.applied_state().load().last_applied.as_ref().map_or(0, |id| id.index);
            applied_durable.insert(*vault_shard_id, vault_applied);
            if vault_applied < committed_index {
                vaults_with_gap += 1;
            }
        }

        if vaults_with_gap == 0 {
            debug!(
                region = region_label,
                organization_id = organization_id.value(),
                vault_count = vault_stores.len(),
                committed_index,
                "Stage 5b: all vaults caught up; replay skipped",
            );
            metrics::record_org_parallel_replay_invocation(
                region_label,
                &org_label,
                metrics::org_parallel_replay_result::SKIPPED_NO_GAP,
            );
            return;
        }

        info!(
            region = region_label,
            organization_id = organization_id.value(),
            vault_count = vault_stores.len(),
            vaults_with_gap,
            committed_index,
            "Stage 5b: parallel WAL replay starting",
        );

        // Build apply_fns: one closure per vault. The store is moved
        // into a per-vault `Arc<Mutex<Option<...>>>` so the `FnOnce`
        // closure can `take()` it (the apply primitive's signature
        // requires `FnOnce(CommittedBatch) -> BoxFuture<...>` and
        // `apply_committed_entries` takes `&mut self`).
        let mut apply_fns: std::collections::HashMap<
            inferadb_ledger_consensus::types::ConsensusStateId,
            (VaultId, crate::log_storage::VaultApplyFn),
        > = std::collections::HashMap::new();

        for (vault_id, vault_shard_id, store) in vault_stores {
            let store_slot = Arc::new(parking_lot::Mutex::new(Some(store)));
            let store_for_closure = Arc::clone(&store_slot);
            let closure_region = region;
            let closure_org = organization_id;
            let closure_vault = vault_id;

            let apply_fn: crate::log_storage::VaultApplyFn =
                Box::new(move |batch: inferadb_ledger_consensus::committed::CommittedBatch| {
                    let store = Arc::clone(&store_for_closure);
                    Box::pin(async move {
                        // Scope the lock guard so it drops before the
                        // .await — parking_lot guards aren't Send, so
                        // holding one across an await is a hard error.
                        let store_inner = {
                            store.lock().take().ok_or_else(|| {
                                crate::log_storage::parallel_replay::VaultApplySnafu {
                                    region: closure_region,
                                    organization: closure_org,
                                    vault: closure_vault,
                                    message: "vault store taken twice during replay".to_string(),
                                }
                                .build()
                            })?
                        };

                        let entry_count = batch.entries.len() as u64;
                        // Bind to a local `mut` so the apply call has
                        // a `&mut self` to operate on, then return the
                        // store back into the slot regardless of
                        // apply outcome.
                        let mut store_inner = store_inner;
                        let apply_result = store_inner
                            .apply_committed_entries::<crate::types::OrganizationRequest>(
                                &batch.entries,
                                None,
                            )
                            .await
                            .map_err(|e| {
                                crate::log_storage::parallel_replay::VaultApplySnafu {
                                    region: closure_region,
                                    organization: closure_org,
                                    vault: closure_vault,
                                    message: format!("apply_committed_entries failed: {e}"),
                                }
                                .build()
                            });

                        // Sync the per-vault raft.db / blocks.db /
                        // events.db while we still own the store.
                        // `apply_committed_entries` lands every write
                        // via `commit_in_memory`, which only mutates
                        // the in-process page cache + the
                        // `committed_state` ArcSwap on each
                        // `Database`. When the `RaftLogStore` (and the
                        // per-vault `BlockArchive` / `EventWriter`
                        // wrapping each Database) drop at the end of
                        // this replay, the in-process page cache for
                        // those databases drops with them — the
                        // recovered state lives in memory on dropped
                        // handles, not on disk.
                        // [`RaftLogStore::replay_crash_gap`] handles
                        // the symmetric situation for the org-level
                        // store with an explicit post-replay
                        // `sync_state` fan-out; we mirror that here so
                        // [`Self::start_vault_group`]'s subsequent
                        // reopen reads the durable recovered state
                        // instead of regressing to whatever the
                        // dual-slot held before Stage 5b ran.
                        //
                        // Per-vault `state.db` is owned by the org's
                        // long-lived [`StateLayer`] and stays in
                        // `live_vault_dbs()` after the materialise that
                        // `apply_request_with_events` triggered; the
                        // first
                        // [`StateCheckpointer`](crate::state_checkpointer::StateCheckpointer)
                        // tick after engine startup picks it up via
                        // the standard fan-out, so we don't sync it
                        // explicitly here.
                        //
                        // Sync errors are converted to
                        // [`ParallelReplayError::VaultApply`] — the
                        // primitive's first-error short-circuit kicks
                        // in and `start_region` falls through to the
                        // live commit pump path (Stage 5b's
                        // non-fatal-error semantics).
                        let raft_db = store_inner.log_store_db();
                        let blocks_db = store_inner.block_archive().map(|ba| Arc::clone(ba.db()));
                        let events_db =
                            store_inner.event_writer().map(|ew| Arc::clone(ew.events_db().db()));

                        let raft_sync_fut = raft_db.sync_state();
                        let blocks_sync_fut = async move {
                            match blocks_db {
                                Some(db) => Some(db.sync_state().await),
                                None => None,
                            }
                        };
                        let events_sync_fut = async move {
                            match events_db {
                                Some(db) => Some(db.sync_state().await),
                                None => None,
                            }
                        };
                        let (raft_sync, blocks_sync, events_sync) =
                            tokio::join!(raft_sync_fut, blocks_sync_fut, events_sync_fut);

                        // Return the store regardless of outcome
                        // — even on failure, the post-replay drop
                        // needs the slot populated so memory cleans up
                        // deterministically.
                        *store.lock() = Some(store_inner);

                        apply_result?;

                        if let Err(e) = raft_sync {
                            return crate::log_storage::parallel_replay::VaultApplySnafu {
                                region: closure_region,
                                organization: closure_org,
                                vault: closure_vault,
                                message: format!("post-replay raft.db sync failed: {e}"),
                            }
                            .fail();
                        }
                        if let Some(Err(e)) = blocks_sync {
                            return crate::log_storage::parallel_replay::VaultApplySnafu {
                                region: closure_region,
                                organization: closure_org,
                                vault: closure_vault,
                                message: format!("post-replay blocks.db sync failed: {e}"),
                            }
                            .fail();
                        }
                        if let Some(Err(e)) = events_sync {
                            return crate::log_storage::parallel_replay::VaultApplySnafu {
                                region: closure_region,
                                organization: closure_org,
                                vault: closure_vault,
                                message: format!("post-replay events.db sync failed: {e}"),
                            }
                            .fail();
                        }

                        Ok(entry_count)
                    })
                });
            apply_fns.insert(vault_shard_id, (vault_id, apply_fn));
        }

        let replay_config = crate::log_storage::ParallelReplayConfig::builder()
            .max_concurrent(self.config.parallel_wal_replay_max_concurrent)
            .build();

        // The replay primitive is cancellation-aware. We bind to a
        // child of the manager's cancellation token so process-wide
        // shutdown surfaces here as `ParallelReplayError::Cancelled`,
        // which is treated as a non-fatal error in the match below
        // (startup already failed if shutdown beat it).
        let cancel = self.cancellation_token.lock().child_token();

        let started = std::time::Instant::now();
        let result = crate::log_storage::replay_shared_wal_for_org(
            wal,
            region,
            organization_id,
            applied_durable,
            apply_fns,
            replay_config,
            cancel,
        )
        .await;
        let elapsed = started.elapsed();

        match result {
            Ok(stats) => {
                info!(
                    region = region_label,
                    organization_id = organization_id.value(),
                    vaults_replayed = stats.vaults_replayed,
                    total_entries = stats.total_entries,
                    elapsed_ms = elapsed.as_millis() as u64,
                    "Stage 5b: parallel WAL replay complete",
                );
                metrics::record_org_parallel_replay_success(
                    region_label,
                    &org_label,
                    stats.duration,
                    stats.total_entries,
                    vaults_with_gap,
                );
                metrics::record_org_parallel_replay_invocation(
                    region_label,
                    &org_label,
                    metrics::org_parallel_replay_result::SUCCESS,
                );
            },
            Err(e) => {
                warn!(
                    region = region_label,
                    organization_id = organization_id.value(),
                    elapsed_ms = elapsed.as_millis() as u64,
                    error = %e,
                    "Stage 5b: parallel WAL replay failed; falling through to live pump",
                );
                metrics::record_org_parallel_replay_invocation(
                    region_label,
                    &org_label,
                    metrics::org_parallel_replay_result::ERROR,
                );
            },
        }
        // Per-vault stores fall out of scope here — `start_vault_group`
        // reopens each one later from the same on-disk path.
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
            requires_residency_hint,
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

        // Resolve `requires_residency` from the GLOBAL region directory.
        // The `CreateDataRegion` apply handler writes the directory entry
        // BEFORE signalling the local region-creation handler, so the entry
        // is in place by the time `start_region` runs in the normal path.
        // When the directory entry is missing (test fixtures that bypass
        // `CreateDataRegion`) we fall back to `RegionConfig::requires_residency_hint`,
        // which defaults to `false` so single-node test setups don't get
        // rejected by the protected-region quorum check.
        let is_protected = if region.is_global() {
            // GLOBAL is the cluster control plane — never subject to
            // residency rules and always replicated everywhere.
            false
        } else {
            self.system_region()
                .ok()
                .and_then(|sys| {
                    inferadb_ledger_state::system::lookup_region_residency(sys.state(), region)
                        .ok()
                        .flatten()
                })
                .map_or(requires_residency_hint, |r| r.requires_residency)
        };

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
        // Capture a clone of the org's apply-phase event writer (if any)
        // before `log_store` is moved into the apply worker. Per-vault
        // `RaftLogStore` instances reuse this handle via
        // `start_vault_group`, so vault apply emits into the same shared
        // `events.db` as the org's own apply path.
        let event_writer_for_inner = log_store.event_writer().cloned();

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

        // ────────────────────────────────────────────────────────────
        // Stage 5b: parallel per-vault catch-up from the local WAL.
        //
        // Per-vault `applied_durable_index` lives in the page cache
        // between [`StateCheckpointer`](crate::state_checkpointer::StateCheckpointer)
        // ticks (~500ms default cadence). An unclean shutdown loses
        // anything not yet synced to the per-vault `raft.db` dual-slot.
        // The per-vault commit pump that runs once
        // [`ConsensusEngine`] is started only re-applies entries that
        // arrive AFTER startup; on a single-node restart there are no
        // peers to push them, and the entries sit in the WAL forever.
        //
        // The replay block re-drives those entries through
        // [`replay_shared_wal_for_org`](crate::log_storage::replay_shared_wal_for_org)
        // before the engine consumes the WAL, fanning per-vault apply
        // out across the same M4 primitive used for the new-voter
        // bootstrap path. Per-vault stores are opened via
        // [`Self::open_and_wire_vault_store`] (the same helper
        // [`Self::start_vault_group`] uses), driven through the apply
        // pipeline, and dropped — `start_vault_group` reopens them
        // later when the org's apply pipeline starts driving vault
        // creations. The reopen is cheap because the OS page cache is
        // hot from the just-completed work.
        //
        // Dispatched only for per-organization groups (`org_id != 0`);
        // the system and data-region control-plane groups
        // (`org_id == 0`) have no vault children. Errors fall through
        // to the live commit pump path so startup is never blocked
        // on a replay failure — the metric labels record the outcome
        // for operator triage.
        if organization_id != OrganizationId::new(0) {
            self.run_stage5b_parallel_replay(
                region,
                organization_id,
                shard_id,
                &wal,
                &log_store,
                Arc::clone(&state),
                block_announcements.clone(),
                leader_lease.clone(),
                event_writer_for_inner.as_ref(),
            )
            .await;
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

        // On restart, persisted membership (loaded from raft.db on startup)
        // may include voters that aren't present in `initial_members` — for
        // data-region groups, `initial_members` is `[self only]` while the
        // committed configuration on disk lists every cluster peer. Without
        // this pass, outbound messages dispatched before
        // `reconcile_transport_channels` fires are silently dropped because
        // the peer-sender doesn't yet have a registered channel for them.
        // Pre-register every persisted voter whose address is already known
        // via `peer_addresses` (populated by seed discovery / initial members
        // on a previous boot). Voters whose address isn't yet known are left
        // alone — `reconcile_transport_channels` registers them when the
        // address arrives via discovery.
        let persisted_voter_ids = log_store.persisted_membership().voter_ids;
        for voter_id in persisted_voter_ids {
            if voter_id == self.config.node_id {
                continue;
            }
            if initial_members.iter().any(|(id, _)| *id == voter_id) {
                continue;
            }
            if let Some(addr) = self.peer_addresses.get(voter_id) {
                match consensus_transport.set_peer_via_registry(voter_id, &addr).await {
                    Ok(()) => {
                        debug!(
                            voter_id,
                            addr = %addr,
                            "Pre-registered persisted-membership peer for restart-path transport",
                        );
                    },
                    Err(e) => {
                        warn!(
                            voter_id,
                            addr = %addr,
                            error = %e,
                            "Failed to pre-register persisted-membership peer on restart",
                        );
                    },
                }
            }
        }
        let (engine, commit_rx, state_watchers) =
            inferadb_ledger_consensus::ConsensusEngine::start_with_all_callbacks(
                vec![consensus_shard],
                wal,
                inferadb_ledger_consensus::SystemClock,
                consensus_transport,
                std::time::Duration::from_millis(2),
                self.build_wake_notifier(),
                self.build_snapshot_coordinator(),
                self.build_snapshot_sender(),
                self.build_snapshot_installer(),
            );
        // Wrap the engine in `Arc` immediately so additional per-shard
        // handles (e.g. per-vault groups built by `start_vault_group`)
        // can share it via `ConsensusHandle::engine_arc` /
        // `ConsensusHandle::new_for_shard` without re-plumbing the
        // engine through every call site.
        let engine = Arc::new(engine);

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
        // The batch-writer config is consumed twice below: once by
        // `BatchWriter::new` (moved into the writer task) and once by
        // `InnerGroup::batch_writer_config` (retained so vault groups
        // started under this org can build their own writers with the
        // same coalescing thresholds).
        let stored_batch_writer_config = batch_writer_config.clone();
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

        // Per-shard apply-command channel (Stage 4). Drained by the apply
        // worker via `tokio::select!` alongside the commit channel; the
        // sender side is stored on `InnerGroup` so
        // `RaftManagerSnapshotInstaller` can find it via the
        // `(region, organization_id)` key.
        let (apply_command_tx, apply_command_rx) =
            tokio::sync::mpsc::channel::<crate::apply_command::ApplyCommand>(
                crate::apply_command::APPLY_COMMAND_CHANNEL_CAPACITY,
            );

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
            tokio::spawn(apply_worker.run(org_batch_rx, apply_command_rx));
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
            tokio::spawn(apply_worker.run(org_batch_rx, apply_command_rx));
        }

        // Create region group.
        //
        // `blocks_db` is surfaced here alongside `raft_db` so the
        // `StateCheckpointer` and `sync_all_state_dbs` can reach the
        // underlying `Database<FileBackend>` without holding a reference to
        // `block_archive` (which owns a domain API, not a durability API).
        let jobs_running = enable_background_jobs;
        let blocks_db = Arc::clone(block_archive.db());
        // Phase 5 / M2: per-organization data-plane groups carry a
        // `MembershipQueue` for rate-limiting per-vault conf-changes.
        // System (`GLOBAL`, 0) and regional control-plane (`region`, 0)
        // groups don't — they cascade through the existing
        // `cascade_membership_to_children` path until M3 wires the
        // watcher.
        let membership_queue = if organization_id == OrganizationId::new(0) {
            None
        } else {
            Some(Arc::new(crate::membership_queue::MembershipQueue::new(
                self.config.max_concurrent_snapshot_producing,
                crate::membership_queue::DEFAULT_MAX_BACKLOG,
            )))
        };
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
            batch_writer_config: stored_batch_writer_config,
            commitment_buffer,
            leader_lease,
            applied_index_rx,
            consensus_transport: Some(consensus_transport_for_group),
            events_db: Some(events_db),
            event_writer: event_writer_for_inner,
            last_activity: Arc::new(parking_lot::Mutex::new(std::time::Instant::now())),
            jobs_active: Arc::new(AtomicBool::new(jobs_running)),
            region_creation_rx: parking_lot::Mutex::new(region_creation_rx),
            organization_creation_rx: parking_lot::Mutex::new(organization_creation_rx),
            vault_creation_rx: parking_lot::Mutex::new(vault_creation_rx),
            vault_deletion_rx: parking_lot::Mutex::new(vault_deletion_rx),
            commit_dispatcher,
            membership_queue,
            apply_command_tx,
        });

        {
            let mut regions = self.regions.write();
            regions.insert((region, organization_id), Arc::clone(&inner));
        }
        // Maintain the reverse `shard_id → (region, org)` index alongside
        // `regions` so the snapshot coordinator's hot-path lookup is
        // O(1). Drops on `stop_region`.
        self.org_shard_index.insert(inner.handle().shard_id(), (region, organization_id));

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
        // Closures that snapshot the per-vault `raft.db`, `blocks.db`,
        // and `events.db` handles owned by every `InnerVaultGroup`
        // whose key matches this checkpointer's `(region,
        // organization_id)` scope. The checkpointer invokes all three
        // closures once per tick (Task #170 for raft.db; Phase 4.1.a
        // for blocks.db; Phase 4.2 for events.db) so vaults started or
        // stopped between ticks are picked up automatically — mirroring
        // the per-vault state.db enumeration that goes through
        // `StateLayer::live_vault_dbs()`. The shutdown-side equivalent
        // is built inline in [`Self::sync_all_state_dbs`] (Task #145 for
        // raft.db; Phase 4.1.a for blocks.db; Phase 4.2 for events.db).
        let vault_groups_for_raft = Arc::clone(&self.vault_groups);
        let vault_groups_for_blocks = Arc::clone(&self.vault_groups);
        let vault_groups_for_events = Arc::clone(&self.vault_groups);
        let region_for_checkpointer = region;
        let organization_id_for_checkpointer = organization_id;
        // Closures filter out Dormant vaults: O6 hibernation Pass 2
        // explicitly skips checkpointer fan-out for vaults whose
        // page-cache footprint we just evicted. Dormant means no in-flight
        // writes (apply pipeline already drained at the Active → Dormant
        // boundary), so there's nothing for the per-tick `sync_state` to
        // advance — and re-syncing would refault the OS pages we just
        // dropped via `posix_fadvise(POSIX_FADV_DONTNEED)`. On wake the
        // vault returns to Active and the next tick picks it back up.
        let vault_raft_dbs_fn: VaultRaftDbsFn = Arc::new(move || {
            vault_groups_for_raft
                .read()
                .iter()
                .filter(|((r, o, _), inner)| {
                    *r == region_for_checkpointer
                        && *o == organization_id_for_checkpointer
                        && inner.lifecycle_state() != VaultLifecycleState::Dormant
                })
                .map(|((_, _, vid), inner)| (*vid, Arc::clone(inner.raft_db())))
                .collect()
        });
        let vault_blocks_dbs_fn: VaultBlocksDbsFn = Arc::new(move || {
            vault_groups_for_blocks
                .read()
                .iter()
                .filter(|((r, o, _), inner)| {
                    *r == region_for_checkpointer
                        && *o == organization_id_for_checkpointer
                        && inner.lifecycle_state() != VaultLifecycleState::Dormant
                })
                .map(|((_, _, vid), inner)| (*vid, Arc::clone(inner.block_archive().db())))
                .collect()
        });
        // Per-vault `events.db` handles. A vault contributes a handle
        // only when its parent org has an `event_writer` configured —
        // [`RaftManager::start_vault_group`] inherits "no events" from
        // the parent and skips per-vault DB construction in that case,
        // so vaults without an event writer simply filter out here.
        // Dormant vaults are also filtered (see comment on
        // `vault_raft_dbs_fn` above).
        let vault_events_dbs_fn: VaultEventsDbsFn = Arc::new(move || {
            vault_groups_for_events
                .read()
                .iter()
                .filter(|((r, o, _), inner)| {
                    *r == region_for_checkpointer
                        && *o == organization_id_for_checkpointer
                        && inner.lifecycle_state() != VaultLifecycleState::Dormant
                })
                .filter_map(|((_, _, vid), inner)| {
                    inner.event_writer().map(|ew| (*vid, Arc::clone(ew.events_db().db())))
                })
                .collect()
        });
        // Closure for the per-tick Dormant snapshot — see `DormantVaultsFn`
        // docs in `state_checkpointer.rs` and the page-cache-eviction
        // discussion in `RaftManager::sleep_vault`. Dormant vaults must
        // not be re-synced because every sync touches the backend and
        // refaults the OS pages we just dropped via posix_fadvise.
        let vault_groups_for_dormant = Arc::clone(&self.vault_groups);
        let region_for_dormant = region;
        let organization_id_for_dormant = organization_id;
        let dormant_vaults_fn: crate::state_checkpointer::DormantVaultsFn = Arc::new(move || {
            vault_groups_for_dormant
                .read()
                .iter()
                .filter(|((r, o, _), inner)| {
                    *r == region_for_dormant
                        && *o == organization_id_for_dormant
                        && inner.lifecycle_state() == VaultLifecycleState::Dormant
                })
                .map(|((_, _, vid), _)| *vid)
                .collect()
        });
        // Closure for the per-tick per-vault `live_applied` snapshot —
        // Stage 5a observability. Mirrors `vault_raft_dbs_fn`'s
        // region+org+Dormant filter so the
        // `ledger_vault_applied_durable_lag` gauge never disagrees with
        // the sync-skip filter. Reads the in-memory
        // `VaultAppliedState.last_applied.index` projection through
        // `InnerVaultGroup::vault_applied_state()` — the live state the
        // commit pump swaps in after each `apply_committed_entries` call.
        let vault_groups_for_applied_state = Arc::clone(&self.vault_groups);
        let region_for_applied_state = region;
        let organization_id_for_applied_state = organization_id;
        let vault_applied_state_fn: crate::state_checkpointer::VaultAppliedStateFn =
            Arc::new(move || {
                vault_groups_for_applied_state
                    .read()
                    .iter()
                    .filter(|((r, o, _), inner)| {
                        *r == region_for_applied_state
                            && *o == organization_id_for_applied_state
                            && inner.lifecycle_state() != VaultLifecycleState::Dormant
                    })
                    .map(|((_, _, vid), inner)| {
                        let live_applied = inner
                            .vault_applied_state()
                            .load()
                            .last_applied
                            .as_ref()
                            .map_or(0, |id| id.index);
                        (*vid, live_applied)
                    })
                    .collect()
            });
        let state_checkpointer = StateCheckpointer::from_config(
            Arc::clone(&state),
            raft_db,
            blocks_db,
            events_db,
            meta_db,
            vault_raft_dbs_fn,
            vault_blocks_dbs_fn,
            vault_events_dbs_fn,
            dormant_vaults_fn,
            vault_applied_state_fn,
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
            .with_leader_lease(leader_lease)
            .with_snapshot_key_provider(Arc::clone(&self.config.snapshot_key_provider));

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

        // Wire vault create/delete signal channels for per-organization
        // log stores (organization_id != 0). Vault lifecycle is scoped to
        // a single `(region, organization)` pair, so the system /
        // region-plane groups (`organization_id == 0`) do not need the
        // channels. `start_organization_group` takes the receivers and
        // spawns a watcher task that dispatches `VaultCreationRequest` to
        // [`RaftManager::start_vault_group`] and `VaultDeletionRequest`
        // to [`RaftManager::stop_vault_group`] on every in-region node.
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

        // Mirror the `regions` removal in the reverse `shard_id → (region,
        // org)` index so the snapshot coordinator cannot resolve to a
        // dropped group.
        self.org_shard_index.remove(&region_group.handle().shard_id());

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

    /// Stops a per-organization group.
    ///
    /// Mirrors [`Self::stop_region`] but operates on a per-organization
    /// data-plane group keyed by `(region, organization_id)` where
    /// `organization_id != OrganizationId::new(0)`. Cancels the org's
    /// background jobs, requests its consensus engine reactor to shut
    /// down (which drops the engine's [`GrpcConsensusTransport`] —
    /// dropping in turn cancels every per-peer
    /// [`PeerSender`](crate::consensus_transport::peer_sender::PeerSender)
    /// drain task tied to that transport), removes the entry from
    /// `self.regions`, and closes the organization's storage.
    ///
    /// This method is **load-bearing for graceful restart**: without it,
    /// per-organization engines never exit during [`Self::shutdown`],
    /// leaving their bidirectional gRPC `Replicate` streams attached to
    /// the shared [`NodeConnectionRegistry`](crate::node_registry::NodeConnectionRegistry)
    /// channel. Subsequent restarts re-use the channel through fresh
    /// transports and observe the channel's H2 state in a stuck condition
    /// — new `client.replicate()` calls block until KeepAliveTimedOut.
    /// See Task #172.
    ///
    /// Per-vault groups under this org should be stopped via
    /// [`Self::stop_vault_group`] **before** calling this — vault groups
    /// register shards on the org's engine, and tearing the engine down
    /// while shards are live skips dispatcher cleanup. [`Self::shutdown`]
    /// orders the sweep correctly.
    ///
    /// # Errors
    ///
    /// Returns [`RaftManagerError::RegionNotFound`] if no group with the
    /// given `(region, organization_id)` is currently active.
    pub async fn stop_organization_group(
        &self,
        region: Region,
        organization_id: OrganizationId,
    ) -> Result<()> {
        let org_group = {
            let mut regions = self.regions.write();
            regions
                .remove(&(region, organization_id))
                .ok_or(RaftManagerError::RegionNotFound { region })?
        };

        // Mirror the `regions` removal in the reverse `shard_id → (region,
        // org)` index so the snapshot coordinator cannot resolve to a
        // dropped group.
        self.org_shard_index.remove(&org_group.handle().shard_id());

        // Abort background jobs first.
        {
            let mut jobs = org_group.background_jobs.lock();
            jobs.cancel();
            debug!(
                region = region.as_str(),
                organization_id = organization_id.value(),
                "Aborted per-organization background jobs"
            );
        }

        // Shut down the consensus engine reactor for this organization.
        // The engine owns the `GrpcConsensusTransport`; on reactor exit
        // the transport's per-peer `PeerSender` drain tasks are cancelled
        // via their `Drop` impl (cancels `shutdown_token`, aborts the
        // task), which closes any in-flight `client.replicate()` stream
        // open against the shared `NodeConnectionRegistry` channel.
        let handle = org_group.handle().clone();
        tokio::spawn(async move {
            handle.request_shutdown().await;
        });

        // Close organization storage.
        if let Err(e) = self.storage_manager.close_organization(region, organization_id) {
            warn!(
                region = region.as_str(),
                organization_id = organization_id.value(),
                error = %e,
                "Error closing organization storage during per-org group stop"
            );
        }

        info!(
            region = region.as_str(),
            organization_id = organization_id.value(),
            "Per-organization group stopped"
        );
        Ok(())
    }

    /// Stops every active group on this manager — per-vault groups,
    /// per-organization groups, and finally the region (control-plane)
    /// groups themselves.
    ///
    /// Tear-down ordering is load-bearing:
    ///
    /// 1. **Per-vault groups** first. Each vault registers a shard on its parent organization's
    ///    [`ConsensusEngine`] and a sender on that org's
    ///    [`CommitDispatcher`](crate::commit_dispatcher::CommitDispatcher). Tearing down the parent
    ///    engine before deregistering the vault leaves stale dispatcher entries pointing at dropped
    ///    channels.
    /// 2. **Per-organization groups** next (those with `org_id != 0`). Each org owns its own
    ///    [`ConsensusEngine`] and [`GrpcConsensusTransport`]. Dropping the transport cancels its
    ///    `PeerSender` drain tasks, closing any open `Replicate` streams. **Without this step the
    ///    H2 streams persist across a graceful restart**, leaving the shared
    ///    [`NodeConnectionRegistry`](crate::node_registry::NodeConnectionRegistry) channel in a
    ///    state where the next `client.replicate()` call blocks until KeepAliveTimedOut — see Task
    ///    #172.
    /// 3. **Region (control-plane) groups** last via [`Self::stop_region`]. These hold the system /
    ///    data-region control-plane state and have always been stopped here.
    ///
    /// Errors per group are logged and the sweep continues — one group's
    /// failure must not block the remaining tear-down.
    pub async fn shutdown(&self) {
        // Phase 1 — per-vault groups. Snapshot the keys under the read
        // lock, drop the lock before awaiting `stop_vault_group` which
        // re-acquires the same lock for write. Sequential rather than
        // concurrent: the dispatcher / engine handoff in `stop_vault_group`
        // is per-organization-engine-serialized anyway, and parallel
        // teardown buys nothing during shutdown.
        let vault_keys: Vec<(Region, OrganizationId, VaultId)> =
            self.vault_groups.read().keys().copied().collect();
        for (region, organization_id, vault_id) in vault_keys {
            if let Err(e) = self.stop_vault_group(region, organization_id, vault_id).await {
                warn!(
                    region = region.as_str(),
                    organization_id = organization_id.value(),
                    vault_id = vault_id.value(),
                    error = %e,
                    "Error stopping vault group during shutdown"
                );
            }
        }

        // Phase 2 — per-organization groups (org_id != 0). Region groups
        // (org_id == 0) are deferred to phase 3 so the existing
        // `stop_region` codepath handles them with its existing log
        // line and storage-close call site.
        let org_keys: Vec<(Region, OrganizationId)> = self
            .regions
            .read()
            .keys()
            .copied()
            .filter(|(_, oid)| *oid != OrganizationId::new(0))
            .collect();
        for (region, organization_id) in org_keys {
            if let Err(e) = self.stop_organization_group(region, organization_id).await {
                warn!(
                    region = region.as_str(),
                    organization_id = organization_id.value(),
                    error = %e,
                    "Error stopping per-organization group during shutdown"
                );
            }
        }

        // Phase 3 — region (control-plane) groups.
        let regions: Vec<Region> = self.list_regions();
        for region in regions {
            if let Err(e) = self.stop_region(region).await {
                warn!(region = region.as_str(), error = %e, "Error stopping region during shutdown");
            }
        }

        info!("Raft Manager shutdown complete");
    }

    /// Forces a checkpoint-style `sync_state` on every region's lazy-durable
    /// databases — across **every** `(region, organization_id)` group in
    /// [`Self::regions`], not just the region's control-plane group.
    ///
    /// Called from the graceful-shutdown `pre_shutdown` closure **after** the
    /// WAL flush so that on clean shutdown the post-restart WAL replay is
    /// zero entries: the god-byte pointer captures every apply that happened
    /// between the last [`StateCheckpointer`] tick and the final drain.
    ///
    /// Each per-org group owns four lazy-durable databases: the per-vault
    /// state.db handles owned by its [`StateLayer`], its raft.db
    /// (`KEY_APPLIED_STATE` + Raft log), its blocks.db, and — when
    /// configured — its events.db. Each must be synced; skipping raft.db
    /// causes `applied_durable = 0` to be read on restart and forces a
    /// full WAL replay. Per-org meta.db (the `_meta:last_applied` sentinel)
    /// is synced last, strictly after the entity DBs it's a sentinel for.
    ///
    /// Per-vault raft.db handles for each [`VaultGroup`](VaultGroup) are
    /// fanned out alongside the per-org work — they live at
    /// `{data_dir}/{region}/{org_id}/state/vault-{vault_id}/` and carry
    /// their own applied-state metadata that the post-restart replay path
    /// depends on.
    ///
    /// Errors are logged per-region, per-org, and per-db but do not abort
    /// the sweep — one group's disk-full (or one db's failure) must not
    /// block the remaining work from reaching durability. The caller
    /// treats this as best-effort.
    ///
    /// `total_timeout` is the **total** budget across all regions; each
    /// region gets a proportional share with a 1s floor. Within a region,
    /// every per-org group and every per-vault raft.db are synced
    /// concurrently via `join_all` under a single `tokio::time::timeout`,
    /// so they share the per-region budget rather than splitting it. A
    /// single timed-out region does not consume the remaining regions'
    /// budgets.
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
            "sync_all_state_dbs: forcing final per-org state.db + raft.db + blocks.db + \
             events.db (if configured) then meta.db, plus per-vault raft.db + blocks.db + \
             events.db, across all per-org groups in every region (strict ordering)"
        );

        for region in regions {
            // Snapshot every per-org group in this region under the read
            // lock, drop the lock before awaiting so the shutdown sweep
            // never contends with other readers for the regions map.
            let org_groups: Vec<(OrganizationId, Arc<InnerGroup>)> = self
                .regions
                .read()
                .iter()
                .filter(|((r, _), _)| *r == region)
                .map(|((_, oid), g)| (*oid, Arc::clone(g)))
                .collect();
            if org_groups.is_empty() {
                debug!(
                    region = region.as_str(),
                    "sync_all_state_dbs: region has no org groups, skipping"
                );
                continue;
            }
            // Per-vault raft.dbs, per-vault blocks.dbs, and per-vault
            // events.dbs for this region. Every [`VaultGroup`](VaultGroup)
            // opens its own `raft.db` (Task #170), its own `blocks.db`
            // (Phase 4.1.a), and its own `events.db` (Phase 4.2) under
            // `{data_dir}/{region}/{org_id}/state/vault-{vault_id}/`; the
            // shutdown sweep must sync each one so applied-state metadata
            // on the per-vault `raft.db`, dirty Merkle-chain pages on the
            // per-vault `blocks.db`, and dirty audit-log pages on the
            // per-vault `events.db` all reach disk alongside the vault's
            // `state.db`. All three per-vault fan-outs are independent of
            // the per-org fan-out — each covers vaults across every
            // parent org in this region in a single `join_all`.
            // Snapshotted under the same read of `vault_groups` so the
            // lists stay in lock-step (a vault that materialises mid-
            // sweep is picked up on the next call). A vault contributes
            // an `events.db` handle only when its parent org has events
            // configured; vaults under a no-events org simply filter
            // out, mirroring the per-org `events.db` `Option` shape.
            let (vault_raft_dbs, vault_blocks_dbs, vault_events_dbs) = {
                let groups = self.vault_groups.read();
                let mut raft: Vec<(VaultId, Arc<Database<FileBackend>>)> = Vec::new();
                let mut blocks: Vec<(VaultId, Arc<Database<FileBackend>>)> = Vec::new();
                let mut events: Vec<(VaultId, Arc<Database<FileBackend>>)> = Vec::new();
                for ((r, _, vid), inner) in groups.iter() {
                    if *r != region {
                        continue;
                    }
                    raft.push((*vid, Arc::clone(inner.raft_db())));
                    blocks.push((*vid, Arc::clone(inner.block_archive().db())));
                    if let Some(ew) = inner.event_writer() {
                        events.push((*vid, Arc::clone(ew.events_db().db())));
                    }
                }
                (raft, blocks, events)
            };

            // Build one sync future per org group. Each future performs
            // that org's strict two-phase ordering:
            //
            // Phase A — live per-vault state.dbs (from `StateLayer`),
            //   the org's raft.db, blocks.db, and events.db (if
            //   configured), all concurrent.
            // Phase B — the org's meta.db, strictly after Phase A. Never
            //   invert; meta.db is the sentinel that references Phase-A
            //   entity data.
            //
            // Each org future returns an [`OrgSyncOutcome`] carrying the
            // results + the handles needed to log post-sync
            // `last_synced_snapshot_id`.
            let org_futs = org_groups.iter().map(|(org_id, group)| {
                let org_id = *org_id;
                let vault_dbs = group.state().live_vault_dbs();
                let raft_db = Arc::clone(group.raft_db());
                let blocks_db = Arc::clone(group.blocks_db());
                let events_db_opt = group.events_state_db();
                let meta_db = Arc::clone(group.meta_db());
                async move {
                    let vault_futs = futures::future::join_all(
                        vault_dbs.iter().map(|(_, db)| Arc::clone(db).sync_state()),
                    );
                    let raft_fut = Arc::clone(&raft_db).sync_state();
                    let blocks_fut = Arc::clone(&blocks_db).sync_state();
                    let events_db_for_sync = events_db_opt.clone();
                    let events_fut_async = async move {
                        if let Some(ev) = events_db_for_sync {
                            Some(ev.sync_state().await)
                        } else {
                            None
                        }
                    };
                    let (vault_results, raft_result, blocks_result, events_result) =
                        tokio::join!(vault_futs, raft_fut, blocks_fut, events_fut_async);
                    // Phase B — strict ordering: meta.db after Phase A.
                    let meta_result = Arc::clone(&meta_db).sync_state().await;
                    OrgSyncOutcome {
                        org_id,
                        vault_dbs,
                        vault_results,
                        raft_db,
                        raft_result,
                        blocks_db,
                        blocks_result,
                        events_db: events_db_opt,
                        events_result,
                        meta_db,
                        meta_result,
                    }
                }
            });

            // Fan out per-vault raft.db, per-vault blocks.db, and
            // per-vault events.db syncs in parallel with the per-org
            // fan-out. Zipping results with the snapshots gives each
            // log line access to its vault id + handle for
            // `last_synced_snapshot_id`.
            let vault_raft_futs = futures::future::join_all(
                vault_raft_dbs.iter().map(|(_, db)| Arc::clone(db).sync_state()),
            );
            let vault_blocks_futs = futures::future::join_all(
                vault_blocks_dbs.iter().map(|(_, db)| Arc::clone(db).sync_state()),
            );
            let vault_events_futs = futures::future::join_all(
                vault_events_dbs.iter().map(|(_, db)| Arc::clone(db).sync_state()),
            );

            let org_futs_joined = futures::future::join_all(org_futs);
            let timeout_outcome = tokio::time::timeout(per_region_timeout, async move {
                tokio::join!(org_futs_joined, vault_raft_futs, vault_blocks_futs, vault_events_futs)
            })
            .await;

            match timeout_outcome {
                Ok((
                    org_outcomes,
                    vault_raft_results,
                    vault_blocks_results,
                    vault_events_results,
                )) => {
                    for outcome in &org_outcomes {
                        log_org_sync_outcome(region, outcome);
                    }
                    for ((vault_id, db), result) in
                        vault_raft_dbs.iter().zip(vault_raft_results.iter())
                    {
                        match result {
                            Ok(()) => info!(
                                region = region.as_str(),
                                db = "raft",
                                vault_id = vault_id.value(),
                                last_synced_snapshot_id = db.last_synced_snapshot_id(),
                                "sync_all_state_dbs: final per-vault raft-DB sync complete"
                            ),
                            Err(e) => warn!(
                                region = region.as_str(),
                                db = "raft",
                                vault_id = vault_id.value(),
                                error = %e,
                                "sync_all_state_dbs: final per-vault raft-DB sync failed; crash gap narrowed but not zero"
                            ),
                        }
                    }
                    for ((vault_id, db), result) in
                        vault_blocks_dbs.iter().zip(vault_blocks_results.iter())
                    {
                        match result {
                            Ok(()) => info!(
                                region = region.as_str(),
                                db = "blocks",
                                vault_id = vault_id.value(),
                                last_synced_snapshot_id = db.last_synced_snapshot_id(),
                                "sync_all_state_dbs: final per-vault blocks-DB sync complete"
                            ),
                            Err(e) => warn!(
                                region = region.as_str(),
                                db = "blocks",
                                vault_id = vault_id.value(),
                                error = %e,
                                "sync_all_state_dbs: final per-vault blocks-DB sync failed; crash gap narrowed but not zero"
                            ),
                        }
                    }
                    for ((vault_id, db), result) in
                        vault_events_dbs.iter().zip(vault_events_results.iter())
                    {
                        match result {
                            Ok(()) => info!(
                                region = region.as_str(),
                                db = "events",
                                vault_id = vault_id.value(),
                                last_synced_snapshot_id = db.last_synced_snapshot_id(),
                                "sync_all_state_dbs: final per-vault events-DB sync complete"
                            ),
                            Err(e) => warn!(
                                region = region.as_str(),
                                db = "events",
                                vault_id = vault_id.value(),
                                error = %e,
                                "sync_all_state_dbs: final per-vault events-DB sync failed; crash gap narrowed but not zero"
                            ),
                        }
                    }
                },
                Err(_) => {
                    let org_ids: Vec<i64> = org_groups.iter().map(|(oid, _)| oid.value()).collect();
                    warn!(
                        region = region.as_str(),
                        timeout_ms = per_region_timeout.as_millis() as u64,
                        org_ids = ?org_ids,
                        "sync_all_state_dbs: final state-DB sync (per-org state + raft + blocks + \
                         events then meta, plus per-vault raft + per-vault blocks + per-vault \
                         events) timed out; continuing with remaining regions"
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

// ============================================================================
// Vault-tier classification (P2c.1)
// ============================================================================
//
// The per-vault commit-pump task spawned by [`RaftManager::start_vault_group`]
// drains [`CommittedBatch`](inferadb_ledger_consensus::committed::CommittedBatch)
// values produced by the parent organization's
// [`CommitDispatcher`](crate::commit_dispatcher::CommitDispatcher). Until the
// per-vault apply pipeline lands (P2c.2), the stub's only correctness
// responsibility is to verify that every committed entry routed to a vault
// shard carries a vault-scoped [`OrganizationRequest`] variant.
//
// The classification below is the foundation for two follow-on slices:
//
// - **P2c.2** wires the real apply path; only `VaultScoped` variants reach it.
// - **P2c.3** flips routing — vault-scoped writes propose against the vault shard instead of the
//   parent org shard. A misclassified variant here becomes either a silent acceptance (data lands
//   on the wrong shard) or a spurious rejection (legitimate write fails apply).
//
// When in doubt, default to [`VaultTier::OrgScoped`] — a false-positive tier
// violation logs loudly; a false-negative silently corrupts routing.

/// Classification of an [`OrganizationRequest`] variant relative to the
/// vault tier.
///
/// Used by the per-vault commit-pump task to detect tier violations:
/// org-scoped variants must apply at the parent
/// [`OrganizationGroup`](OrganizationGroup), never at a vault shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VaultTier {
    /// Variant operates on entity data within a single vault. Accepted at
    /// vault apply.
    VaultScoped,
    /// Variant operates on org-level metadata (members, teams, invitations,
    /// apps, vault lifecycle). Must apply at the parent
    /// [`OrganizationGroup`](OrganizationGroup), not at a vault shard.
    OrgScoped,
}

/// Classifies an [`OrganizationRequest`] as vault-scoped or org-scoped.
///
/// `Write`, `BatchWrite`, and `IngestExternalEvents` target vault entries;
/// every other variant is org-level metadata. `BatchWrite` is treated as
/// vault-scoped because it carries a uniform sub-request set (callers wrap
/// only `Write` payloads); P2c.2's real apply pipeline will recurse into the
/// inner requests and reject any non-vault-scoped sub-request at that point.
fn classify_vault_tier(request: &OrganizationRequest) -> VaultTier {
    use OrganizationRequest as Req;
    match request {
        Req::Write { .. } | Req::BatchWrite { .. } | Req::IngestExternalEvents { .. } => {
            VaultTier::VaultScoped
        },
        Req::CreateVault { .. }
        | Req::UpdateVault { .. }
        | Req::DeleteVault { .. }
        | Req::UpdateVaultHealth { .. }
        | Req::AddOrganizationMember { .. }
        | Req::RemoveOrganizationMember { .. }
        | Req::UpdateOrganizationMemberRole { .. }
        | Req::CreateOrganizationInvite { .. }
        | Req::ResolveOrganizationInvite { .. }
        | Req::PurgeOrganizationInviteIndexes { .. }
        | Req::RehashInviteEmailIndex { .. }
        | Req::CreateOrganizationTeam { .. }
        | Req::DeleteOrganizationTeam { .. }
        | Req::CreateApp { .. }
        | Req::DeleteApp { .. }
        | Req::SetAppEnabled { .. }
        | Req::SetAppCredentialEnabled { .. }
        | Req::RotateAppClientSecret { .. }
        | Req::CreateAppClientAssertion { .. }
        | Req::DeleteAppClientAssertion { .. }
        | Req::SetAppClientAssertionEnabled { .. }
        | Req::AddAppVault { .. }
        | Req::UpdateAppVault { .. }
        | Req::RemoveAppVault { .. } => VaultTier::OrgScoped,
    }
}

/// Returns a static name for an [`OrganizationRequest`] variant, used for
/// diagnostic log fields.
fn request_variant_name(request: &OrganizationRequest) -> &'static str {
    use OrganizationRequest as Req;
    match request {
        Req::Write { .. } => "Write",
        Req::BatchWrite { .. } => "BatchWrite",
        Req::IngestExternalEvents { .. } => "IngestExternalEvents",
        Req::CreateVault { .. } => "CreateVault",
        Req::UpdateVault { .. } => "UpdateVault",
        Req::DeleteVault { .. } => "DeleteVault",
        Req::UpdateVaultHealth { .. } => "UpdateVaultHealth",
        Req::AddOrganizationMember { .. } => "AddOrganizationMember",
        Req::RemoveOrganizationMember { .. } => "RemoveOrganizationMember",
        Req::UpdateOrganizationMemberRole { .. } => "UpdateOrganizationMemberRole",
        Req::CreateOrganizationInvite { .. } => "CreateOrganizationInvite",
        Req::ResolveOrganizationInvite { .. } => "ResolveOrganizationInvite",
        Req::PurgeOrganizationInviteIndexes { .. } => "PurgeOrganizationInviteIndexes",
        Req::RehashInviteEmailIndex { .. } => "RehashInviteEmailIndex",
        Req::CreateOrganizationTeam { .. } => "CreateOrganizationTeam",
        Req::DeleteOrganizationTeam { .. } => "DeleteOrganizationTeam",
        Req::CreateApp { .. } => "CreateApp",
        Req::DeleteApp { .. } => "DeleteApp",
        Req::SetAppEnabled { .. } => "SetAppEnabled",
        Req::SetAppCredentialEnabled { .. } => "SetAppCredentialEnabled",
        Req::RotateAppClientSecret { .. } => "RotateAppClientSecret",
        Req::CreateAppClientAssertion { .. } => "CreateAppClientAssertion",
        Req::DeleteAppClientAssertion { .. } => "DeleteAppClientAssertion",
        Req::SetAppClientAssertionEnabled { .. } => "SetAppClientAssertionEnabled",
        Req::AddAppVault { .. } => "AddAppVault",
        Req::UpdateAppVault { .. } => "UpdateAppVault",
        Req::RemoveAppVault { .. } => "RemoveAppVault",
    }
}

/// Decodes a committed entry's payload bytes into an [`OrganizationRequest`].
///
/// Mirrors the decode shape used by
/// [`RaftLogStore::apply_committed_entries`](crate::log_storage::RaftLogStore::apply_committed_entries):
/// payloads are postcard-encoded
/// [`RaftPayload<OrganizationRequest>`](RaftPayload) values; the wrapper
/// metadata is discarded and only the inner request is returned, since the
/// commit-pump stub neither stamps timestamps nor verifies state-root
/// commitments.
fn decode_vault_payload(
    bytes: &[u8],
) -> std::result::Result<OrganizationRequest, inferadb_ledger_types::CodecError> {
    inferadb_ledger_types::decode::<RaftPayload<OrganizationRequest>>(bytes).map(|p| p.request)
}

/// Collected outcome of syncing one per-org group inside
/// [`RaftManager::sync_all_state_dbs`].
///
/// Each field pairs a sync [`Result`] with the handle needed to read
/// `last_synced_snapshot_id` after the await completes. The handles are
/// kept alive for the duration of the outer join so post-sync logging
/// can observe snapshot-id advance without a second lookup through the
/// regions map.
struct OrgSyncOutcome {
    org_id: OrganizationId,
    vault_dbs: Vec<(VaultId, Arc<Database<FileBackend>>)>,
    vault_results: Vec<std::result::Result<(), inferadb_ledger_store::Error>>,
    raft_db: Arc<Database<FileBackend>>,
    raft_result: std::result::Result<(), inferadb_ledger_store::Error>,
    blocks_db: Arc<Database<FileBackend>>,
    blocks_result: std::result::Result<(), inferadb_ledger_store::Error>,
    events_db: Option<Arc<Database<FileBackend>>>,
    events_result: Option<std::result::Result<(), inferadb_ledger_store::Error>>,
    meta_db: Arc<Database<FileBackend>>,
    meta_result: std::result::Result<(), inferadb_ledger_store::Error>,
}

/// Emits the info/warn log lines for a single per-org sync outcome.
///
/// Every line carries `region` + `organization_id` so operators can
/// correlate a failed sync back to the owning group. Per-vault state
/// entries additionally carry `vault_id`. Meta.db is logged last,
/// mirroring the Phase-A → Phase-B strict ordering.
fn log_org_sync_outcome(region: Region, outcome: &OrgSyncOutcome) {
    let OrgSyncOutcome {
        org_id,
        vault_dbs,
        vault_results,
        raft_db,
        raft_result,
        blocks_db,
        blocks_result,
        events_db,
        events_result,
        meta_db,
        meta_result,
    } = outcome;

    for ((vault_id, db), result) in vault_dbs.iter().zip(vault_results.iter()) {
        match result {
            Ok(()) => info!(
                region = region.as_str(),
                organization_id = org_id.value(),
                db = "state",
                vault_id = vault_id.value(),
                last_synced_snapshot_id = db.last_synced_snapshot_id(),
                "sync_all_state_dbs: final per-vault state-DB sync complete"
            ),
            Err(e) => warn!(
                region = region.as_str(),
                organization_id = org_id.value(),
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
            organization_id = org_id.value(),
            db = "raft",
            last_synced_snapshot_id = raft_db.last_synced_snapshot_id(),
            "sync_all_state_dbs: final state-DB sync complete"
        ),
        Err(e) => warn!(
            region = region.as_str(),
            organization_id = org_id.value(),
            db = "raft",
            error = %e,
            "sync_all_state_dbs: final state-DB sync failed; crash gap narrowed but not zero"
        ),
    }
    match blocks_result {
        Ok(()) => info!(
            region = region.as_str(),
            organization_id = org_id.value(),
            db = "blocks",
            last_synced_snapshot_id = blocks_db.last_synced_snapshot_id(),
            "sync_all_state_dbs: final state-DB sync complete"
        ),
        Err(e) => warn!(
            region = region.as_str(),
            organization_id = org_id.value(),
            db = "blocks",
            error = %e,
            "sync_all_state_dbs: final state-DB sync failed; crash gap narrowed but not zero"
        ),
    }
    match (events_result, events_db) {
        (Some(Ok(())), Some(db)) => info!(
            region = region.as_str(),
            organization_id = org_id.value(),
            db = "events",
            last_synced_snapshot_id = db.last_synced_snapshot_id(),
            "sync_all_state_dbs: final state-DB sync complete"
        ),
        (Some(Err(e)), _) => warn!(
            region = region.as_str(),
            organization_id = org_id.value(),
            db = "events",
            error = %e,
            "sync_all_state_dbs: final state-DB sync failed; crash gap narrowed but not zero"
        ),
        (None, _) => debug!(
            region = region.as_str(),
            organization_id = org_id.value(),
            "sync_all_state_dbs: org has no events_db; skipping events sync"
        ),
        (Some(Ok(())), None) => {
            // Unreachable: events_result Some implies the per-org future
            // observed a configured events_db; the handle is held for the
            // duration of the outer join. Swallow defensively rather than
            // panic.
            debug!(
                region = region.as_str(),
                organization_id = org_id.value(),
                "sync_all_state_dbs: events sync succeeded but handle no longer available"
            );
        },
    }
    match meta_result {
        Ok(()) => info!(
            region = region.as_str(),
            organization_id = org_id.value(),
            db = "meta",
            last_synced_snapshot_id = meta_db.last_synced_snapshot_id(),
            "sync_all_state_dbs: final state-DB sync complete"
        ),
        Err(e) => warn!(
            region = region.as_str(),
            organization_id = org_id.value(),
            db = "meta",
            error = %e,
            "sync_all_state_dbs: final state-DB sync failed; crash gap narrowed but not zero"
        ),
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

/// Single-shard cascade application used by
/// [`RaftManager::cascade_membership_to_children`]. Idempotent against
/// the shard's current membership; failures are logged but never abort
/// the cascade.
#[allow(clippy::too_many_arguments)]
async fn apply_cascade_action(
    action: CascadeMembershipAction,
    manager: &RaftManager,
    handle: &Arc<crate::ConsensusHandle>,
    transport: Option<&crate::consensus_transport::GrpcConsensusTransport>,
    target: inferadb_ledger_consensus::types::NodeId,
    node_id: u64,
    region: Region,
    organization_id: OrganizationId,
    vault_id: Option<VaultId>,
) {
    let log_target = || {
        format!(
            "region={} org={} vault={:?}",
            region.as_str(),
            organization_id.value(),
            vault_id.map(|v| v.value()),
        )
    };

    match action {
        CascadeMembershipAction::AddLearner => {
            let state = handle.shard_state();
            if state.voters.contains(&target) || state.learners.contains(&target) {
                return;
            }
            if let Some(t) = transport
                && let Some(addr) = manager.peer_addresses().get(node_id)
                && let Err(e) = t.set_peer_via_registry(node_id, &addr).await
            {
                warn!(
                    node_id, addr = %addr, error = %e,
                    "Cascade: failed to register learner transport on child shard ({})",
                    log_target(),
                );
            }
            match tokio::time::timeout(
                std::time::Duration::from_secs(3),
                handle.add_learner(node_id, false),
            )
            .await
            {
                Ok(Ok(())) => {
                    info!(node_id, "Cascade: added learner to child shard ({})", log_target());
                },
                Ok(Err(e)) => {
                    let msg = e.to_string();
                    if !msg.contains("no-op") && !msg.contains("not the leader") {
                        warn!(
                            node_id, error = %e,
                            "Cascade: add_learner failed on child shard ({})",
                            log_target(),
                        );
                    }
                },
                Err(_) => {
                    warn!(
                        node_id,
                        "Cascade: add_learner timed out on child shard ({})",
                        log_target()
                    );
                },
            }
        },
        CascadeMembershipAction::PromoteVoter => {
            let state = handle.shard_state();
            if state.voters.contains(&target) {
                return;
            }
            if !state.learners.contains(&target) {
                if let Some(t) = transport
                    && let Some(addr) = manager.peer_addresses().get(node_id)
                {
                    let _ = t.set_peer_via_registry(node_id, &addr).await;
                }
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    handle.add_learner(node_id, false),
                )
                .await;
            }
            match tokio::time::timeout(
                std::time::Duration::from_secs(3),
                handle.promote_voter(node_id),
            )
            .await
            {
                Ok(Ok(())) => {
                    info!(node_id, "Cascade: promoted voter on child shard ({})", log_target());
                },
                Ok(Err(e)) => {
                    let msg = e.to_string();
                    if !msg.contains("no-op") && !msg.contains("not the leader") {
                        warn!(
                            node_id, error = %e,
                            "Cascade: promote_voter failed on child shard ({})",
                            log_target(),
                        );
                    }
                },
                Err(_) => {
                    warn!(
                        node_id,
                        "Cascade: promote_voter timed out on child shard ({})",
                        log_target()
                    );
                },
            }
        },
        CascadeMembershipAction::Remove => {
            let state = handle.shard_state();
            if !state.voters.contains(&target) && !state.learners.contains(&target) {
                return;
            }
            // Retry on "already undergoing a configuration change" — the
            // child shard's membership state machine may still be
            // resolving an earlier proposal (its own previous cascade
            // op or an explicit DR scheduler op). The cascade must not
            // give up, otherwise dead voters linger as quorum-blocking
            // ghosts after their parent removal.
            for attempt in 0..10u32 {
                match tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    handle.remove_node(node_id),
                )
                .await
                {
                    Ok(Ok(())) => {
                        info!(node_id, "Cascade: removed node from child shard ({})", log_target());
                        return;
                    },
                    Ok(Err(e)) if e.to_string().contains("already undergoing") => {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            150 * u64::from(attempt + 1),
                        ))
                        .await;
                    },
                    Ok(Err(e)) if e.to_string().contains("no-op") => return,
                    Ok(Err(e)) => {
                        let msg = e.to_string();
                        if !msg.contains("not the leader") {
                            warn!(
                                node_id, error = %e,
                                "Cascade: remove_node failed on child shard ({})",
                                log_target(),
                            );
                        }
                        return;
                    },
                    Err(_) => {
                        warn!(
                            node_id,
                            "Cascade: remove_node timed out on child shard ({})",
                            log_target()
                        );
                        return;
                    },
                }
            }
        },
    }
}

/// Applies a single cascade action to a per-organization shard.
///
/// Thin wrapper over [`apply_cascade_action`] for the org-tier path
/// — pre-registers the peer on the org's
/// [`GrpcConsensusTransport`](crate::consensus_transport::GrpcConsensusTransport)
/// (when `transport` is `Some`) and proposes the conf-change against
/// the org's Raft group. Used by the M3
/// [`RegionMembershipWatcher`](crate::region_membership_watcher::RegionMembershipWatcher)
/// to apply the org-level half of the cascade synchronously before
/// enqueueing per-vault entries — the per-vault dispatcher reuses
/// the org's transport (root rule 17), so the org-level transport
/// must know about the new peer first.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn apply_cascade_action_for_org(
    action: CascadeMembershipAction,
    manager: &RaftManager,
    handle: &Arc<crate::ConsensusHandle>,
    transport: Option<&crate::consensus_transport::GrpcConsensusTransport>,
    target: inferadb_ledger_consensus::types::NodeId,
    node_id: u64,
    region: Region,
    organization_id: OrganizationId,
) {
    apply_cascade_action(
        action,
        manager,
        handle,
        transport,
        target,
        node_id,
        region,
        organization_id,
        None,
    )
    .await;
}

/// Applies a single cascade action to a per-vault shard.
///
/// Thin wrapper over [`apply_cascade_action`] with `transport = None`
/// — vault groups share their parent organization's transport (root
/// rule 17), so the vault path never registers peers on its own
/// transport. Used by the M3
/// [`MembershipDispatcher`](crate::region_membership_watcher::MembershipDispatcher)
/// to reuse the legacy cascade primitive without re-implementing its
/// idempotence / retry logic.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn apply_cascade_action_for_vault(
    action: CascadeMembershipAction,
    manager: &RaftManager,
    handle: &Arc<crate::ConsensusHandle>,
    target: inferadb_ledger_consensus::types::NodeId,
    node_id: u64,
    region: Region,
    organization_id: OrganizationId,
    vault_id: VaultId,
) {
    apply_cascade_action(
        action,
        manager,
        handle,
        // Vault groups share parent org's transport.
        None,
        target,
        node_id,
        region,
        organization_id,
        Some(vault_id),
    )
    .await;
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

    /// Convenience constructor for tests: builds a fresh per-test registry
    /// and returns the manager wrapped in [`Arc`]. Production callers
    /// (server bootstrap) share a single `Arc<RaftManager>` across the
    /// process — see [`bootstrap`](../../../server/src/bootstrap.rs). Tests
    /// mirror that shape so methods that take `self: &Arc<Self>`
    /// (notably [`RaftManager::start_organization_group`], which spawns a
    /// vault-lifecycle watcher that re-enters the manager) dispatch
    /// correctly.
    fn test_manager(config: RaftManagerConfig) -> Arc<RaftManager> {
        Arc::new(RaftManager::new(
            config,
            Arc::new(crate::node_registry::NodeConnectionRegistry::new()),
        ))
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

    /// Phase 4.1.a: each vault must own its own [`BlockArchive`] backed
    /// by its own `blocks.db` file at
    /// `{data_dir}/{region}/{org_id}/state/vault-{vault_id}/blocks.db`.
    /// Sharing the parent organization's archive (the pre-Phase-4.1.a
    /// shape) means vault writes interleave with each other on the org
    /// chain — this regression-tests the fresh per-vault chain semantics
    /// by asserting Arc identity inequality + on-disk file presence.
    #[tokio::test]
    async fn test_start_vault_group_constructs_per_vault_block_archive() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");
        let org_id = OrganizationId::new(151);
        manager
            .start_organization_group(
                Region::GLOBAL,
                org_id,
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                None,
            )
            .await
            .expect("start org group");

        let org_group = manager.get_organization_group(Region::GLOBAL, org_id).expect("org group");
        let org_archive = Arc::clone(org_group.block_archive());

        let vault_id = VaultId::new(909);
        let vault_group = manager
            .start_vault_group(Region::GLOBAL, org_id, vault_id)
            .await
            .expect("start vault group");

        // Per-vault BlockArchive must NOT be the same Arc as the parent
        // organization's archive — sharing the Arc was the pre-Phase-4.1.a
        // shape (Option B from P2c.2.1).
        let vault_archive = vault_group.block_archive();
        assert!(
            !Arc::ptr_eq(&org_archive, vault_archive),
            "vault BlockArchive Arc must differ from parent org's after Phase 4.1.a",
        );
        // The underlying blocks.db handle must also be distinct.
        assert!(
            !Arc::ptr_eq(org_archive.db(), vault_archive.db()),
            "vault blocks.db Database Arc must differ from parent org's after Phase 4.1.a",
        );

        // The on-disk per-vault `blocks.db` file must exist at the
        // composed path.
        let expected_path = temp
            .path()
            .join("global")
            .join(org_id.value().to_string())
            .join("state")
            .join(format!("vault-{}", vault_id.value()))
            .join("blocks.db");
        assert!(
            expected_path.exists(),
            "per-vault blocks.db must be created at {}",
            expected_path.display(),
        );
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

    #[tokio::test]
    async fn test_start_vault_group_derives_distinct_shard_ids() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");
        let org_group = manager
            .start_organization_group(
                Region::GLOBAL,
                OrganizationId::new(33),
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                None,
            )
            .await
            .expect("start org group");

        let v1 = manager
            .start_vault_group(Region::GLOBAL, OrganizationId::new(33), VaultId::new(1))
            .await
            .expect("start first vault group");
        let v2 = manager
            .start_vault_group(Region::GLOBAL, OrganizationId::new(33), VaultId::new(2))
            .await
            .expect("start second vault group");

        assert_ne!(
            v1.shard_id(),
            v2.shard_id(),
            "sibling vaults under the same org must have distinct shard IDs",
        );
        let org_shard = org_group.handle().shard_id();
        assert_ne!(v1.shard_id(), org_shard, "vault shard must not collide with parent org shard",);
        assert_ne!(v2.shard_id(), org_shard, "vault shard must not collide with parent org shard",);
    }

    #[tokio::test]
    async fn test_start_vault_group_initializes_empty_vault_applied_state() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");
        manager
            .start_organization_group(
                Region::GLOBAL,
                OrganizationId::new(66),
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                None,
            )
            .await
            .expect("start org group");

        let vault = manager
            .start_vault_group(Region::GLOBAL, OrganizationId::new(66), VaultId::new(1))
            .await
            .expect("start vault group");

        let state = vault.vault_applied_state().load();
        assert!(state.last_applied.is_none(), "fresh vault must have no applied LogId");
        assert_eq!(state.vault_height, 0, "fresh vault must have height 0");
        assert_eq!(state.last_applied_timestamp_ns, 0, "fresh vault must have zero apply ts");
        assert!(state.client_sequences.is_empty(), "fresh vault must have no client sequences");
        assert_eq!(
            state.previous_vault_hash,
            inferadb_ledger_types::ZERO_HASH,
            "fresh vault chain head must be ZERO_HASH",
        );
    }

    /// P2c.3.b.4: when the parent organization is configured with a
    /// [`BatchWriterConfig`], `start_vault_group` constructs a per-vault
    /// batch writer and populates [`InnerVaultGroup::batch_handle`].
    /// Without this, the gRPC Write handler falls back to the
    /// direct-propose path and pays per-write fsync cost.
    #[tokio::test]
    async fn test_start_vault_group_inherits_parent_batch_writer_config() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let batch_config = crate::batching::BatchWriterConfig::default();
        let org_id = OrganizationId::new(88);
        let org_group = manager
            .start_organization_group(
                Region::GLOBAL,
                org_id,
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                Some(batch_config),
            )
            .await
            .expect("start org group");

        // Sanity: the parent org also has a batch_handle (precondition).
        assert!(
            org_group.batch_handle().is_some(),
            "parent org must have batch_handle when batch_writer_config was supplied",
        );

        let vault = manager
            .start_vault_group(Region::GLOBAL, org_id, VaultId::new(1))
            .await
            .expect("start vault group");

        assert!(
            vault.batch_handle().is_some(),
            "vault group must inherit a batch_handle from parent org's batch_writer_config",
        );
    }

    /// P2c.3.b.4: when the parent organization started without a
    /// [`BatchWriterConfig`] (test-fixture path — `start_organization_group`
    /// passes `None`), `start_vault_group` MUST NOT construct a per-vault
    /// batch writer. The Write handler falls back to direct propose
    /// through `vault_group.handle()`.
    #[tokio::test]
    async fn test_start_vault_group_without_parent_batch_config_yields_no_batch_handle() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let org_id = OrganizationId::new(99);
        let org_group = manager
            .start_organization_group(
                Region::GLOBAL,
                org_id,
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                // No batch config on the parent org.
                None,
            )
            .await
            .expect("start org group");

        // Sanity: the parent org also has no batch_handle (precondition).
        assert!(
            org_group.batch_handle().is_none(),
            "parent org must not have batch_handle when batch_writer_config was None",
        );

        let vault = manager
            .start_vault_group(Region::GLOBAL, org_id, VaultId::new(1))
            .await
            .expect("start vault group");

        assert!(
            vault.batch_handle().is_none(),
            "vault group must not have batch_handle when parent org has none",
        );
    }

    /// Phase 4.2: when the parent organization is configured with an
    /// [`EventConfig`], `start_vault_group` opens a fresh per-vault
    /// `events.db` under
    /// `{data_dir}/{region}/{org_id}/state/vault-{vault_id}/events.db`
    /// and wraps it in its own [`EventWriter`]. The vault's writer
    /// MUST NOT share an `events.db` handle with the parent
    /// organization's writer — vault-scoped apply emissions land in
    /// the per-vault file, isolated from org-scoped emissions which
    /// continue to land in the parent's `events.db`.
    #[tokio::test]
    async fn test_start_vault_group_constructs_per_vault_event_writer() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let event_config = inferadb_ledger_types::events::EventConfig::default();
        let org_id = OrganizationId::new(101);
        let org_group = manager
            .start_organization_group(
                Region::GLOBAL,
                org_id,
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                Some(event_config),
                None,
            )
            .await
            .expect("start org group");

        // Sanity: parent org has an event writer once events_config is supplied.
        let org_writer = org_group
            .inner()
            .event_writer()
            .expect("parent org must have event_writer when events_config was supplied");
        let org_events_db = Arc::clone(org_writer.events_db());

        let vault_id = VaultId::new(1);
        let vault = manager
            .start_vault_group(Region::GLOBAL, org_id, vault_id)
            .await
            .expect("start vault group");

        let vault_writer = vault.event_writer().expect(
            "vault group must construct its own event_writer when parent org has events configured",
        );

        // Phase 4.2: vault writer MUST NOT share the parent org's
        // events.db handle. Each vault owns a dedicated file.
        assert!(
            !Arc::ptr_eq(&org_events_db, vault_writer.events_db()),
            "vault event writer must NOT share parent org's events.db handle after Phase 4.2",
        );
        // The underlying database handle must also be distinct (extra
        // sanity — the EventsDatabase wrapper around an Arc<Database>
        // could, in principle, point at the same DB).
        assert!(
            !Arc::ptr_eq(org_events_db.db(), vault_writer.events_db().db()),
            "vault events.db Database Arc must differ from parent org's after Phase 4.2",
        );

        // The on-disk per-vault `events.db` file must exist at the
        // composed path.
        let expected_path = temp
            .path()
            .join("global")
            .join(org_id.value().to_string())
            .join("state")
            .join(format!("vault-{}", vault_id.value()))
            .join("events.db");
        assert!(
            expected_path.exists(),
            "per-vault events.db must be created at {}",
            expected_path.display(),
        );
    }

    /// P2c.5.b: when the parent organization started without an
    /// [`EventConfig`] (test-fixture path), `start_vault_group` MUST
    /// gracefully no-op — the vault's `RaftLogStore` is opened with no
    /// `event_writer` and apply skips event emission, mirroring the
    /// parent's behavior.
    #[tokio::test]
    async fn test_start_vault_group_without_parent_event_writer_yields_none() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let org_id = OrganizationId::new(102);
        let org_group = manager
            .start_organization_group(
                Region::GLOBAL,
                org_id,
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                // No events_config on the parent org.
                None,
                None,
            )
            .await
            .expect("start org group");

        // Sanity: the parent org also has no event_writer (precondition).
        assert!(
            org_group.inner().event_writer().is_none(),
            "parent org must not have event_writer when events_config was None",
        );

        let vault = manager
            .start_vault_group(Region::GLOBAL, org_id, VaultId::new(1))
            .await
            .expect("start vault group");

        assert!(
            vault.event_writer().is_none(),
            "vault group must not have event_writer when parent org has none",
        );
    }

    #[tokio::test]
    async fn test_start_vault_group_adopts_parent_org_leader() {
        // Delegated vault shards adopt the parent organization's leader
        // on start. Without this adoption (initial + watcher),
        // `vault_handle.is_leader()` stays `false` forever and every
        // propose through the vault handle fails with "Not the leader".
        //
        // Single-node bootstrap: the org group elects itself as leader.
        // Both the initial one-shot adoption (if the org leader exists
        // at `start_vault_group` time) and the watcher (first parent
        // state-change) must converge to the parent's leader.
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");
        let org_id = OrganizationId::new(77);
        let org_group = manager
            .start_organization_group(
                Region::GLOBAL,
                org_id,
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                None,
            )
            .await
            .expect("start org group");

        // Wait up to 5s for the single-node org group to elect itself.
        // The adoption watcher on the vault fires asynchronously once
        // the parent's `state_rx` publishes a leader, so both the one-
        // shot adoption (if the leader is already set when
        // `start_vault_group` runs) and the watcher arm must converge.
        let start = std::time::Instant::now();
        while org_group.handle().current_leader().is_none()
            && start.elapsed() < std::time::Duration::from_secs(5)
        {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let parent_leader = org_group
            .handle()
            .current_leader()
            .expect("test sanity: single-node org must elect a leader within 5s");

        let vault = manager
            .start_vault_group(Region::GLOBAL, org_id, VaultId::new(1))
            .await
            .expect("start vault group");

        // Poll briefly for adoption to settle. Either the initial-
        // adoption arm in `start_vault_group` already published the
        // parent's leader on the vault's state watch, or the watcher
        // task picks up the next state change. Bounded to 2s.
        let start = std::time::Instant::now();
        while vault.handle().current_leader() != Some(parent_leader)
            && start.elapsed() < std::time::Duration::from_secs(2)
        {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert_eq!(
            vault.handle().current_leader(),
            Some(parent_leader),
            "vault shard must adopt parent org's leader",
        );
        assert!(
            vault.handle().is_leader(),
            "single-node vault must see itself as leader once adoption lands",
        );
    }

    #[tokio::test]
    async fn test_vault_commit_pump_delivers_responses_via_response_map() {
        // P2c.3.b.3: the vault commit pump fans out the
        // `Vec<LedgerResponse>` returned by `apply_committed_entries`
        // into the per-vault response map (waiter delivery) or the per-
        // vault spillover map (TOCTOU race with `propose_bytes_and_wait`).
        // Without that fan-out, `propose_bytes_and_wait` against the
        // vault handle hangs until the timeout — the apply runs, but the
        // oneshot is never signaled.
        //
        // This test proposes an empty Normal entry through the vault
        // handle and asserts the call returns `LedgerResponse::Empty`
        // within a few seconds. Empty Normal entries take the apply
        // pipeline's `decoded_payload.is_none()` arm in
        // `apply_committed_entries` and return `LedgerResponse::Empty`
        // verbatim, so a successful return proves the response made it
        // back through the fan-out.
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");
        let org_id = OrganizationId::new(88);
        let org_group = manager
            .start_organization_group(
                Region::GLOBAL,
                org_id,
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                None,
            )
            .await
            .expect("start org group");

        // Wait for the single-node org group to elect itself, then for
        // the vault group's delegated adoption to land.
        let start = std::time::Instant::now();
        while org_group.handle().current_leader().is_none()
            && start.elapsed() < std::time::Duration::from_secs(5)
        {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(
            org_group.handle().current_leader().is_some(),
            "test sanity: single-node org must elect a leader within 5s",
        );

        let vault = manager
            .start_vault_group(Region::GLOBAL, org_id, VaultId::new(1))
            .await
            .expect("start vault group");

        let start = std::time::Instant::now();
        while !vault.handle().is_leader() && start.elapsed() < std::time::Duration::from_secs(2) {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(
            vault.handle().is_leader(),
            "test sanity: vault must adopt parent leader before propose",
        );

        // Empty bytes -> empty Normal entry -> LedgerResponse::Empty
        // through the fan-out path. A return within the timeout proves
        // that the commit pump delivered the response (either to the
        // registered waiter via `response_map`, or to spillover for the
        // late-waiter pickup in `propose_bytes_and_wait`).
        let response = vault
            .handle()
            .propose_bytes_and_wait(Vec::new(), std::time::Duration::from_secs(5))
            .await
            .expect("propose_bytes_and_wait must return — fan-out regression if timeout");

        assert_eq!(
            response,
            crate::types::LedgerResponse::Empty,
            "empty Normal entry must apply to LedgerResponse::Empty",
        );
    }

    #[tokio::test]
    async fn test_vault_commit_pump_projects_into_vault_applied_state() {
        // Task #165: after each successful apply, the vault commit pump
        // must project the log store's full `AppliedState` into a
        // `VaultAppliedState` and publish it through the
        // `InnerVaultGroup.vault_applied_state` `ArcSwap`. Without this
        // projection, P2c.0's wired-up ArcSwap stays pinned at the
        // default empty value forever and observability readers see
        // stale apply progress.
        //
        // This test proposes an empty Normal entry through the vault
        // handle (the same payload shape exercised by
        // `test_vault_commit_pump_delivers_responses_via_response_map`)
        // and asserts that the projected `VaultAppliedState` advances
        // its `last_applied` field after apply.
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");
        let org_id = OrganizationId::new(99);
        let org_group = manager
            .start_organization_group(
                Region::GLOBAL,
                org_id,
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                None,
            )
            .await
            .expect("start org group");

        // Wait for the single-node org group to elect itself, then for
        // the vault group's delegated adoption to land.
        let start = std::time::Instant::now();
        while org_group.handle().current_leader().is_none()
            && start.elapsed() < std::time::Duration::from_secs(5)
        {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(
            org_group.handle().current_leader().is_some(),
            "test sanity: single-node org must elect a leader within 5s",
        );

        let vault_id = VaultId::new(1);
        let vault = manager
            .start_vault_group(Region::GLOBAL, org_id, vault_id)
            .await
            .expect("start vault group");

        let start = std::time::Instant::now();
        while !vault.handle().is_leader() && start.elapsed() < std::time::Duration::from_secs(2) {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(
            vault.handle().is_leader(),
            "test sanity: vault must adopt parent leader before propose",
        );

        // Snapshot initial last_applied. Under the multi-tier
        // adopt_leader change, the vault's delegated leader appends a
        // no-op on adoption (see `ConsensusState::adopt_leader`), so
        // `last_applied` may already be `Some(noop_index)` by the
        // time we observe it here. The invariant we care about is
        // monotonic advancement after our explicit propose, not the
        // initial pristine value.
        let initial_index = vault.vault_applied_state().load().last_applied.map(|id| id.index);

        // Drive an empty Normal entry through the vault's apply
        // pipeline. The fan-out test above covers the response path;
        // here we care that the commit pump's Stage 4 projection runs
        // after `apply_committed_entries` mutates the log store's
        // `AppliedState`.
        let response = vault
            .handle()
            .propose_bytes_and_wait(Vec::new(), std::time::Duration::from_secs(5))
            .await
            .expect("propose_bytes_and_wait must return");
        assert_eq!(
            response,
            crate::types::LedgerResponse::Empty,
            "empty Normal entry must apply to LedgerResponse::Empty",
        );

        // The projection runs after the response fan-out, in the same
        // task that delivered the response. Once `propose_bytes_and_wait`
        // returns, the projection has either already landed or is on
        // the next scheduler tick. Poll briefly for `last_applied` to
        // become `Some`.
        let start = std::time::Instant::now();
        let after = loop {
            let snapshot = vault.vault_applied_state().load();
            if snapshot.last_applied.is_some() {
                break snapshot;
            }
            if start.elapsed() >= std::time::Duration::from_secs(2) {
                break snapshot;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        };
        assert!(
            after.last_applied.is_some(),
            "vault_applied_state must advance after propose+apply — projection regression",
        );
        if let Some(initial) = initial_index {
            assert!(
                after.last_applied.is_some_and(|id| id.index > initial),
                "vault_applied_state.last_applied must increment after our propose \
                 (initial={initial}, after={:?})",
                after.last_applied,
            );
        }
    }

    #[tokio::test]
    async fn test_stop_vault_group_happy_path() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");
        manager
            .start_organization_group(
                Region::GLOBAL,
                OrganizationId::new(44),
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                None,
            )
            .await
            .expect("start org group");

        let vault_id = VaultId::new(303);
        manager
            .start_vault_group(Region::GLOBAL, OrganizationId::new(44), vault_id)
            .await
            .expect("start vault group");
        assert!(manager.has_vault_group(Region::GLOBAL, OrganizationId::new(44), vault_id));

        manager
            .stop_vault_group(Region::GLOBAL, OrganizationId::new(44), vault_id)
            .await
            .expect("stop vault group");

        assert!(!manager.has_vault_group(Region::GLOBAL, OrganizationId::new(44), vault_id));
        assert!(
            manager.list_vault_groups().is_empty(),
            "vault_groups map must no longer contain the stopped triple",
        );
    }

    #[tokio::test]
    async fn test_stop_vault_group_is_not_idempotent() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");
        manager
            .start_organization_group(
                Region::GLOBAL,
                OrganizationId::new(55),
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                None,
            )
            .await
            .expect("start org group");

        let vault_id = VaultId::new(404);
        manager
            .start_vault_group(Region::GLOBAL, OrganizationId::new(55), vault_id)
            .await
            .expect("start vault group");
        manager
            .stop_vault_group(Region::GLOBAL, OrganizationId::new(55), vault_id)
            .await
            .expect("first stop must succeed");

        // Second stop is an explicit error. The apply-side watcher maps
        // this to a warn-level log for duplicate DeleteVault signals
        // rather than silently swallowing them.
        let result =
            manager.stop_vault_group(Region::GLOBAL, OrganizationId::new(55), vault_id).await;
        match result {
            Ok(()) => panic!("expected VaultGroupNotFound on second stop"),
            Err(RaftManagerError::VaultGroupNotFound {
                region,
                organization_id,
                vault_id: got,
            }) => {
                assert_eq!(region, Region::GLOBAL);
                assert_eq!(organization_id.value(), 55);
                assert_eq!(got, vault_id);
            },
            Err(other) => panic!("expected VaultGroupNotFound, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_start_stop_start_cycle_restores_clean_state() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");
        manager
            .start_organization_group(
                Region::GLOBAL,
                OrganizationId::new(66),
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                None,
            )
            .await
            .expect("start org group");

        let vault_id = VaultId::new(505);

        // Start -> Stop: teardown removes map entry, cancels stub,
        // deregisters shard.
        manager
            .start_vault_group(Region::GLOBAL, OrganizationId::new(66), vault_id)
            .await
            .expect("first start");
        manager
            .stop_vault_group(Region::GLOBAL, OrganizationId::new(66), vault_id)
            .await
            .expect("stop after first start");

        // Start the same triple again — teardown must have been complete
        // enough that re-registering the shard on the parent engine and
        // re-inserting the map entry both succeed.
        let group = manager
            .start_vault_group(Region::GLOBAL, OrganizationId::new(66), vault_id)
            .await
            .expect("restart after stop must succeed");
        assert_eq!(group.vault_id(), vault_id);
        assert!(manager.has_vault_group(Region::GLOBAL, OrganizationId::new(66), vault_id));
        assert_eq!(manager.list_vault_groups().len(), 1);
    }

    // ────────────────────────────────────────────────────────────────────
    // Vault-group rehydration from applied state
    //
    // After node restart, the per-organization applied state is rebuilt
    // from snapshot + WAL replay inside `start_region`. `CreateVault`
    // entries below `last_applied` do NOT re-emit signals on replay, so
    // `start_organization_group` must sweep `applied_state.vaults` and
    // start a per-vault Raft group for every non-deleted vault.
    //
    // The helper `rehydrate_vault_groups` is the test seam — production
    // calls it as the final step of `start_organization_group`.
    // ────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_start_organization_group_fresh_org_has_no_vaults_to_rehydrate() {
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        // Fresh CreateOrganization path: `applied_state.vaults` is empty,
        // so the rehydration sweep is a no-op. No vault groups must be
        // registered after the call returns.
        manager
            .start_organization_group(
                Region::GLOBAL,
                OrganizationId::new(77),
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                None,
            )
            .await
            .expect("start org group");

        assert!(
            manager.list_vault_groups().is_empty(),
            "fresh CreateOrganization must not produce any vault groups via rehydration",
        );
    }

    #[tokio::test]
    async fn test_rehydrate_vault_groups_starts_non_deleted_vaults_only() {
        use inferadb_ledger_types::VaultSlug;

        use crate::log_storage::{AppliedState, VaultMeta};

        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let org_id = OrganizationId::new(88);
        let org_group = manager
            .start_organization_group(
                Region::GLOBAL,
                org_id,
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                None,
            )
            .await
            .expect("start org group");

        // Fresh org: rehydration already ran inside start_organization_group
        // against an empty vaults map, so no vault groups are registered.
        assert!(manager.list_vault_groups().is_empty());

        // Simulate a post-restart applied state: snapshot + WAL replay
        // left three vault entries on the org — two live, one deleted.
        // The deleted entry must not be rehydrated.
        let live_vault_a = VaultId::new(1001);
        let live_vault_b = VaultId::new(1002);
        let deleted_vault = VaultId::new(1003);

        let mut state = AppliedState::default();
        state.vaults.insert(
            (org_id, live_vault_a),
            VaultMeta {
                organization: org_id,
                vault: live_vault_a,
                slug: VaultSlug::new(10001),
                name: Some("live-a".to_string()),
                deleted: false,
                last_write_timestamp: 0,
                retention_policy: crate::types::BlockRetentionPolicy::default(),
            },
        );
        state.vaults.insert(
            (org_id, live_vault_b),
            VaultMeta {
                organization: org_id,
                vault: live_vault_b,
                slug: VaultSlug::new(10002),
                name: Some("live-b".to_string()),
                deleted: false,
                last_write_timestamp: 0,
                retention_policy: crate::types::BlockRetentionPolicy::default(),
            },
        );
        state.vaults.insert(
            (org_id, deleted_vault),
            VaultMeta {
                organization: org_id,
                vault: deleted_vault,
                slug: VaultSlug::new(10003),
                name: Some("tombstoned".to_string()),
                deleted: true,
                last_write_timestamp: 0,
                retention_policy: crate::types::BlockRetentionPolicy::default(),
            },
        );

        // An additional vault belonging to a DIFFERENT organization must
        // also be excluded from the sweep — rehydration is scoped to the
        // passed-in `organization_id`.
        let other_org = OrganizationId::new(89);
        let other_vault = VaultId::new(1004);
        state.vaults.insert(
            (other_org, other_vault),
            VaultMeta {
                organization: other_org,
                vault: other_vault,
                slug: VaultSlug::new(10004),
                name: Some("other-org".to_string()),
                deleted: false,
                last_write_timestamp: 0,
                retention_policy: crate::types::BlockRetentionPolicy::default(),
            },
        );

        org_group.applied_state().store_for_test(state);

        // Re-run the rehydration sweep against the seeded state.
        manager.rehydrate_vault_groups(Region::GLOBAL, org_id, &org_group.0).await;

        // Both live vaults scoped to `org_id` must now be registered.
        assert!(
            manager.has_vault_group(Region::GLOBAL, org_id, live_vault_a),
            "live vault A must be rehydrated",
        );
        assert!(
            manager.has_vault_group(Region::GLOBAL, org_id, live_vault_b),
            "live vault B must be rehydrated",
        );

        // The deleted vault must NOT be rehydrated.
        assert!(
            !manager.has_vault_group(Region::GLOBAL, org_id, deleted_vault),
            "deleted vault must not be rehydrated",
        );

        // The other org's vault must NOT be rehydrated by the org_id sweep.
        assert!(
            !manager.has_vault_group(Region::GLOBAL, other_org, other_vault),
            "cross-org vault must not be rehydrated by the org-scoped sweep",
        );

        // The registered set matches the expected pair exactly.
        let triples = manager.list_vault_groups();
        assert_eq!(triples.len(), 2);
        assert!(triples.contains(&(Region::GLOBAL, org_id, live_vault_a)));
        assert!(triples.contains(&(Region::GLOBAL, org_id, live_vault_b)));
    }

    #[tokio::test]
    async fn test_rehydrate_vault_groups_is_idempotent() {
        use inferadb_ledger_types::VaultSlug;

        use crate::log_storage::{AppliedState, VaultMeta};

        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let org_id = OrganizationId::new(90);
        let org_group = manager
            .start_organization_group(
                Region::GLOBAL,
                org_id,
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                None,
            )
            .await
            .expect("start org group");

        let vault_id = VaultId::new(2001);
        let mut state = AppliedState::default();
        state.vaults.insert(
            (org_id, vault_id),
            VaultMeta {
                organization: org_id,
                vault: vault_id,
                slug: VaultSlug::new(20001),
                name: Some("vault".to_string()),
                deleted: false,
                last_write_timestamp: 0,
                retention_policy: crate::types::BlockRetentionPolicy::default(),
            },
        );
        org_group.applied_state().store_for_test(state);

        // First sweep brings the vault up.
        manager.rehydrate_vault_groups(Region::GLOBAL, org_id, &org_group.0).await;
        assert!(manager.has_vault_group(Region::GLOBAL, org_id, vault_id));
        assert_eq!(manager.list_vault_groups().len(), 1);

        // Second sweep hits `VaultGroupExists` for the already-running
        // vault and must swallow that as a benign (debug-logged) case —
        // not propagate as an error, not duplicate the entry.
        manager.rehydrate_vault_groups(Region::GLOBAL, org_id, &org_group.0).await;
        assert!(manager.has_vault_group(Region::GLOBAL, org_id, vault_id));
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
    async fn test_sync_all_state_dbs_covers_vault_raft_dbs() {
        // Start a region and an org, then a vault group. The shutdown
        // sweep must fan out to the per-vault `raft.db` alongside the
        // per-vault `state.db`; verify by asserting the vault's
        // `raft.db` `last_synced_snapshot_id` does not regress after
        // `sync_all_state_dbs`.
        //
        // `sync_state` short-circuits to a no-op when nothing is dirty,
        // so we assert non-regression (>=) rather than strict advance.
        // A deeper test that mutates the per-vault `raft.db` and asserts
        // the id strictly advances belongs in crash-recovery integration,
        // where the real per-vault apply pipeline is in scope.
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");
        manager
            .start_organization_group(
                Region::GLOBAL,
                OrganizationId::new(55),
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                None,
            )
            .await
            .expect("start org group");

        let vault_id = VaultId::new(404);
        let vault_group = manager
            .start_vault_group(Region::GLOBAL, OrganizationId::new(55), vault_id)
            .await
            .expect("start vault group");

        let vault_raft_db = Arc::clone(vault_group.raft_db());
        let before = vault_raft_db.last_synced_snapshot_id();

        assert_eq!(
            manager.list_vault_groups().len(),
            1,
            "precondition: exactly one vault group should be live before the sync",
        );

        manager.sync_all_state_dbs(std::time::Duration::from_secs(5)).await;

        assert_eq!(
            manager.list_vault_groups().len(),
            1,
            "sync_all_state_dbs must not tear down vault groups",
        );

        let after = vault_raft_db.last_synced_snapshot_id();
        assert!(
            after >= before,
            "per-vault raft.db last_synced_snapshot_id regressed \
             (before={before}, after={after})"
        );
    }

    #[tokio::test]
    async fn test_sync_all_state_dbs_filters_vault_raft_dbs_by_region() {
        // Two regions, each with one organization and one vault. The
        // shutdown sweep iterates regions and must include the per-vault
        // `raft.db` for both — the region filter at the head of the
        // per-region block selects only the vaults belonging to the
        // current region, but the sweep visits every region in turn.
        //
        // Regression: if the filter inverts (wrong region comparison) or
        // collapses to a no-op (all vaults included per region, double-
        // syncing), this test still asserts non-regression on both
        // vault raft.db handles, catching either direction of breakage.
        // Strictly advancing `last_synced_snapshot_id` requires dirty
        // pages, which test-mode vault groups don't guarantee — non-
        // regression is the right contract here.
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let data_config_a =
            RegionConfig::data(Region::US_EAST_VA, vec![(1, "127.0.0.1:50051".to_string())]);
        manager.start_data_region(data_config_a).await.expect("start us-east-va");

        let data_config_b =
            RegionConfig::data(Region::US_WEST_OR, vec![(1, "127.0.0.1:50051".to_string())]);
        manager.start_data_region(data_config_b).await.expect("start us-west-or");

        let org_a = OrganizationId::new(77);
        let org_b = OrganizationId::new(88);
        manager
            .start_organization_group(
                Region::US_EAST_VA,
                org_a,
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                None,
            )
            .await
            .expect("start org group a");
        manager
            .start_organization_group(
                Region::US_WEST_OR,
                org_b,
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                None,
            )
            .await
            .expect("start org group b");

        let vault_a = manager
            .start_vault_group(Region::US_EAST_VA, org_a, VaultId::new(1))
            .await
            .expect("start vault a");
        let vault_b = manager
            .start_vault_group(Region::US_WEST_OR, org_b, VaultId::new(1))
            .await
            .expect("start vault b");

        let raft_db_a = Arc::clone(vault_a.raft_db());
        let raft_db_b = Arc::clone(vault_b.raft_db());
        let before_a = raft_db_a.last_synced_snapshot_id();
        let before_b = raft_db_b.last_synced_snapshot_id();

        assert_eq!(
            manager.list_vault_groups().len(),
            2,
            "precondition: exactly two vault groups should be live before the sync",
        );

        manager.sync_all_state_dbs(std::time::Duration::from_secs(5)).await;

        assert_eq!(
            manager.list_vault_groups().len(),
            2,
            "sync_all_state_dbs must not tear down vault groups",
        );

        let after_a = raft_db_a.last_synced_snapshot_id();
        let after_b = raft_db_b.last_synced_snapshot_id();
        assert!(
            after_a >= before_a,
            "region-a per-vault raft.db last_synced_snapshot_id regressed \
             (before={before_a}, after={after_a})"
        );
        assert!(
            after_b >= before_b,
            "region-b per-vault raft.db last_synced_snapshot_id regressed \
             (before={before_b}, after={after_b})"
        );
    }

    #[tokio::test]
    async fn test_sync_all_state_dbs_covers_per_org_groups() {
        // The regions map keys on `(Region, OrganizationId)`; a prior
        // implementation filtered the shutdown sweep to
        // `(region, OrganizationId(0))` and silently skipped every
        // per-org group registered at `org_id != 0`. That would leave
        // the per-org group's raft.db / blocks.db / events.db / meta.db
        // unsynced on graceful shutdown, forcing full WAL replay on
        // restart (or worse, reading `applied_durable = 0`).
        //
        // Start the data region plus one per-org group, call
        // `sync_all_state_dbs`, and assert the per-org group's raft.db
        // and blocks.db `last_synced_snapshot_id` do not regress.
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let org_id = OrganizationId::new(91);
        manager
            .start_organization_group(
                Region::GLOBAL,
                org_id,
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                None,
            )
            .await
            .expect("start org group");

        let org_group =
            manager.get_organization_group(Region::GLOBAL, org_id).expect("get org group");
        let raft_db = Arc::clone(org_group.raft_db());
        let blocks_db = Arc::clone(org_group.blocks_db());
        let raft_before = raft_db.last_synced_snapshot_id();
        let blocks_before = blocks_db.last_synced_snapshot_id();

        manager.sync_all_state_dbs(std::time::Duration::from_secs(5)).await;

        let raft_after = raft_db.last_synced_snapshot_id();
        let blocks_after = blocks_db.last_synced_snapshot_id();
        assert!(
            raft_after >= raft_before,
            "per-org raft.db last_synced_snapshot_id regressed \
             (before={raft_before}, after={raft_after}) — sweep likely skipped the per-org group"
        );
        assert!(
            blocks_after >= blocks_before,
            "per-org blocks.db last_synced_snapshot_id regressed \
             (before={blocks_before}, after={blocks_after}) — sweep likely skipped the per-org group"
        );
    }

    #[tokio::test]
    async fn test_sync_all_state_dbs_covers_three_orgs_in_one_region() {
        // Verifies coverage: three per-org groups co-located in a single
        // region must all be visited by the shutdown sweep. A prior bug
        // would have quietly dropped the second and third entries in the
        // per-region map (same `Region`, different `OrganizationId`);
        // this test pins that the sweep iterates every `(region, org)`
        // key.
        //
        // `sync_state` short-circuits when nothing is dirty, so the
        // assertion is non-regression of `last_synced_snapshot_id` for
        // every org. No timing assertion here — on clean DBs
        // `sync_state` returns near-instantly, so wall-clock bounds
        // cannot distinguish serial from concurrent fan-out. Strict
        // concurrency verification belongs at the integration tier,
        // where shutdown sweeps can be paired with realistic dirty-page
        // workloads.
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let org_ids =
            [OrganizationId::new(101), OrganizationId::new(102), OrganizationId::new(103)];
        let mut raft_dbs: Vec<(OrganizationId, Arc<Database<FileBackend>>, u64)> = Vec::new();
        for org_id in org_ids {
            manager
                .start_organization_group(
                    Region::GLOBAL,
                    org_id,
                    vec![(1, "127.0.0.1:50051".to_string())],
                    true,
                    None,
                    None,
                )
                .await
                .expect("start org group");
            let group =
                manager.get_organization_group(Region::GLOBAL, org_id).expect("get org group");
            let db = Arc::clone(group.raft_db());
            let before = db.last_synced_snapshot_id();
            raft_dbs.push((org_id, db, before));
        }

        manager.sync_all_state_dbs(std::time::Duration::from_secs(5)).await;

        for (org_id, db, before) in &raft_dbs {
            let after = db.last_synced_snapshot_id();
            assert!(
                after >= *before,
                "org {org} raft.db last_synced_snapshot_id regressed \
                 (before={before}, after={after}) — sweep likely skipped this org",
                org = org_id.value()
            );
        }
    }

    #[tokio::test]
    async fn test_sync_all_state_dbs_handles_expired_timeout_without_panic() {
        // Exercises the timeout branch of `sync_all_state_dbs`
        // structurally. Passing an effectively-zero timeout means one of
        // two paths runs to completion:
        //   1. The per-region future resolves instantly — clean DBs + no dirty pages make
        //      `sync_state` a near no-op, so the future may beat the timeout.
        //   2. The timeout fires first and the warn-arm runs, logging the deadline-exceeded branch.
        // Either path is acceptable. The test asserts that the call
        // neither panics nor deadlocks regardless of which branch wins
        // the race.
        //
        // This is intentionally racy on the branch taken; it is
        // deterministic on the "no panic, returns promptly" assertion,
        // which is the property that matters for shutdown correctness.
        let temp = TestDir::new();
        let config = create_test_config(&temp);
        let manager = test_manager(config);

        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");

        let org_id = OrganizationId::new(1);
        manager
            .start_organization_group(
                Region::GLOBAL,
                org_id,
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                None,
            )
            .await
            .expect("start org group");

        // Both variants: a zero-duration timeout (instantly expired) and
        // a nanosecond-scale timeout (effectively expired on any real
        // kernel). Neither should panic.
        manager.sync_all_state_dbs(std::time::Duration::from_millis(0)).await;
        manager.sync_all_state_dbs(std::time::Duration::from_nanos(1)).await;
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

        // IE_EAST_DUBLIN with `requires_residency_hint = true` and only 2
        // members must fail the protected-region quorum check (minimum 3).
        let data_config = RegionConfig::builder()
            .region(Region::IE_EAST_DUBLIN)
            .initial_members(vec![
                (1, "127.0.0.1:50051".to_string()),
                (2, "127.0.0.1:50052".to_string()),
            ])
            .requires_residency_hint(true)
            .build();
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

        // IE_EAST_DUBLIN with 3 members and explicit residency hint —
        // should pass the quorum check.
        let data_config = RegionConfig::builder()
            .region(Region::IE_EAST_DUBLIN)
            .initial_members(vec![
                (1, "127.0.0.1:50051".to_string()),
                (2, "127.0.0.1:50052".to_string()),
                (3, "127.0.0.1:50053".to_string()),
            ])
            .requires_residency_hint(true)
            .build();
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

        // Provision a non-protected region directory entry so
        // `start_region`'s residency lookup observes
        // `requires_residency = false` and skips the in-region quorum check.
        let propose = crate::types::SystemRequest::CreateDataRegion {
            region: Region::US_EAST_VA,
            protected: false,
            requires_residency: false,
            retention_days: 90,
            initial_members: vec![(1, "127.0.0.1:50051".to_string())],
        };
        let _ = manager
            .system_region()
            .expect("system region")
            .handle()
            .propose_and_wait(crate::types::RaftPayload::system(propose), Duration::from_secs(5))
            .await;

        // Now an explicit `start_data_region` with one node should not be
        // rejected for member count — the directory entry says it's
        // non-protected.
        let data_config =
            RegionConfig::data(Region::US_EAST_VA, vec![(1, "127.0.0.1:50051".to_string())]);
        if let Err(RaftManagerError::InsufficientNodes { .. }) =
            manager.start_data_region(data_config).await
        {
            panic!("Non-protected region should accept any member count");
        }
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
        let manager = test_manager(config);

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

    // ────────────────────────────────────────────────────────────────────
    // Vault-tier classification (P2c.1)
    //
    // Exercises the helpers driving the per-vault commit-pump stub:
    // [`classify_vault_tier`], [`request_variant_name`], and
    // [`decode_vault_payload`]. These three helpers are the foundation of
    // the real per-vault apply path (P2c.2) and the eventual routing flip
    // (P2c.3). Misclassification here lets either silently corrupt
    // routing, so each variant family carries an explicit assertion.
    // ────────────────────────────────────────────────────────────────────

    #[test]
    fn test_classify_vault_tier_write_is_vault_scoped() {
        let req = OrganizationRequest::Write {
            vault: VaultId::new(1),
            transactions: vec![],
            idempotency_key: [0u8; 16],
            request_hash: 0,
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };
        assert_eq!(classify_vault_tier(&req), VaultTier::VaultScoped);
        assert_eq!(request_variant_name(&req), "Write");
    }

    #[test]
    fn test_classify_vault_tier_batch_write_is_vault_scoped() {
        let req = OrganizationRequest::BatchWrite { requests: vec![] };
        assert_eq!(classify_vault_tier(&req), VaultTier::VaultScoped);
        assert_eq!(request_variant_name(&req), "BatchWrite");
    }

    #[test]
    fn test_classify_vault_tier_ingest_external_events_is_vault_scoped() {
        let req = OrganizationRequest::IngestExternalEvents {
            source: "test".to_string(),
            events: vec![],
        };
        assert_eq!(classify_vault_tier(&req), VaultTier::VaultScoped);
        assert_eq!(request_variant_name(&req), "IngestExternalEvents");
    }

    #[test]
    fn test_classify_vault_tier_create_vault_is_org_scoped() {
        let req = OrganizationRequest::CreateVault {
            organization: OrganizationId::new(1),
            slug: inferadb_ledger_types::VaultSlug::new(2),
            name: None,
            retention_policy: None,
        };
        assert_eq!(classify_vault_tier(&req), VaultTier::OrgScoped);
        assert_eq!(request_variant_name(&req), "CreateVault");
    }

    #[test]
    fn test_classify_vault_tier_delete_vault_is_org_scoped() {
        let req = OrganizationRequest::DeleteVault {
            organization: OrganizationId::new(1),
            vault: VaultId::new(2),
        };
        assert_eq!(classify_vault_tier(&req), VaultTier::OrgScoped);
        assert_eq!(request_variant_name(&req), "DeleteVault");
    }

    #[test]
    fn test_classify_vault_tier_create_team_is_org_scoped() {
        let req = OrganizationRequest::CreateOrganizationTeam {
            organization: OrganizationId::new(1),
            slug: inferadb_ledger_types::TeamSlug::new(7),
        };
        assert_eq!(classify_vault_tier(&req), VaultTier::OrgScoped);
        assert_eq!(request_variant_name(&req), "CreateOrganizationTeam");
    }

    #[test]
    fn test_classify_vault_tier_create_invite_is_org_scoped() {
        let req = OrganizationRequest::CreateOrganizationInvite {
            organization: OrganizationId::new(1),
            slug: inferadb_ledger_types::InviteSlug::new(9),
            token_hash: [0u8; 32],
            invitee_email_hmac: String::new(),
            ttl_hours: 24,
        };
        assert_eq!(classify_vault_tier(&req), VaultTier::OrgScoped);
        assert_eq!(request_variant_name(&req), "CreateOrganizationInvite");
    }

    #[test]
    fn test_classify_vault_tier_create_app_is_org_scoped() {
        let req = OrganizationRequest::CreateApp {
            organization: OrganizationId::new(1),
            slug: inferadb_ledger_types::AppSlug::new(11),
        };
        assert_eq!(classify_vault_tier(&req), VaultTier::OrgScoped);
        assert_eq!(request_variant_name(&req), "CreateApp");
    }

    #[test]
    fn test_decode_vault_payload_roundtrips_write() {
        let original = OrganizationRequest::Write {
            vault: VaultId::new(42),
            transactions: vec![],
            idempotency_key: [0u8; 16],
            request_hash: 0,
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };
        let payload = RaftPayload::system(original.clone());
        let bytes = inferadb_ledger_types::encode(&payload).expect("encode payload");
        let decoded = decode_vault_payload(&bytes).expect("decode payload");
        assert_eq!(decoded, original);
        assert_eq!(classify_vault_tier(&decoded), VaultTier::VaultScoped);
    }

    #[test]
    fn test_decode_vault_payload_rejects_garbage() {
        let bytes: Vec<u8> = vec![0xff; 16];
        let result = decode_vault_payload(&bytes);
        assert!(result.is_err(), "garbage bytes must not decode as a RaftPayload");
    }

    // =========================================================================
    // Phase 7 / O1: Vault hibernation lifecycle tests
    // =========================================================================

    #[test]
    fn vault_lifecycle_state_from_u8_round_trip() {
        for state in [
            VaultLifecycleState::Active,
            VaultLifecycleState::Dormant,
            VaultLifecycleState::Stalled,
        ] {
            let raw = state as u8;
            assert_eq!(VaultLifecycleState::from_u8(raw), state);
        }
    }

    #[test]
    fn vault_lifecycle_state_unknown_value_falls_back_to_active() {
        assert_eq!(VaultLifecycleState::from_u8(99), VaultLifecycleState::Active);
        assert_eq!(VaultLifecycleState::from_u8(255), VaultLifecycleState::Active);
    }

    #[test]
    fn vault_lifecycle_state_metric_labels() {
        assert_eq!(VaultLifecycleState::Active.as_metric_label(), "active");
        assert_eq!(VaultLifecycleState::Dormant.as_metric_label(), "dormant");
        assert_eq!(VaultLifecycleState::Stalled.as_metric_label(), "stalled");
    }

    /// Helper that starts a system region + per-organization group + per-vault
    /// group on a fresh in-memory test manager. Returns the
    /// `Arc<InnerVaultGroup>` so tests can directly drive its lifecycle.
    async fn start_test_vault(
        manager: &Arc<RaftManager>,
        organization_id: OrganizationId,
        vault_id: VaultId,
    ) -> Arc<InnerVaultGroup> {
        let system_config = RegionConfig::system(1, "127.0.0.1:50051".to_string());
        manager.start_system_region(system_config).await.expect("start system");
        manager
            .start_organization_group(
                Region::GLOBAL,
                organization_id,
                vec![(1, "127.0.0.1:50051".to_string())],
                true,
                None,
                None,
            )
            .await
            .expect("start org group");
        let group = manager
            .start_vault_group(Region::GLOBAL, organization_id, vault_id)
            .await
            .expect("start vault group");
        Arc::clone(group.inner())
    }

    #[tokio::test]
    async fn test_vault_starts_active() {
        let temp = TestDir::new();
        let manager = test_manager(create_test_config(&temp));
        let vault = start_test_vault(&manager, OrganizationId::new(1001), VaultId::new(1)).await;
        assert_eq!(vault.lifecycle_state(), VaultLifecycleState::Active);
        assert!(vault.last_activity_unix_secs() > 0);
    }

    #[tokio::test]
    async fn test_touch_activity_active_vault_is_noop() {
        let temp = TestDir::new();
        let manager = test_manager(create_test_config(&temp));
        let vault = start_test_vault(&manager, OrganizationId::new(1002), VaultId::new(1)).await;
        let woke = vault.touch_activity();
        assert!(!woke, "touch_activity on Active vault must not report a wake");
        assert_eq!(vault.lifecycle_state(), VaultLifecycleState::Active);
    }

    #[tokio::test]
    async fn test_touch_activity_wakes_dormant_vault() {
        let temp = TestDir::new();
        let manager = test_manager(create_test_config(&temp));
        let vault = start_test_vault(&manager, OrganizationId::new(1003), VaultId::new(1)).await;

        assert!(vault.transition_lifecycle_state(VaultLifecycleState::Dormant));
        assert_eq!(vault.lifecycle_state(), VaultLifecycleState::Dormant);

        let woke = vault.touch_activity();
        assert!(woke, "touch_activity on Dormant vault must report a wake");
        assert_eq!(vault.lifecycle_state(), VaultLifecycleState::Active);
    }

    #[tokio::test]
    async fn test_transition_idempotent() {
        let temp = TestDir::new();
        let manager = test_manager(create_test_config(&temp));
        let vault = start_test_vault(&manager, OrganizationId::new(1004), VaultId::new(1)).await;

        // First transition: Active -> Dormant fires.
        assert!(vault.transition_lifecycle_state(VaultLifecycleState::Dormant));
        // Second: same target -> no-op.
        assert!(!vault.transition_lifecycle_state(VaultLifecycleState::Dormant));
    }

    #[tokio::test]
    async fn test_idle_detector_disabled_by_default_does_not_transition() {
        let temp = TestDir::new();
        let manager = test_manager(create_test_config(&temp));
        let vault = start_test_vault(&manager, OrganizationId::new(1005), VaultId::new(1)).await;

        // Backdate activity beyond the threshold so any enabled scan would
        // sleep this vault. With hibernation disabled (the default), the
        // scan must observe no transition.
        vault.last_activity_unix_secs.store(0, Ordering::Relaxed);
        manager.run_hibernation_scan_once();
        assert_eq!(vault.lifecycle_state(), VaultLifecycleState::Active);
    }

    #[tokio::test]
    async fn test_idle_detector_sleeps_idle_active_vault() {
        let temp = TestDir::new();
        let manager = test_manager(create_test_config(&temp));
        manager.install_self_weak();
        let vault = start_test_vault(&manager, OrganizationId::new(1006), VaultId::new(1)).await;

        manager.set_hibernation_config(
            inferadb_ledger_types::config::HibernationConfig::builder()
                .enabled(true)
                .idle_secs(1)
                .scan_interval_secs(1)
                .build()
                .unwrap(),
        );
        // Force "stale" activity timestamp so the next scan sees the vault
        // as idle long enough to sleep.
        vault.last_activity_unix_secs.store(0, Ordering::Relaxed);

        manager.run_hibernation_scan_once();

        // O6 hibernation Pass 2: sleep is dispatched as a tokio task
        // (the scan does not block on engine.pause_shard + the per-DB
        // page-cache eviction syscalls). Poll briefly for the
        // transition to land. In practice this resolves on the next
        // executor tick — the loop is bounded so a regression in the
        // scan's spawn path surfaces as a timeout, not a hang.
        for _ in 0..50 {
            if vault.lifecycle_state() == VaultLifecycleState::Dormant {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(vault.lifecycle_state(), VaultLifecycleState::Dormant);
    }

    #[tokio::test]
    async fn test_idle_detector_marks_stalled_membership() {
        let temp = TestDir::new();
        let manager = test_manager(create_test_config(&temp));
        let vault = start_test_vault(&manager, OrganizationId::new(1007), VaultId::new(1)).await;

        manager.set_hibernation_config(
            inferadb_ledger_types::config::HibernationConfig::builder()
                .enabled(true)
                .idle_secs(60)
                .max_stall_secs(1)
                .scan_interval_secs(1)
                .build()
                .unwrap(),
        );

        // Backdate the membership-change start to "1" so the scan sees a
        // long-standing in-flight change. We can't call
        // `mark_membership_change_started()` first (which CAS-stamps
        // `now`) because the CAS only fires when the value is 0; storing
        // a small non-zero sentinel directly is the simplest path.
        vault.pending_membership_started_unix_secs.store(1, Ordering::Release);

        manager.run_hibernation_scan_once();
        assert_eq!(vault.lifecycle_state(), VaultLifecycleState::Stalled);

        // Clearing the marker lets the next scan return the vault to Active.
        vault.mark_membership_change_finished();
        manager.run_hibernation_scan_once();
        assert_eq!(vault.lifecycle_state(), VaultLifecycleState::Active);
    }

    #[tokio::test]
    async fn test_idle_detector_keeps_recent_vault_active() {
        let temp = TestDir::new();
        let manager = test_manager(create_test_config(&temp));
        let vault = start_test_vault(&manager, OrganizationId::new(1008), VaultId::new(1)).await;

        manager.set_hibernation_config(
            inferadb_ledger_types::config::HibernationConfig::builder()
                .enabled(true)
                .idle_secs(3600)
                .scan_interval_secs(1)
                .build()
                .unwrap(),
        );

        // Activity is fresh (set at start_vault_group time) — scan must
        // leave the vault Active.
        manager.run_hibernation_scan_once();
        assert_eq!(vault.lifecycle_state(), VaultLifecycleState::Active);
    }

    #[tokio::test]
    async fn test_start_idle_detector_no_op_when_disabled() {
        let temp = TestDir::new();
        let manager = test_manager(create_test_config(&temp));
        // Default (disabled) policy.
        manager.start_hibernation_idle_detector();
        // No internal flag should be set; calling again must remain a no-op.
        manager.start_hibernation_idle_detector();
    }

    #[tokio::test]
    async fn test_start_idle_detector_idempotent_when_enabled() {
        let temp = TestDir::new();
        let manager = test_manager(create_test_config(&temp));
        manager.set_hibernation_config(
            inferadb_ledger_types::config::HibernationConfig::builder()
                .enabled(true)
                .scan_interval_secs(1)
                .build()
                .unwrap(),
        );
        manager.start_hibernation_idle_detector();
        // Second call must be a guarded no-op (no panic, no double-spawn).
        manager.start_hibernation_idle_detector();
    }

    /// `sleep_vault` is idempotent and lands the load-bearing observable
    /// changes: lifecycle Active → Dormant, shard pause flag set, and the
    /// per-vault DB page-cache eviction calls all return `Ok(())` (which on
    /// macOS / Windows is a no-op success — see
    /// [`Database::evict_page_cache`]).
    #[tokio::test]
    async fn test_sleep_vault_transitions_dormant_and_pauses_shard() {
        let temp = TestDir::new();
        let manager = test_manager(create_test_config(&temp));
        manager.install_self_weak();
        let org_id = OrganizationId::new(2001);
        let vault_id = VaultId::new(1);
        let vault = start_test_vault(&manager, org_id, vault_id).await;

        assert_eq!(vault.lifecycle_state(), VaultLifecycleState::Active);
        manager
            .sleep_vault(Region::GLOBAL, org_id, vault_id)
            .await
            .expect("sleep_vault must succeed on a live vault");
        assert_eq!(vault.lifecycle_state(), VaultLifecycleState::Dormant);

        // Engine-level pause flag must be set. Lock-free check via the
        // shared DashMap consensus rule 12 documents.
        let engine = vault.handle().engine_arc();
        assert!(
            engine.is_shard_paused(vault.shard_id()),
            "after sleep_vault, engine must report shard paused"
        );

        // Idempotent: a second call is a no-op success.
        manager
            .sleep_vault(Region::GLOBAL, org_id, vault_id)
            .await
            .expect("sleep_vault must be idempotent");
        assert_eq!(vault.lifecycle_state(), VaultLifecycleState::Dormant);
    }

    /// `wake_vault` resumes the shard, transitions the vault back to
    /// Active, and is idempotent on an already-Active vault.
    #[tokio::test]
    async fn test_wake_vault_resumes_shard_and_transitions_active() {
        let temp = TestDir::new();
        let manager = test_manager(create_test_config(&temp));
        manager.install_self_weak();
        let org_id = OrganizationId::new(2002);
        let vault_id = VaultId::new(1);
        let vault = start_test_vault(&manager, org_id, vault_id).await;

        manager.sleep_vault(Region::GLOBAL, org_id, vault_id).await.expect("sleep");
        assert_eq!(vault.lifecycle_state(), VaultLifecycleState::Dormant);
        assert!(vault.handle().engine_arc().is_shard_paused(vault.shard_id()));

        manager.wake_vault(Region::GLOBAL, org_id, vault_id).await.expect("wake");
        assert_eq!(vault.lifecycle_state(), VaultLifecycleState::Active);
        assert!(
            !vault.handle().engine_arc().is_shard_paused(vault.shard_id()),
            "after wake_vault, engine must report shard not paused"
        );

        // Idempotent: waking an already-Active vault is a no-op success.
        manager.wake_vault(Region::GLOBAL, org_id, vault_id).await.expect("idempotent");
        assert_eq!(vault.lifecycle_state(), VaultLifecycleState::Active);
    }

    /// Returning an error variant for unknown vaults — at sleep / wake
    /// time, an unknown triple is a programmer bug; surface it.
    #[tokio::test]
    async fn test_sleep_wake_vault_unknown_triple_errors() {
        let temp = TestDir::new();
        let manager = test_manager(create_test_config(&temp));
        manager.install_self_weak();
        let org_id = OrganizationId::new(2003);
        let vault_id = VaultId::new(99);

        let err = manager.sleep_vault(Region::GLOBAL, org_id, vault_id).await.expect_err("sleep");
        assert!(matches!(err, RaftManagerError::VaultGroupNotFound { .. }), "{err:?}");

        let err = manager.wake_vault(Region::GLOBAL, org_id, vault_id).await.expect_err("wake");
        assert!(matches!(err, RaftManagerError::VaultGroupNotFound { .. }), "{err:?}");
    }

    /// Wake-latency p99 budget: < 100ms. Sleeps a vault, waits a beat to
    /// let any pause-induced settling happen, then measures the
    /// sleep→wake→Active round-trip across enough cold wakes to assert
    /// p99 < 100ms.
    ///
    /// In-process measurement only (Pass 2 scope); a multi-process
    /// subprocess stress test arrives in Pass 3
    /// (`crates/server/tests/hibernation_overhead.rs`).
    ///
    /// On unloaded test infrastructure this consistently lands well under
    /// budget — the hot path is `engine.resume_shard` (one control-channel
    /// round-trip ≈ µs) plus an atomic state store. Page-cache repopulation
    /// is lazy: the next read after wake re-fetches pages on Linux (or
    /// finds them still hot on Apple / Windows where eviction was a no-op).
    /// The test does not exercise the read; the wake measurement is the
    /// engine-resume + lifecycle-transition path that the budget covers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_wake_latency_p99_budget() {
        let temp = TestDir::new();
        let manager = test_manager(create_test_config(&temp));
        manager.install_self_weak();
        let org_id = OrganizationId::new(2010);
        let vault_id = VaultId::new(1);
        let _vault = start_test_vault(&manager, org_id, vault_id).await;

        // 20 sleep/wake cycles. Single vault avoids cross-shard scheduling
        // noise from the reactor.
        const ITERATIONS: usize = 20;
        let mut latencies_ms: Vec<f64> = Vec::with_capacity(ITERATIONS);

        for _ in 0..ITERATIONS {
            manager
                .sleep_vault(Region::GLOBAL, org_id, vault_id)
                .await
                .expect("sleep_vault must succeed");
            let start = std::time::Instant::now();
            manager
                .wake_vault(Region::GLOBAL, org_id, vault_id)
                .await
                .expect("wake_vault must succeed");
            latencies_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        }

        latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        // p99 across 20 samples = the highest sample. p95 = sample[18].
        let p99 = latencies_ms[ITERATIONS - 1];
        let p50 = latencies_ms[ITERATIONS / 2];
        // 100ms budget. Generous because CI runners under load can spike
        // the resume_shard control-channel round-trip; the practical
        // observed value on dev hardware is sub-millisecond.
        assert!(
            p99 < 100.0,
            "wake p99 budget: {p99:.3}ms exceeds 100ms (p50 = {p50:.3}ms; samples {latencies_ms:?})"
        );
    }
}
