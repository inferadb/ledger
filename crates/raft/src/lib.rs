//! Orchestration layer between the consensus engine and the gRPC service layer.
//!
//! This crate is **not** Raft internals — those live in `inferadb-ledger-consensus`.
//! It turns committed proposals into state mutations, manages the lifecycle of Raft
//! groups at each tier, runs long-lived background jobs, and coordinates across nodes.
//!
//! # Four-Tier Architecture
//!
//! Every cluster runs four distinct Raft tiers:
//!
//! | Tier | Type | Scope | Request type |
//! |------|------|-------|--------------|
//! | Cluster control plane | [`raft_manager::SystemGroup`] | One per cluster (`GLOBAL` region) | `SystemRequest` |
//! | Regional control plane | [`raft_manager::RegionGroup`] | One per data region | `RegionRequest` |
//! | Organization data plane | [`raft_manager::OrganizationGroup`] | One per org per region | `OrganizationRequest` |
//! | Vault data plane | [`raft_manager::VaultGroup`] | One per vault | `OrganizationRequest` (vault-scoped variants) |
//!
//! Groups at each tier own disjoint operation sets and route through disjoint request
//! enums. Cross-tier routing is a compile error. Per-organization and per-vault groups
//! run under [`LeadershipMode::Delegated`](inferadb_ledger_consensus::LeadershipMode):
//! org groups follow the region leader; vault groups follow the parent org leader.
//!
//! # Apply Pipeline
//!
//! The [`apply_pool`] distributes committed
//! [`CommittedBatch`](inferadb_ledger_consensus::committed::CommittedBatch)es from the consensus
//! reactor across a fixed-size pool of [`apply_worker`]s. Each worker is typed to its tier's
//! request enum — cross-tier misrouting is rejected at compile time. Committed entries flow:
//! `Reactor → ApplyPool → ApplyWorker → RaftLogStore → StateLayer`.
//!
//! # Membership Cascade
//!
//! When a voter joins or leaves a region, [`region_membership_watcher`] observes the
//! region-group shard state, propagates the change to the parent org group, and enqueues
//! one [`membership_queue::MembershipChangeRequest`] per child vault into a bounded,
//! rate-limited [`membership_queue::MembershipQueue`]. The
//! [`region_membership_watcher::MembershipDispatcher`] drains the queue under a concurrency
//! cap and timeout, preventing snapshot storms at scale.
//!
//! # Saga Orchestrator
//!
//! [`saga_orchestrator::SagaOrchestrator`] drives cross-organization operations using
//! an eventual-consistency model. PII for in-flight sagas is stored only at the
//! `_tmp:saga_pii:{saga_id}` key (Regional tier, TTL-bound); completed sagas leave
//! only a global-tier audit record.
//!
//! # Durability Contract
//!
//! Writes are WAL-durable on response. The four per-region databases (state.db, raft.db,
//! blocks.db, events.db) materialize lazily via [`state_checkpointer::StateCheckpointer`]
//! (per-region, ~500ms default cadence). On crash, [`log_storage::RaftLogStore`] replays
//! the `(applied_durable, last_committed]` WAL window before the apply worker starts.
//! Handler-phase events batched via the [`event_writer`] flush queue are not WAL-backed;
//! they land durably on the next checkpointer tick.
//!
//! # Public API
//!
//! The stable public API surface consists of:
//! - [`metrics`] — Prometheus metric constants and recording helpers
//! - [`cardinality`] — cardinality-safe metric label helpers
//!
//! All other modules are server-internal infrastructure marked `#[doc(hidden)]`.
//! They may change without notice.
//!
//! # Security Model
//!
//! Ledger runs behind WireGuard VPN. Authentication and authorization are handled by
//! the Engine/Control services upstream. Ledger trusts all incoming gRPC requests.

#![deny(unsafe_code)]
#![warn(missing_docs)]
// Wire-shaped errors carry per-RPC details (correlation IDs, leader hints) that exceed clippy's
// large-result threshold; the alternative would be heap-allocating every Result.
#![allow(clippy::result_large_err)]

// ---------------------------------------------------------------------------
// Public modules — stable API surface for SDK and external consumers
// ---------------------------------------------------------------------------

pub mod cardinality;
pub mod metrics;

// ---------------------------------------------------------------------------
// Server-internal modules — implementation details hidden from `cargo doc`.
// These are `pub` so the server/services crates can access them, but
// `#[doc(hidden)]` keeps the documentation focused on SDK-facing types.
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub mod apply_command;
#[doc(hidden)]
pub mod apply_pool;
#[doc(hidden)]
pub mod apply_worker;
#[doc(hidden)]
pub mod backup;
#[doc(hidden)]
pub mod batching;
#[doc(hidden)]
pub mod commit_dispatcher;
#[doc(hidden)]
pub mod consensus_handle;
#[doc(hidden)]
pub mod deadline;
#[doc(hidden)]
pub mod dek_rewrap;
#[doc(hidden)]
pub mod dependency_health;
#[doc(hidden)]
pub mod entry_crypto;
#[doc(hidden)]
pub mod error;
#[doc(hidden)]
pub mod event_writer;
#[doc(hidden)]
pub mod graceful_shutdown;
#[doc(hidden)]
pub mod hot_key_detector;
#[doc(hidden)]
pub mod idempotency;
#[doc(hidden)]
pub mod integrity_scrubber;
#[doc(hidden)]
pub mod invite_maintenance;
#[doc(hidden)]
pub mod leader_lease;
#[doc(hidden)]
pub mod leader_transfer;
#[doc(hidden)]
pub mod log_storage;
#[doc(hidden)]
pub mod logging;
#[doc(hidden)]
pub mod membership_queue;
#[doc(hidden)]
pub mod node_registry;
#[doc(hidden)]
pub mod otel;
#[doc(hidden)]
pub mod pagination;
#[doc(hidden)]
pub mod peer_address_map;
#[doc(hidden)]
pub mod peer_tracker;
#[doc(hidden)]
pub mod proof;
#[doc(hidden)]
pub mod raft_manager;
#[doc(hidden)]
pub mod read_index;
#[doc(hidden)]
pub mod region_membership_watcher;
#[doc(hidden)]
pub mod resource_metrics;
#[doc(hidden)]
pub mod runtime_config;

#[doc(hidden)]
pub mod auto_recovery;
#[doc(hidden)]
pub mod block_compaction;
#[doc(hidden)]
pub mod btree_compaction;
#[doc(hidden)]
pub mod events_gc;
#[doc(hidden)]
pub mod file_lock;
#[doc(hidden)]
pub mod learner_refresh;
#[doc(hidden)]
pub mod organization_purge;
#[doc(hidden)]
pub mod orphan_cleanup;
#[doc(hidden)]
pub mod post_erasure_compaction;
#[doc(hidden)]
pub mod rate_limit;
#[doc(hidden)]
pub mod region_storage;
#[doc(hidden)]
pub mod saga_orchestrator;
#[doc(hidden)]
pub mod snapshot;
#[doc(hidden)]
pub mod snapshot_installer;
#[doc(hidden)]
pub mod snapshot_key_provider;
#[doc(hidden)]
pub mod snapshot_persister;
#[doc(hidden)]
pub mod state_checkpointer;
#[doc(hidden)]
pub mod state_root_verifier;
#[doc(hidden)]
pub mod token_maintenance;
#[doc(hidden)]
pub mod ttl_gc;
#[doc(hidden)]
pub mod types;
#[doc(hidden)]
pub mod user_retention;
#[doc(hidden)]
pub mod wire_consensus_transport;

// ---------------------------------------------------------------------------
// Server infrastructure re-exports — consumed by the server crate for
// bootstrapping, configuration, and background job management.
//
// These are `#[doc(hidden)]` to keep `cargo doc` focused on SDK types.
// Server code should prefer direct module paths (e.g.
// `inferadb_ledger_raft::graceful_shutdown::HealthState`) over these
// convenience re-exports.
// ---------------------------------------------------------------------------
#[doc(hidden)]
pub use apply_worker::{OrganizationApplyWorker, RegionApplyWorker, SystemApplyWorker};
#[doc(hidden)]
pub use auto_recovery::AutoRecoveryJob;
#[doc(hidden)]
pub use backup::{BackupJob, BackupManager, RestoreTrashSweepJob};
#[doc(hidden)]
pub use block_compaction::BlockCompactor;
#[doc(hidden)]
pub use commit_dispatcher::CommitDispatcher;
#[doc(hidden)]
pub use consensus_handle::{ConsensusHandle, HandleError, ResponseMap, SpilloverMap};
#[doc(hidden)]
pub use events_gc::EventsGarbageCollector;
#[doc(hidden)]
pub use graceful_shutdown::{
    BackgroundJobWatchdog, GracefulShutdown, HealthState, watchdog_now_nanos,
};
#[doc(hidden)]
pub use hot_key_detector::HotKeyDetector;
#[doc(hidden)]
pub use integrity_scrubber::IntegrityScrubberJob;
#[doc(hidden)]
pub use invite_maintenance::InviteMaintenanceJob;
#[doc(hidden)]
pub use leader_lease::LeaderLease;
#[doc(hidden)]
pub use learner_refresh::LearnerRefreshJob;
#[doc(hidden)]
pub use log_storage::{RaftLogStore, RecoveryStats};
#[doc(hidden)]
pub use organization_purge::OrganizationPurgeJob;
#[doc(hidden)]
pub use orphan_cleanup::OrphanCleanupJob;
#[doc(hidden)]
pub use peer_address_map::PeerAddressMap;
#[doc(hidden)]
pub use post_erasure_compaction::PostErasureCompactionJob;
#[doc(hidden)]
pub use raft_manager::{
    ConsensusTransportImpl, InnerGroup, OrganizationGroup, RaftManager, RaftManagerConfig,
    RegionConfig, RegionGroup, SystemGroup, SystemStateReader, VaultGroup, VaultLifecycleState,
};
#[doc(hidden)]
pub use rate_limit::RateLimiter;
#[doc(hidden)]
pub use read_index::wait_for_apply;
#[doc(hidden)]
pub use region_storage::{RegionStorage, RegionStorageManager};
#[doc(hidden)]
pub use resource_metrics::ResourceMetricsCollector;
#[doc(hidden)]
pub use runtime_config::RuntimeConfigHandle;
#[doc(hidden)]
pub use saga_orchestrator::{
    OnboardingPii, OrgPii, SagaOrchestrator, SagaOrchestratorHandle, SagaOutput, SagaPii,
    SagaSubmission,
};
#[doc(hidden)]
pub use snapshot_key_provider::{
    InMemorySnapshotKeyProvider, NoopSnapshotKeyProvider, SnapshotKeyProvider,
};
#[doc(hidden)]
pub use snapshot_persister::{
    DEFAULT_RETENTION_COUNT, DecryptedSnapshot, PersistError, PersistedSnapshotMeta,
    SnapshotPersister, StagedSnapshotMeta,
};
#[doc(hidden)]
pub use state_checkpointer::StateCheckpointer;
#[doc(hidden)]
pub use token_maintenance::TokenMaintenanceJob;
#[doc(hidden)]
pub use ttl_gc::TtlGarbageCollector;
#[doc(hidden)]
pub use types::LedgerNodeId;
#[doc(hidden)]
pub use types::LivenessConfig;
pub use types::NodeStatus;
#[doc(hidden)]
pub use types::{OrganizationRequest, RegionRequest};
#[doc(hidden)]
pub use user_retention::UserRetentionReaper;
