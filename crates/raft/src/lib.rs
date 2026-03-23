//! Raft consensus infrastructure for InferaDB Ledger.
//!
//! Provides:
//! - OpenRaft integration with inferadb-ledger-store log storage
//! - Combined RaftStorage implementation (log + state machine)
//! - Inter-node Raft network transport
//! - Transaction batching, rate limiting, and background jobs
//!
//! # Public API
//!
//! The stable public API surface consists of:
//! - [`trace_context`] — distributed tracing propagation helpers
//! - [`metrics`] — Prometheus metric constants and recording helpers
//! - [`LedgerTypeConfig`] — OpenRaft type configuration
//!
//! All other modules and re-exports are server-internal infrastructure
//! hidden from documentation. They may change without notice.
//!
//! ## Architecture Note
//!
//! OpenRaft 0.9 has sealed traits for `RaftLogStorage` and `RaftStateMachine` (v2 API).
//! We use the deprecated but non-sealed `RaftStorage` trait which combines both
//! log storage and state machine functionality into a single implementation.
//!
//! ## Security Model
//!
//! Ledger runs behind WireGuard VPN. Authentication and authorization are handled
//! by Engine/Control services upstream. Ledger trusts all incoming requests.

#![deny(unsafe_code)]
#![warn(missing_docs)]
// gRPC services return tonic::Status (176 bytes) - this is standard practice for gRPC error
// handling
#![allow(clippy::result_large_err)]

// ---------------------------------------------------------------------------
// Public modules — stable API surface for SDK and external consumers
// ---------------------------------------------------------------------------

pub mod metrics;
pub mod trace_context;

// ---------------------------------------------------------------------------
// Server-internal modules — implementation details hidden from `cargo doc`.
// These are `pub` so the server/services crates can access them, but
// `#[doc(hidden)]` keeps the documentation focused on SDK-facing types.
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub mod backup;
#[doc(hidden)]
pub mod batching;
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
pub mod leader_transfer;
#[doc(hidden)]
pub mod log_storage;
#[doc(hidden)]
pub mod logging;
#[doc(hidden)]
pub mod otel;
#[doc(hidden)]
pub mod pagination;
#[doc(hidden)]
pub mod peer_tracker;
#[doc(hidden)]
pub mod proof;
#[doc(hidden)]
pub mod raft_manager;
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
pub mod raft_network;
#[doc(hidden)]
pub mod rate_limit;
#[doc(hidden)]
pub mod region_router;
#[doc(hidden)]
pub mod region_storage;
#[doc(hidden)]
pub mod saga_orchestrator;
#[doc(hidden)]
pub mod snapshot;
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
pub use auto_recovery::AutoRecoveryJob;
#[doc(hidden)]
pub use backup::{BackupJob, BackupManager};
#[doc(hidden)]
pub use block_compaction::BlockCompactor;
#[doc(hidden)]
pub use events_gc::EventsGarbageCollector;
#[doc(hidden)]
pub use graceful_shutdown::{BackgroundJobWatchdog, GracefulShutdown, HealthState};
#[doc(hidden)]
pub use hot_key_detector::HotKeyDetector;
#[doc(hidden)]
pub use integrity_scrubber::IntegrityScrubberJob;
#[doc(hidden)]
pub use invite_maintenance::InviteMaintenanceJob;
#[doc(hidden)]
pub use learner_refresh::LearnerRefreshJob;
#[doc(hidden)]
pub use log_storage::RaftLogStore;
#[doc(hidden)]
pub use organization_purge::OrganizationPurgeJob;
#[doc(hidden)]
pub use orphan_cleanup::OrphanCleanupJob;
#[doc(hidden)]
pub use post_erasure_compaction::PostErasureCompactionJob;
#[doc(hidden)]
pub use raft_manager::{RaftManager, RaftManagerConfig, RegionConfig, RegionGroup};
#[doc(hidden)]
pub use raft_network::GrpcRaftNetworkFactory;
#[doc(hidden)]
pub use rate_limit::RateLimiter;
#[doc(hidden)]
pub use region_storage::{RegionStorage, RegionStorageManager};
#[doc(hidden)]
pub use resource_metrics::ResourceMetricsCollector;
#[doc(hidden)]
pub use runtime_config::RuntimeConfigHandle;
#[doc(hidden)]
pub use saga_orchestrator::{
    OnboardingPii, OrgPii, SagaOrchestrator, SagaOrchestratorHandle, SagaOutput, SagaSubmission,
};
#[doc(hidden)]
pub use token_maintenance::TokenMaintenanceJob;
#[doc(hidden)]
pub use ttl_gc::TtlGarbageCollector;
#[doc(hidden)]
pub use types::LedgerNodeId;
/// OpenRaft type configuration for the ledger's Raft consensus layer.
pub use types::LedgerTypeConfig;
#[doc(hidden)]
pub use user_retention::UserRetentionReaper;
