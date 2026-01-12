//! Raft consensus and gRPC services for InferaDB Ledger.
//!
//! This crate provides:
//! - OpenRaft integration with redb log storage
//! - Combined RaftStorage implementation (log + state machine)
//! - gRPC services (Read, Write, Admin, Health, Discovery)
//! - Inter-node Raft network transport
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

mod auto_recovery;
pub mod batching;
mod block_compaction;
mod idempotency;
mod learner_refresh;
mod log_storage;
pub mod metrics;
mod orphan_cleanup;
pub mod pagination;
pub mod peer_maintenance;
pub mod peer_tracker;
pub mod proof;
mod raft_network;
mod rate_limit;
mod saga_orchestrator;
mod server;
pub mod services;
mod ttl_gc;
mod types;

/// Generated protobuf types and service traits.
pub mod proto {
    #![allow(clippy::all)]
    #![allow(missing_docs)]
    tonic::include_proto!("ledger.v1");
}

pub use auto_recovery::{AutoRecoveryConfig, AutoRecoveryJob, RecoveryResult};
pub use batching::{BatchConfig, BatchError, BatchWriter, BatchWriterHandle};
pub use learner_refresh::{CachedSystemState, LearnerRefreshConfig, LearnerRefreshJob};
pub use block_compaction::BlockCompactor;
pub use idempotency::IdempotencyCache;
pub use log_storage::{
    AppliedState, AppliedStateAccessor, NamespaceMeta, RaftLogStore, SequenceCounters,
    VaultHealthStatus, VaultMeta,
};
pub use orphan_cleanup::OrphanCleanupJob;
pub use pagination::{PageToken, PageTokenCodec, PageTokenError};
pub use peer_maintenance::PeerMaintenance;
pub use peer_tracker::{PeerTracker, PeerTrackerConfig};
pub use raft_network::{GrpcRaftNetwork, GrpcRaftNetworkFactory};
pub use rate_limit::{NamespaceRateLimiter, RateLimitExceeded};
pub use saga_orchestrator::SagaOrchestrator;
pub use server::LedgerServer;
pub use ttl_gc::TtlGarbageCollector;
pub use types::{
    BlockRetentionMode, BlockRetentionPolicy, LedgerNodeId, LedgerRequest, LedgerResponse,
    LedgerTypeConfig, SystemRequest,
};
