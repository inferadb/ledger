//! InferaDB Ledger Consensus Engine.
//!
//! A purpose-built multi-shard Raft consensus engine optimized for
//! high-frequency authorization workloads.

#![deny(unsafe_code)]

pub mod action;
pub mod bootstrap;
pub mod buggify;
pub mod circuit_breaker;
pub mod clock;
pub mod closed_ts;
pub mod committed;
pub mod config;
pub mod crypto;
pub mod engine;
pub mod error;
pub mod idempotency;
pub mod leadership;
pub mod lease;
pub mod message;
pub mod network_outbox;
pub mod reactor;
pub mod recovery;
pub mod rng;
pub mod router;
pub mod shard;
pub mod simulation;
pub mod snapshot_crypto;
pub mod snapshot_utils;
pub mod split;
pub mod state_machine;
pub mod timer;
pub mod transport;
pub mod types;
pub mod wal;
pub mod wal_backend;
pub mod zero_copy;

pub use action::Action;
pub use clock::{Clock, SystemClock};
pub use closed_ts::ClosedTimestampTracker;
pub use committed::{CommittedBatch, CommittedEntry};
pub use config::ShardConfig;
pub use engine::{ConsensusEngine, ConsensusEngineOptions};
pub use error::ConsensusError;
pub use leadership::ShardState;
pub use lease::LeaderLease;
pub use message::Message;
pub use rng::{RngSource, SystemRng};
pub use shard::Shard;
pub use state_machine::{ApplyResult, NoopStateMachine, SnapshotError, StateMachine};
pub use transport::{InMemoryTransport, NetworkTransport};
pub use types::*;
pub use wal_backend::{FsyncPhase, WalBackend};
pub use zero_copy::{ZeroCopyError, access_archived, from_archived_bytes, to_archived_bytes};
