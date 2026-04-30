//! InferaDB Ledger — custom in-house multi-shard Raft consensus engine.
//!
//! This crate owns the full consensus stack: state machine, event loop, WAL,
//! leader lease, snapshot pipeline, and deterministic simulation harness.
//! It does **not** depend on openraft or any external Raft library.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────┐
//! │  ConsensusEngine  (engine.rs)                        │
//! │  Public handle — propose, read_index, membership,    │
//! │  snapshot callbacks, pause/resume.                   │
//! │  Communicates with the reactor via mpsc channels.    │
//! └────────────────────┬─────────────────────────────────┘
//!                      │ ReactorEvent
//! ┌────────────────────▼─────────────────────────────────┐
//! │  Reactor  (reactor.rs)                               │
//! │  Single tokio task. Dispatches events to shards,     │
//! │  batches WAL writes (one fsync per batch), sends     │
//! │  messages through NetworkTransport, and delivers     │
//! │  CommittedBatch values to the apply worker.          │
//! └────────────────────┬─────────────────────────────────┘
//!                      │ Action values
//! ┌────────────────────▼─────────────────────────────────┐
//! │  ConsensusState  (consensus_state.rs)                │
//! │  Pure Raft state machine for one shard. Receives     │
//! │  events, returns Action values. Zero I/O.            │
//! └──────────────────────────────────────────────────────┘
//! ```
//!
//! # Key invariants
//!
//! - [`ConsensusState`] returns [`Action`] values and performs **no I/O**. Any blocking call inside
//!   it is a correctness bug.
//! - [`reactor::Reactor`] is the only place I/O happens. WAL writes are batched with a single
//!   `fsync` per batch — never per proposal.
//! - The production WAL is [`wal::segmented::SegmentedWalBackend`]: per-vault AES-256-GCM
//!   encrypted, segmented, with CRC32 integrity per frame.
//! - Snapshots are encrypted end-to-end via [`snapshot_crypto`] functions. Bypassing encryption
//!   defeats per-vault key rotation.
//! - Cross-shard state changes flow through [`ConsensusEngine`], never directly between
//!   [`ConsensusState`] instances.
//!
//! # Module map
//!
//! | Module | Role |
//! |---|---|
//! | [`engine`] | [`ConsensusEngine`] — public multi-shard handle |
//! | [`consensus_state`] | [`ConsensusState`] — pure Raft state machine |
//! | [`reactor`] | [`reactor::Reactor`] — single-task I/O event loop |
//! | [`action`] | [`Action`] — commands returned by the state machine |
//! | [`wal_backend`] | [`WalBackend`] trait + [`wal_backend::CheckpointFrame`] |
//! | [`wal`] | WAL implementations: segmented (production), encrypted, memory |
//! | [`lease`] | [`LeaderLease`] — quorum-renewed linearizable-read lease |
//! | [`leadership`] | [`ShardState`] — observable shard snapshot |
//! | [`closed_ts`] | [`ClosedTimestampTracker`] — follower stale-read gating |
//! | [`committed`] | [`CommittedBatch`] / [`CommittedEntry`] — apply-worker input |
//! | [`idempotency`] | [`idempotency::IdempotencyCache`] — pre-deduplication before Raft log |
//! | [`snapshot_crypto`] | AES-256-GCM snapshot envelope functions |
//! | [`snapshot_coordinator`] | [`SnapshotCoordinator`] — async snapshot build callback |
//! | [`snapshot_sender`] | [`SnapshotSender`] — async snapshot wire-transfer callback |
//! | [`snapshot_installer`] | [`SnapshotInstaller`] — async snapshot install callback |
//! | [`state_machine`] | [`StateMachine`] trait consumed by the apply worker |
//! | [`recovery`] | WAL-based crash recovery utilities |
//! | [`transport`] | [`NetworkTransport`] trait + in-memory test implementation |
//! | [`simulation`] | Deterministic multi-node simulation harness (no tokio) |
//! | [`wake`] | [`ShardWakeNotifier`] — hibernation wake-on-peer-message |
//! | [`circuit_breaker`] | Per-shard commit-liveness circuit breaker |
//! | [`split`] | Shard split/merge configuration and phase tracking |
//! | [`buggify`] | Fault injection (simulation only — never in production) |

#![deny(unsafe_code)]

pub mod action;
pub mod bootstrap;
pub mod buggify;
pub mod circuit_breaker;
pub mod clock;
pub mod closed_ts;
pub mod committed;
pub mod config;
pub mod consensus_state;
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
pub mod simulation;
pub mod snapshot_coordinator;
pub mod snapshot_crypto;
pub mod snapshot_installer;
pub mod snapshot_sender;
pub mod snapshot_utils;
pub mod split;
pub mod state_machine;
pub mod timer;
pub mod transport;
pub mod types;
pub mod wake;
pub mod wal;
pub mod wal_backend;
pub mod zero_copy;

pub use action::Action;
pub use clock::{Clock, SystemClock};
pub use closed_ts::ClosedTimestampTracker;
pub use committed::{CommittedBatch, CommittedEntry};
pub use config::ShardConfig;
pub use consensus_state::{ConsensusState, LeadershipMode};
pub use engine::ConsensusEngine;
pub use error::ConsensusError;
pub use leadership::ShardState;
pub use lease::LeaderLease;
pub use message::Message;
pub use rng::{RngSource, SystemRng};
pub use snapshot_coordinator::{NoopSnapshotCoordinator, SnapshotCoordinator};
pub use snapshot_installer::{NoopSnapshotInstaller, SnapshotInstaller};
pub use snapshot_sender::{NoopSnapshotSender, SnapshotSender};
pub use state_machine::{ApplyResult, NoopStateMachine, SnapshotError, StateMachine};
pub use transport::{InMemoryTransport, NetworkTransport};
pub use types::*;
pub use wake::{NoopShardWakeNotifier, ShardWakeNotifier};
pub use wal_backend::{FsyncPhase, WalBackend};
pub use zero_copy::{ZeroCopyError, access_archived, from_archived_bytes, to_archived_bytes};
