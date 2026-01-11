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

mod log_storage;
mod types;

pub use log_storage::{AppliedState, RaftLogStore, SequenceCounters, VaultHealthStatus};
pub use types::{
    LedgerNodeId, LedgerRequest, LedgerResponse, LedgerTypeConfig, SystemRequest,
};
