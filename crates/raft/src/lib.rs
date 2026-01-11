//! Raft consensus and gRPC services for InferaDB Ledger.
//!
//! This crate provides:
//! - OpenRaft integration with redb log storage
//! - State machine implementation
//! - gRPC services (Read, Write, Admin, Health, Discovery)
//! - Inter-node Raft network transport

mod log_storage;
mod state_machine;
mod types;

pub use log_storage::RaftLogStore;
pub use state_machine::LedgerStateMachine;
pub use types::{
    LedgerNodeId, LedgerRequest, LedgerResponse, LedgerTypeConfig, SystemRequest,
};
