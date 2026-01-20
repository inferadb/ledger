//! InferaDB Ledger server library.
//!
//! Provides the core server functionality including configuration,
//! cluster bootstrap, and shutdown handling.

#![deny(unsafe_code)]

pub mod bootstrap;
pub mod config;
pub mod discovery;
pub mod node_id;
pub mod shutdown;

pub mod coordinator;
