//! InferaDB Ledger server library.
//!
//! Provides the core server functionality including configuration,
//! cluster bootstrap, and shutdown handling.

#![deny(unsafe_code)]
#![warn(missing_docs)]

/// Node bootstrap and cluster initialization.
pub mod bootstrap;
/// Server configuration and CLI argument parsing.
pub mod config;
/// Peer discovery via DNS and cached peer lists.
pub mod discovery;
/// Snowflake-based node ID generation and persistence.
pub mod node_id;
/// Graceful shutdown coordination.
pub mod shutdown;

/// Multi-node bootstrap coordination protocol.
pub mod coordinator;
