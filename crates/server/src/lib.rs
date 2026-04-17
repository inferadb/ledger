//! InferaDB Ledger server library.
//!
//! Core server functionality: configuration, cluster bootstrap, and shutdown handling.

#![deny(unsafe_code)]
#![warn(missing_docs)]

/// Node bootstrap and cluster initialization.
pub mod bootstrap;
/// Cluster ID persistence (sentinel for initialized state).
pub mod cluster_id;
/// Server configuration and CLI argument parsing.
pub mod config;
/// Multi-node bootstrap coordination protocol.
pub mod coordinator;
/// Peer discovery via DNS and cached peer lists.
pub mod discovery;
/// TiKV-style data region membership scheduler (checker + scheduler + operator).
pub mod dr_scheduler;
/// Snowflake-based node ID generation and persistence.
pub mod node_id;
/// Event-driven data region membership controller (TiKV-style Placement Driver).
pub mod placement;
/// Graceful shutdown coordination.
pub mod shutdown;
