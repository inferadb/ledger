//! InferaDB Ledger server library.
//!
//! Exposes the core server components used by both the binary entrypoint and the
//! integration test binary: configuration ([`config`]), cluster bootstrap
//! ([`bootstrap`]), shutdown coordination ([`shutdown`]), cluster identity
//! ([`cluster_id`]), node identity ([`node_id`]), peer discovery ([`discovery`]),
//! data-region placement ([`placement`], [`dr_scheduler`]), and the two-phase
//! bootstrap coordinator stub ([`coordinator`]).

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
/// K8s `tcpSocket` health probe listener (Phase 0.0.E.8).
pub mod health_probe;
/// Snowflake-based node ID generation and persistence.
pub mod node_id;
/// Event-driven data region membership controller (TiKV-style Placement Driver).
pub mod placement;
/// Graceful shutdown coordination.
pub mod shutdown;
/// Wire-transport TLS plumbing (PEM cert/key loaders).
pub mod tls;
