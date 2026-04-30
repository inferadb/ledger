//! Integration test binary — consolidates all server integration tests into a
//! single compilation unit to avoid separate link steps against the full
//! server dependency chain.
//!
//! Run with: `cargo test -p inferadb-ledger-server --test integration`
//! Or: `just test-integration`
//!
//! ## Adding a test
//!
//! 1. Create `tests/<name>.rs` with `use crate::common::` imports.
//! 2. Add `mod <name>;` to this file.
//!
//! Never use `mod common;` inside a test file — all shared helpers come
//! from `crate::common`.

mod common;

mod backup_restore;
mod check_relationship;
mod cluster_b1_helpers;
mod design_compliance;
mod election;
mod externalized_state;
mod get_node_info;
mod invitation;
mod isolation;
mod leader_failover;
mod multi_region;
mod onboarding;
mod orphan_cleanup;
mod region_opt_in;
mod region_residency;
mod replication;
mod saga_orchestrator;
mod telemetry_context;
mod three_tier_consensus;
mod token_lifecycle;
mod ttl_gc;
mod vault_lifecycle;
mod vaults_admin;
mod watch_blocks_realtime;
mod wire_multi_node_smoke;
mod wire_snapshot_install_smoke;
mod wire_test_cluster_smoke;
mod write_read;
