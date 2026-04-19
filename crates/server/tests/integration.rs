//! Integration test binary — consolidates all server integration tests into a
//! single compilation unit to avoid 18 separate link steps against the full
//! server dependency chain.
//!
//! Run with: `cargo test -p inferadb-ledger-server --test integration`
//! Or: `just test-integration`

mod common;
mod turmoil_common;

mod backup_restore;
mod bootstrap_coordination;
mod chaos_consistency;
mod check_relationship;
mod checkpoint_crash_recovery;
mod design_compliance;
mod election;
mod externalized_state;
mod get_node_info;
mod invitation;
mod isolation;
mod leader_failover;
mod multi_region;
mod network_simulation;
mod onboarding;
mod orphan_cleanup;
mod redirect_routing;
mod replication;
mod saga_orchestrator;
mod telemetry_context;
mod token_lifecycle;
mod ttl_gc;
mod watch_blocks_realtime;
mod write_read;
