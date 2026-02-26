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
mod design_compliance;
mod election;
mod externalized_state;
mod get_node_info;
mod isolation;
mod leader_failover;
mod multi_shard;
mod network_simulation;
mod orphan_cleanup;
mod replication;
mod saga_orchestrator;
mod ttl_gc;
mod watch_blocks_realtime;
mod write_read;
