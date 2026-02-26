//! Stress test binary — heavy workloads for throughput, scale, and sustained load validation.
//!
//! Run with: `cargo test -p inferadb-ledger-server --test stress`
//! Or: `just test-stress`

mod common;

mod stress_integration;
mod stress_scale;
mod stress_test;
mod stress_watch;
