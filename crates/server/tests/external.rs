//! External cluster test binary — tests that run against a live Ledger cluster
//! started by `scripts/run-server-integration-tests.sh`.
//!
//! All tests skip gracefully when `LEDGER_ENDPOINTS` is not set.
//!
//! Run with: `cargo test -p inferadb-ledger-server --test external`

mod common;

mod background_jobs;
