//! Bootstrap coordination (legacy).
//!
//! This module previously contained the auto-coordination algorithm where
//! the node with the lowest Snowflake ID bootstrapped the cluster. This has
//! been replaced by the explicit `init` command (CockroachDB-style two-phase
//! bootstrap). The coordinator is no longer used.
