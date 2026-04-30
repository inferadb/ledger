//! Bootstrap coordination.
//!
//! Previously contained the auto-coordination algorithm where the node with the
//! lowest Snowflake ID bootstrapped the cluster. Replaced by the explicit `init`
//! subcommand (CockroachDB-style two-phase bootstrap). This module is now empty.
