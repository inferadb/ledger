//! Canonical log lines for structured request-level observability.
//!
//! Canonical log line pattern where each request emits
//! exactly one structured JSON event containing all request context. This enables:
//!
//! - Single-line debugging: all request context in one log entry
//! - Log-based analytics: query patterns without separate metrics infrastructure
//! - Compliance auditing: cryptographic field inclusion (state roots, block hashes)
//!
//! # Architecture
//!
//! The core abstraction is [`RequestContext`] (aliased as [`CanonicalLogLine`]),
//! a builder that accumulates fields throughout the request lifecycle. On drop,
//! it emits a single structured event to the `ledger::events` tracing target.
//!
//! # Canonical Log Line Schema
//!
//! Every event emitted to `ledger::events` includes these field groups:
//!
//! | Group | Fields | Description |
//! |-------|--------|-------------|
//! | Identity | `request_id`, `service`, `method` | Request routing |
//! | Client | `client_id`, `sequence`, `caller`, `sdk_version`, `source_ip` | Client metadata |
//! | Target | `organization`, `vault` | Scope of the operation |
//! | System | `node_id`, `is_leader`, `raft_term`, `region` | Cluster state |
//! | Write | `operations_count`, `operation_types`, `bytes_written`, `raft_round_trips` | Write metrics |
//! | Read | `key`, `keys_count`, `found_count`, `bytes_read` | Read metrics |
//! | Outcome | `outcome`, `error_code`, `error_message`, `block_height` | Result |
//! | Tracing | `trace_id`, `span_id`, `parent_span_id`, `trace_flags` | W3C Trace Context |
//! | Timing | `duration_ms`, `raft_latency_ms`, `storage_latency_ms` | Latency breakdown |
//!
//! # Sampling
//!
//! High-volume deployments can reduce log volume via [`SamplingConfig`]:
//! - Errors and slow requests are always logged (100% sample rate)
//! - VIP organizations get elevated sampling (default 50%)
//! - Normal writes sample at 10%, reads at 1%
//! - Admin operations are always logged
//!
//! # Log Aggregation
//!
//! Events are emitted as structured JSON to the `ledger::events` tracing target.
//! Configure your tracing subscriber to route this target to your log aggregation
//! system (Datadog, Loki, Elasticsearch, etc.).
//!
//! Example Datadog query: `@service:WriteService @outcome:error @duration_ms:>100`
//!
//! # Example
//!
//! ```no_run
//! use inferadb_ledger_raft::logging::{CanonicalLogLine, Outcome};
//!
//! async fn handle_request() {
//!     let mut ctx = CanonicalLogLine::new("WriteService", "write");
//!     ctx.set_client_info("client_123", 42);
//!     ctx.set_target(1, 2);
//!
//!     // ... process request ...
//!
//!     ctx.set_outcome(Outcome::Success);
//!     // Event emitted automatically on drop
//! }
//! ```

pub mod fields;
mod job_context;
mod request_context;
mod sampling;

#[cfg(test)]
mod tests;

pub use job_context::JobContext;
pub use request_context::{
    CanonicalLogLine, RequestContext, RequestContextGuard, try_with_current_context,
    with_current_context,
};
pub use sampling::{OperationType, Outcome, Sampler, SamplingConfig};
