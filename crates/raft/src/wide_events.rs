//! Canonical log lines (wide events) for structured request-level observability.
//!
//! This module implements the canonical log line pattern where each request emits
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
//! | Client | `client_id`, `sequence`, `actor`, `sdk_version`, `source_ip` | Client metadata |
//! | Target | `namespace_id`, `vault_id` | Scope of the operation |
//! | System | `node_id`, `is_leader`, `raft_term`, `shard_id` | Cluster state |
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
//! - VIP namespaces get elevated sampling (default 50%)
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
//! use inferadb_ledger_raft::wide_events::{CanonicalLogLine, Outcome};
//!
//! async fn handle_request() {
//!     let mut ctx = CanonicalLogLine::new("WriteService", "write");
//!     ctx.set_client_info("client_123", 42, Some("user@example.com".into()));
//!     ctx.set_target(1, 2);
//!
//!     // ... process request ...
//!
//!     ctx.set_outcome(Outcome::Success);
//!     // Event emitted automatically on drop
//! }
//! ```

use std::{cell::RefCell, time::Instant};

use serde::Serialize;
use tracing::info;
use uuid::Uuid;

// Task-local storage for the current request context.
// Uses tokio::task_local! instead of thread_local! because tokio's work-stealing
// scheduler moves tasks between OS threads at await points.
tokio::task_local! {
    static CURRENT_CONTEXT: RefCell<RequestContext>;
}

/// Outcome of a request.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// Request completed successfully.
    Success,
    /// Request failed with an error.
    Error {
        /// Error code (gRPC status or domain-specific).
        code: String,
        /// Human-readable error message.
        message: String,
    },
    /// Request returned a cached result (idempotency hit).
    Cached,
    /// Request was rate limited.
    RateLimited,
    /// Request failed due to precondition (CAS failure).
    PreconditionFailed {
        /// Key that failed the precondition.
        key: Option<String>,
    },
}

impl Outcome {
    /// Returns the outcome type as a string for logging.
    fn as_str(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error { .. } => "error",
            Self::Cached => "cached",
            Self::RateLimited => "rate_limited",
            Self::PreconditionFailed { .. } => "precondition_failed",
        }
    }
}

/// Operation type for sampling decisions.
///
/// Different operation types have different default sampling rates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationType {
    /// Reads operations (lowest default sampling rate).
    Read,
    /// Writes operations (medium default sampling rate).
    Write,
    /// Admin operations (always 100% sampled).
    Admin,
}

/// Configuration for wide events sampling.
///
/// Tail sampling samples events at emission time based on outcome and latency.
/// This ensures errors and slow requests are never dropped while reducing log
/// volume for healthy traffic.
///
/// # Example
///
/// ```no_run
/// use inferadb_ledger_raft::wide_events::SamplingConfig;
///
/// // Production: aggressive sampling
/// let config = SamplingConfig::builder()
///     .read_rate(0.01)   // 1% of successful reads
///     .write_rate(0.1)   // 10% of successful writes
///     .build();
///
/// // Development: no sampling
/// let config = SamplingConfig::disabled();
/// ```
#[derive(Debug, Clone, bon::Builder)]
pub struct SamplingConfig {
    /// Sample rate for error outcomes (0.0-1.0). Default: 1.0 (100%).
    #[builder(default = 1.0)]
    pub error_rate: f64,

    /// Sample rate for slow requests (0.0-1.0). Default: 1.0 (100%).
    #[builder(default = 1.0)]
    pub slow_rate: f64,

    /// Sample rate for VIP namespaces (0.0-1.0). Default: 0.5 (50%).
    #[builder(default = 0.5)]
    pub vip_rate: f64,

    /// Sample rate for successful write operations (0.0-1.0). Default: 0.1 (10%).
    #[builder(default = 0.1)]
    pub write_rate: f64,

    /// Sample rate for successful read operations (0.0-1.0). Default: 0.01 (1%).
    #[builder(default = 0.01)]
    pub read_rate: f64,

    /// Threshold for slow read operations, in milliseconds. Default: 10.0.
    #[builder(default = 10.0)]
    pub slow_threshold_read_ms: f64,

    /// Threshold for slow write operations, in milliseconds. Default: 100.0.
    #[builder(default = 100.0)]
    pub slow_threshold_write_ms: f64,

    /// Threshold for slow admin operations, in milliseconds. Default: 1000.0.
    #[builder(default = 1000.0)]
    pub slow_threshold_admin_ms: f64,

    /// List of VIP namespace IDs with elevated sampling.
    #[builder(default)]
    pub vip_namespaces: Vec<i64>,

    /// Whether sampling is enabled. Default: true.
    #[builder(default = true)]
    pub enabled: bool,
}

impl Default for SamplingConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl SamplingConfig {
    /// Creates a config with sampling disabled (100% sample rate for all events).
    pub fn disabled() -> Self {
        Self::builder().enabled(false).build()
    }

    /// Creates a config suitable for testing (no sampling, all events emitted).
    pub fn for_test() -> Self {
        Self::disabled()
    }
}

/// Sampler for wide events using deterministic tail sampling.
///
/// Sampling decisions are made at event emission time based on:
/// 1. Outcome (errors always sampled at 100%)
/// 2. Latency (slow requests always sampled at 100%)
/// 3. Namespace VIP status
/// 4. Operation type
///
/// The decision is deterministic: `seahash(request_id) % 10000 < (rate * 10000)`.
/// This ensures the same request_id always produces the same sampling decision.
#[derive(Debug, Clone)]
pub struct Sampler {
    config: SamplingConfig,
}

impl Default for Sampler {
    fn default() -> Self {
        Self::new(SamplingConfig::default())
    }
}

impl Sampler {
    /// Creates a new sampler with the given configuration.
    pub fn new(config: SamplingConfig) -> Self {
        Self { config }
    }

    /// Creates a sampler with sampling disabled.
    pub fn disabled() -> Self {
        Self::new(SamplingConfig::disabled())
    }

    /// Determines whether an event should be sampled (emitted).
    ///
    /// Returns true if the event should be emitted, false if it should be dropped.
    pub fn should_sample(
        &self,
        request_id: &uuid::Uuid,
        outcome: Option<&Outcome>,
        duration_ms: f64,
        operation_type: OperationType,
        namespace_id: Option<i64>,
    ) -> bool {
        // If sampling is disabled, always emit
        if !self.config.enabled {
            return true;
        }

        // Rule 1: Errors are always sampled at 100%
        if matches!(outcome, Some(Outcome::Error { .. })) {
            return self.sample_at_rate(request_id, self.config.error_rate);
        }

        // Rule 2: Slow requests are always sampled at 100%
        let slow_threshold = match operation_type {
            OperationType::Read => self.config.slow_threshold_read_ms,
            OperationType::Write => self.config.slow_threshold_write_ms,
            OperationType::Admin => self.config.slow_threshold_admin_ms,
        };
        if duration_ms > slow_threshold {
            return self.sample_at_rate(request_id, self.config.slow_rate);
        }

        // Rule 3: Admin operations are always sampled at 100%
        if operation_type == OperationType::Admin {
            return true;
        }

        // Rule 4: VIP namespaces get elevated sampling
        if namespace_id.is_some_and(|ns_id| self.config.vip_namespaces.contains(&ns_id)) {
            return self.sample_at_rate(request_id, self.config.vip_rate);
        }

        // Rule 5: Normal sampling based on operation type
        let rate = match operation_type {
            OperationType::Read => self.config.read_rate,
            OperationType::Write => self.config.write_rate,
            OperationType::Admin => 1.0, // Already handled above, but for completeness
        };

        self.sample_at_rate(request_id, rate)
    }

    /// Performs deterministic sampling using seahash.
    ///
    /// `seahash(request_id) % 10000 < (rate * 10000)`
    fn sample_at_rate(&self, request_id: &uuid::Uuid, rate: f64) -> bool {
        if rate >= 1.0 {
            return true;
        }
        if rate <= 0.0 {
            return false;
        }

        let hash = seahash::hash(request_id.as_bytes());
        let threshold = (rate * 10000.0) as u64;
        (hash % 10000) < threshold
    }
}

/// Timing breakdown for request phases.
#[derive(Debug, Default)]
struct TimingBreakdown {
    /// When Raft consensus started.
    raft_start: Option<Instant>,
    /// Duration in Raft consensus (milliseconds).
    raft_latency_ms: Option<f64>,
    /// When storage operation started.
    storage_start: Option<Instant>,
    /// Duration in storage layer (milliseconds).
    storage_latency_ms: Option<f64>,
}

/// Truncates a string to a maximum length, appending "..." if truncated.
fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

/// Sanitizes a string by replacing control characters with the replacement character.
fn sanitize_string(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_control() && c != '\n' && c != '\r' && c != '\t' { '\u{FFFD}' } else { c })
        .collect()
}

/// Truncates a hex hash to 16 characters.
fn truncate_hash(hash: &str) -> String {
    if hash.len() <= 16 { hash.to_string() } else { hash[..16].to_string() }
}

/// Accesses the current task-local request context, if one exists.
///
/// This function provides safe access to the request context from anywhere
/// in the call stack within the same tokio task. The context is set when
/// a [`RequestContextGuard`] is used with [`RequestContextGuard::run_with`]
/// and accessed via the [`with_current_context`] or [`try_with_current_context`]
/// functions.
///
/// # Returns
///
/// Returns `Some(result)` if a context exists and the closure executes successfully,
/// or `None` if no context is set for the current task.
///
/// # Example
///
/// ```no_run
/// use inferadb_ledger_raft::wide_events::with_current_context;
///
/// fn record_storage_timing() {
///     with_current_context(|ctx| {
///         ctx.start_storage_timer();
///     });
/// }
/// ```
pub fn with_current_context<F, R>(f: F) -> Option<R>
where
    F: FnOnce(&mut RequestContext) -> R,
{
    CURRENT_CONTEXT.try_with(|cell| f(&mut cell.borrow_mut())).ok()
}

/// Attempts to access the current task-local request context.
///
/// Similar to [`with_current_context`], but returns `false` if no context exists
/// instead of `None`. Useful when you just want to perform an action if context
/// exists but don't need a return value.
///
/// # Returns
///
/// Returns `true` if the context existed and the closure executed, `false` otherwise.
pub fn try_with_current_context<F>(f: F) -> bool
where
    F: FnOnce(&mut RequestContext),
{
    with_current_context(|ctx| {
        f(ctx);
    })
    .is_some()
}

/// RAII guard that sets the current request context for the duration of its scope.
///
/// When created, the guard sets up task-local storage with the provided context.
/// When dropped, it removes the context from task-local storage and emits the
/// wide event (via the context's Drop implementation).
///
/// # Task-Local Behavior
///
/// - The context is scoped to the current tokio task, not the OS thread
/// - The context survives across `.await` points within the same task
/// - Child tasks created with `tokio::spawn` do NOT inherit the parent's context
/// - Each task must create its own guard if it needs context access
///
/// # Example
///
/// ```no_run
/// use inferadb_ledger_raft::wide_events::{RequestContext, RequestContextGuard, with_current_context};
///
/// async fn handle_request() {
///     let ctx = RequestContext::new("WriteService", "write");
///     RequestContextGuard::run_with(ctx, async {
///         // Context is accessible here and across await points
///         with_current_context(|ctx| {
///             ctx.set_client_info("client_123", 42, None);
///         });
///
///         // Simulate async work
///         tokio::time::sleep(std::time::Duration::from_millis(1)).await;
///
///         // Still accessible after await
///         with_current_context(|ctx| {
///             ctx.set_success();
///         });
///     }).await;
/// }
/// ```
pub struct RequestContextGuard {
    _private: (), // Prevent external construction
}

impl RequestContextGuard {
    /// Runs an async block with the given request context set as the current context.
    ///
    /// The context is available via [`with_current_context`] for the duration of the
    /// future's execution. When the future completes (or is dropped), the context's
    /// wide event is emitted.
    ///
    /// # Arguments
    ///
    /// * `context` - The request context to make current
    /// * `f` - The async block to execute with the context set
    ///
    /// # Returns
    ///
    /// The result of the async block.
    pub async fn run_with<F, R>(context: RequestContext, f: F) -> R
    where
        F: std::future::Future<Output = R>,
    {
        CURRENT_CONTEXT.scope(RefCell::new(context), f).await
    }

    /// Runs a synchronous closure with the given request context set as the current context.
    ///
    /// The context is available via [`with_current_context`] for the duration of the
    /// closure's execution. When the closure returns, the context's wide event is emitted.
    ///
    /// # Arguments
    ///
    /// * `context` - The request context to make current
    /// * `f` - The closure to execute with the context set
    ///
    /// # Returns
    ///
    /// The result of the closure.
    pub fn run_with_sync<F, R>(context: RequestContext, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        CURRENT_CONTEXT.sync_scope(RefCell::new(context), f)
    }
}

/// Context builder for wide event emission.
///
/// Accumulates contextual fields throughout request processing and emits
/// a single structured event on drop. Fields can be set in any order and
/// can be overwritten (last write wins).
///
/// # Drop Behavior
///
/// When dropped, emits a JSON event to the `ledger::events` target at INFO level.
/// This ensures events are emitted even on early returns or panics.
///
/// # Sampling
///
/// When a sampler is configured via [`set_sampler`](Self::set_sampler), the context
/// will apply tail sampling at emission time. Sampling decisions are deterministic
/// based on request_id and consider outcome, latency, and operation type.
#[derive(Debug)]
pub struct RequestContext {
    // Request metadata
    request_id: Uuid,
    service: &'static str,
    method: &'static str,
    start_time: Instant,

    // Client info
    client_id: Option<String>,
    sequence: Option<u64>,
    actor: Option<String>,

    // Target
    namespace_id: Option<i64>,
    vault_id: Option<i64>,

    // System context (populated externally)
    node_id: Option<u64>,
    is_leader: Option<bool>,
    raft_term: Option<u64>,
    shard_id: Option<u32>,

    // VIP status (for wide event field)
    is_vip: Option<bool>,

    // Operation-specific (write)
    operations_count: Option<usize>,
    operation_types: Option<Vec<&'static str>>,
    include_tx_proof: Option<bool>,
    idempotency_hit: Option<bool>,
    batch_coalesced: Option<bool>,
    batch_size: Option<usize>,
    condition_type: Option<String>,

    // Operation-specific (read)
    key: Option<String>,
    keys_count: Option<usize>,
    found_count: Option<usize>,
    consistency: Option<String>,
    at_height: Option<u64>,
    include_proof: Option<bool>,
    proof_size_bytes: Option<usize>,
    found: Option<bool>,
    value_size_bytes: Option<usize>,

    // Operation-specific (admin)
    admin_action: Option<&'static str>,
    target_namespace_name: Option<String>,
    retention_mode: Option<String>,
    recovery_force: Option<bool>,

    // Outcome
    outcome: Option<Outcome>,
    block_height: Option<u64>,
    block_hash: Option<String>,
    state_root: Option<String>,

    // I/O metrics
    bytes_read: Option<usize>,
    bytes_written: Option<usize>,
    raft_round_trips: Option<u32>,

    // Client transport metadata
    sdk_version: Option<String>,
    source_ip: Option<String>,

    // Tracing context (W3C Trace Context)
    trace_id: Option<String>,
    span_id: Option<String>,
    parent_span_id: Option<String>,
    trace_flags: Option<u8>,

    // Timing
    timing: TimingBreakdown,

    // Emission control
    emitted: bool,

    // Sampling
    operation_type: OperationType,
    sampler: Option<Sampler>,
}

impl RequestContext {
    /// Creates a new request context with auto-generated request ID.
    ///
    /// # Arguments
    ///
    /// * `service` - The gRPC service name (e.g., "WriteService").
    /// * `method` - The gRPC method name (e.g., "write").
    #[must_use]
    pub fn new(service: &'static str, method: &'static str) -> Self {
        Self {
            request_id: Uuid::new_v4(),
            service,
            method,
            start_time: Instant::now(),
            client_id: None,
            sequence: None,
            actor: None,
            namespace_id: None,
            vault_id: None,
            node_id: None,
            is_leader: None,
            raft_term: None,
            shard_id: None,
            is_vip: None,
            operations_count: None,
            operation_types: None,
            include_tx_proof: None,
            idempotency_hit: None,
            batch_coalesced: None,
            batch_size: None,
            condition_type: None,
            key: None,
            keys_count: None,
            found_count: None,
            consistency: None,
            at_height: None,
            include_proof: None,
            proof_size_bytes: None,
            found: None,
            value_size_bytes: None,
            admin_action: None,
            target_namespace_name: None,
            retention_mode: None,
            recovery_force: None,
            outcome: None,
            block_height: None,
            block_hash: None,
            state_root: None,
            bytes_read: None,
            bytes_written: None,
            raft_round_trips: None,
            sdk_version: None,
            source_ip: None,
            trace_id: None,
            span_id: None,
            parent_span_id: None,
            trace_flags: None,
            timing: TimingBreakdown::default(),
            emitted: false,
            operation_type: OperationType::Write, // Default, can be overridden
            sampler: None,
        }
    }

    /// Creates a new request context with a specific request ID (for testing).
    #[must_use]
    pub fn with_request_id(service: &'static str, method: &'static str, request_id: Uuid) -> Self {
        let mut ctx = Self::new(service, method);
        ctx.request_id = request_id;
        ctx
    }

    /// Returns the request ID.
    #[must_use]
    pub fn request_id(&self) -> Uuid {
        self.request_id
    }

    // =========================================================================
    // Client info setters
    // =========================================================================

    /// Sets client information.
    ///
    /// # Arguments
    ///
    /// * `client_id` - Idempotency client identifier (truncated to 128 chars).
    /// * `sequence` - Per-client sequence number.
    /// * `actor` - Identity performing the operation.
    pub fn set_client_info(&mut self, client_id: &str, sequence: u64, actor: Option<String>) {
        self.client_id = Some(truncate_string(&sanitize_string(client_id), 128));
        self.sequence = Some(sequence);
        self.actor = actor.map(|a| sanitize_string(&a));
    }

    /// Sets the client ID only.
    pub fn set_client_id(&mut self, client_id: &str) {
        self.client_id = Some(truncate_string(&sanitize_string(client_id), 128));
    }

    /// Sets the sequence number only.
    pub fn set_sequence(&mut self, sequence: u64) {
        self.sequence = Some(sequence);
    }

    /// Sets the actor only.
    pub fn set_actor(&mut self, actor: &str) {
        self.actor = Some(sanitize_string(actor));
    }

    // =========================================================================
    // Target setters
    // =========================================================================

    /// Sets the target namespace and vault.
    pub fn set_target(&mut self, namespace_id: i64, vault_id: i64) {
        self.namespace_id = Some(namespace_id);
        self.vault_id = Some(vault_id);
    }

    /// Sets the namespace ID only.
    pub fn set_namespace_id(&mut self, namespace_id: i64) {
        self.namespace_id = Some(namespace_id);
    }

    /// Sets the vault ID only.
    pub fn set_vault_id(&mut self, vault_id: i64) {
        self.vault_id = Some(vault_id);
    }

    // =========================================================================
    // System context setters
    // =========================================================================

    /// Sets system context information.
    pub fn set_system_context(
        &mut self,
        node_id: u64,
        is_leader: bool,
        raft_term: u64,
        shard_id: Option<u32>,
    ) {
        self.node_id = Some(node_id);
        self.is_leader = Some(is_leader);
        self.raft_term = Some(raft_term);
        self.shard_id = shard_id;
    }

    /// Sets the node ID.
    pub fn set_node_id(&mut self, node_id: u64) {
        self.node_id = Some(node_id);
    }

    /// Sets whether this node is the Raft leader.
    pub fn set_is_leader(&mut self, is_leader: bool) {
        self.is_leader = Some(is_leader);
    }

    /// Sets the current Raft term.
    pub fn set_raft_term(&mut self, raft_term: u64) {
        self.raft_term = Some(raft_term);
    }

    /// Sets the shard ID.
    pub fn set_shard_id(&mut self, shard_id: u32) {
        self.shard_id = Some(shard_id);
    }

    /// Sets the VIP status for this request's namespace.
    ///
    /// This field indicates whether the namespace received elevated
    /// sampling rates due to VIP status.
    pub fn set_is_vip(&mut self, is_vip: bool) {
        self.is_vip = Some(is_vip);
    }

    // =========================================================================
    // Write operation setters
    // =========================================================================

    /// Sets write operation details.
    pub fn set_write_operation(
        &mut self,
        operations_count: usize,
        operation_types: Vec<&'static str>,
        include_tx_proof: bool,
    ) {
        self.operations_count = Some(operations_count);
        self.operation_types = Some(operation_types);
        self.include_tx_proof = Some(include_tx_proof);
    }

    /// Sets the operations count.
    pub fn set_operations_count(&mut self, count: usize) {
        self.operations_count = Some(count);
    }

    /// Sets the operation types.
    pub fn set_operation_types(&mut self, types: Vec<&'static str>) {
        self.operation_types = Some(types);
    }

    /// Sets whether a transaction proof was requested.
    pub fn set_include_tx_proof(&mut self, include: bool) {
        self.include_tx_proof = Some(include);
    }

    /// Sets whether this request was served from idempotency cache.
    pub fn set_idempotency_hit(&mut self, hit: bool) {
        self.idempotency_hit = Some(hit);
    }

    /// Sets batch coalescing information.
    pub fn set_batch_info(&mut self, coalesced: bool, batch_size: usize) {
        self.batch_coalesced = Some(coalesced);
        self.batch_size = Some(batch_size);
    }

    /// Sets the condition type for CAS operations.
    pub fn set_condition_type(&mut self, condition: &str) {
        self.condition_type = Some(condition.to_string());
    }

    // =========================================================================
    // Read operation setters
    // =========================================================================

    /// Sets read operation details.
    pub fn set_read_operation(&mut self, key: &str, consistency: &str, include_proof: bool) {
        self.key = Some(truncate_string(&sanitize_string(key), 256));
        self.consistency = Some(consistency.to_string());
        self.include_proof = Some(include_proof);
    }

    /// Sets the key being read (truncated to 256 chars).
    pub fn set_key(&mut self, key: &str) {
        self.key = Some(truncate_string(&sanitize_string(key), 256));
    }

    /// Sets the number of keys in a batch read.
    pub fn set_keys_count(&mut self, count: usize) {
        self.keys_count = Some(count);
    }

    /// Sets the number of keys found in a batch read.
    pub fn set_found_count(&mut self, count: usize) {
        self.found_count = Some(count);
    }

    /// Sets the consistency level.
    pub fn set_consistency(&mut self, consistency: &str) {
        self.consistency = Some(consistency.to_string());
    }

    /// Sets the historical read height.
    pub fn set_at_height(&mut self, height: u64) {
        self.at_height = Some(height);
    }

    /// Sets whether a proof was requested.
    pub fn set_include_proof(&mut self, include: bool) {
        self.include_proof = Some(include);
    }

    /// Sets the size of the proof in bytes.
    pub fn set_proof_size_bytes(&mut self, size: usize) {
        self.proof_size_bytes = Some(size);
    }

    /// Sets whether the key was found.
    pub fn set_found(&mut self, found: bool) {
        self.found = Some(found);
    }

    /// Sets the value size in bytes.
    pub fn set_value_size_bytes(&mut self, size: usize) {
        self.value_size_bytes = Some(size);
    }

    // --- Admin operation fields ---

    /// Sets the admin action being performed.
    ///
    /// Example values: "create_namespace", "delete_vault", "recover_vault"
    pub fn set_admin_action(&mut self, action: &'static str) {
        self.admin_action = Some(action);
    }

    /// Sets the target namespace name for namespace operations.
    pub fn set_target_namespace_name(&mut self, name: &str) {
        self.target_namespace_name = Some(truncate_string(name, 128));
    }

    /// Sets the retention mode for vault creation.
    pub fn set_retention_mode(&mut self, mode: &str) {
        self.retention_mode = Some(mode.to_string());
    }

    /// Sets whether force mode was used for recovery operations.
    pub fn set_recovery_force(&mut self, force: bool) {
        self.recovery_force = Some(force);
    }

    /// Sets admin operation fields for admin-specific events.
    ///
    /// Convenience method for setting multiple admin fields at once.
    pub fn set_admin_operation(&mut self, action: &'static str) {
        self.admin_action = Some(action);
    }

    // =========================================================================
    // Outcome setters
    // =========================================================================

    /// Sets the request outcome.
    pub fn set_outcome(&mut self, outcome: Outcome) {
        self.outcome = Some(outcome);
    }

    /// Sets the outcome as success.
    pub fn set_success(&mut self) {
        self.outcome = Some(Outcome::Success);
    }

    /// Sets the outcome as error with code and message.
    pub fn set_error(&mut self, code: &str, message: &str) {
        self.outcome = Some(Outcome::Error {
            code: code.to_string(),
            message: truncate_string(&sanitize_string(message), 512),
        });
    }

    /// Sets the outcome as cached (idempotency hit).
    pub fn set_cached(&mut self) {
        self.outcome = Some(Outcome::Cached);
    }

    /// Sets the outcome as rate limited.
    pub fn set_rate_limited(&mut self) {
        self.outcome = Some(Outcome::RateLimited);
    }

    /// Sets the outcome as precondition failed.
    pub fn set_precondition_failed(&mut self, key: Option<&str>) {
        self.outcome = Some(Outcome::PreconditionFailed {
            key: key.map(|k| truncate_string(&sanitize_string(k), 256)),
        });
    }

    /// Sets the committed block height.
    pub fn set_block_height(&mut self, height: u64) {
        self.block_height = Some(height);
    }

    /// Sets the block hash (truncated to 16 hex chars).
    pub fn set_block_hash(&mut self, hash: &str) {
        self.block_hash = Some(truncate_hash(hash));
    }

    /// Sets the state root (truncated to 16 hex chars).
    pub fn set_state_root(&mut self, root: &str) {
        self.state_root = Some(truncate_hash(root));
    }

    // =========================================================================
    // I/O metrics setters
    // =========================================================================

    /// Sets the number of bytes read during this request.
    pub fn set_bytes_read(&mut self, bytes: usize) {
        self.bytes_read = Some(bytes);
    }

    /// Sets the number of bytes written during this request.
    pub fn set_bytes_written(&mut self, bytes: usize) {
        self.bytes_written = Some(bytes);
    }

    /// Increments the Raft round trip counter by one.
    ///
    /// Call this each time a Raft proposal round trip completes.
    pub fn increment_raft_round_trips(&mut self) {
        *self.raft_round_trips.get_or_insert(0) += 1;
    }

    /// Sets the Raft round trip count directly.
    pub fn set_raft_round_trips(&mut self, count: u32) {
        self.raft_round_trips = Some(count);
    }

    // =========================================================================
    // Client transport metadata setters
    // =========================================================================

    /// Sets the SDK version string reported by the client.
    ///
    /// Extracted from gRPC `x-sdk-version` metadata header.
    pub fn set_sdk_version(&mut self, version: &str) {
        self.sdk_version = Some(truncate_string(version, 64));
    }

    /// Sets the source IP address of the client connection.
    ///
    /// Extracted from the gRPC transport remote address.
    pub fn set_source_ip(&mut self, ip: &str) {
        self.source_ip = Some(truncate_string(ip, 45)); // IPv6 max length
    }

    /// Extracts SDK version and source IP from gRPC request metadata.
    ///
    /// Reads:
    /// - `x-sdk-version` header for the client SDK version
    /// - `x-forwarded-for` header for the client source IP (behind load balancers)
    pub fn extract_transport_metadata(&mut self, metadata: &tonic::metadata::MetadataMap) {
        if let Some(version) = metadata.get("x-sdk-version")
            && let Ok(v) = version.to_str()
        {
            self.set_sdk_version(v);
        }
        if let Some(forwarded_for) = metadata.get("x-forwarded-for")
            && let Ok(ips) = forwarded_for.to_str()
        {
            // x-forwarded-for may contain multiple IPs; take the first (client IP)
            if let Some(client_ip) = ips.split(',').next() {
                self.set_source_ip(client_ip.trim());
            }
        }
    }

    // === Tracing Context ===

    /// Sets the W3C trace ID (32 hex chars).
    pub fn set_trace_id(&mut self, trace_id: String) {
        self.trace_id = Some(trace_id);
    }

    /// Sets the span ID (16 hex chars).
    pub fn set_span_id(&mut self, span_id: String) {
        self.span_id = Some(span_id);
    }

    /// Sets the parent span ID (16 hex chars).
    pub fn set_parent_span_id(&mut self, parent_span_id: String) {
        self.parent_span_id = Some(parent_span_id);
    }

    /// Sets the W3C trace flags.
    pub fn set_trace_flags(&mut self, flags: u8) {
        self.trace_flags = Some(flags);
    }

    /// Sets complete trace context from W3C traceparent format.
    ///
    /// # Arguments
    ///
    /// * `trace_id` - 32 hex character trace ID
    /// * `span_id` - 16 hex character span ID
    /// * `parent_span_id` - Optional 16 hex character parent span ID
    /// * `flags` - W3C trace flags (sampled bit)
    pub fn set_trace_context(
        &mut self,
        trace_id: &str,
        span_id: &str,
        parent_span_id: Option<&str>,
        flags: u8,
    ) {
        self.trace_id = Some(trace_id.to_string());
        self.span_id = Some(span_id.to_string());
        self.parent_span_id = parent_span_id.map(String::from);
        self.trace_flags = Some(flags);
    }

    // =========================================================================
    // Timing methods
    // =========================================================================

    /// Starts the Raft consensus timer.
    pub fn start_raft_timer(&mut self) {
        self.timing.raft_start = Some(Instant::now());
    }

    /// Ends the Raft consensus timer and records the duration.
    ///
    /// Also increments the Raft round trip counter.
    pub fn end_raft_timer(&mut self) {
        if let Some(start) = self.timing.raft_start.take() {
            self.timing.raft_latency_ms = Some(start.elapsed().as_secs_f64() * 1000.0);
            *self.raft_round_trips.get_or_insert(0) += 1;
        }
    }

    /// Starts the storage layer timer.
    pub fn start_storage_timer(&mut self) {
        self.timing.storage_start = Some(Instant::now());
    }

    /// Ends the storage layer timer and records the duration.
    pub fn end_storage_timer(&mut self) {
        if let Some(start) = self.timing.storage_start.take() {
            self.timing.storage_latency_ms = Some(start.elapsed().as_secs_f64() * 1000.0);
        }
    }

    // =========================================================================
    // Emission control
    // =========================================================================

    /// Suppresses event emission on drop.
    ///
    /// Use this when the context is being transferred to another owner
    /// or when emission should be handled manually.
    pub fn suppress_emission(&mut self) {
        self.emitted = true;
    }

    /// Sets the operation type for sampling decisions.
    ///
    /// Different operation types have different default sampling rates:
    /// - Read: 1% default
    /// - Write: 10% default
    /// - Admin: 100% (always sampled)
    pub fn set_operation_type(&mut self, operation_type: OperationType) {
        self.operation_type = operation_type;
    }

    /// Sets the sampler for tail sampling decisions.
    ///
    /// When a sampler is set, the context will apply sampling at emission time.
    /// Events that don't pass sampling are silently dropped.
    pub fn set_sampler(&mut self, sampler: Sampler) {
        self.sampler = Some(sampler);
    }

    /// Returns the elapsed time since context creation in seconds.
    ///
    /// Useful for recording metrics before the context is dropped.
    pub fn elapsed_secs(&self) -> f64 {
        self.start_time.elapsed().as_secs_f64()
    }

    /// Manually emit the event. Prevents double emission on drop.
    pub fn emit(&mut self) {
        if self.emitted {
            return;
        }
        self.emitted = true;
        self.emit_event();
    }

    /// Internal event emission logic.
    fn emit_event(&self) {
        let duration_ms = self.start_time.elapsed().as_secs_f64() * 1000.0;

        // Apply sampling if configured
        if self.sampler.as_ref().is_some_and(|sampler| {
            !sampler.should_sample(
                &self.request_id,
                self.outcome.as_ref(),
                duration_ms,
                self.operation_type,
                self.namespace_id,
            )
        }) {
            // Event sampled out, don't emit
            return;
        }

        let outcome_str = self.outcome.as_ref().map_or("unknown", Outcome::as_str);

        // Extract error fields if present
        let (error_code, error_message, error_key) = match &self.outcome {
            Some(Outcome::Error { code, message }) => {
                (Some(code.as_str()), Some(message.as_str()), None)
            },
            Some(Outcome::PreconditionFailed { key }) => {
                (Some("precondition_failed"), None, key.as_deref())
            },
            _ => (None, None, None),
        };

        // Emit to the ledger::events target
        info!(
            target: "ledger::events",
            request_id = %self.request_id,
            service = self.service,
            method = self.method,
            client_id = self.client_id.as_deref(),
            sequence = self.sequence,
            actor = self.actor.as_deref(),
            namespace_id = self.namespace_id,
            vault_id = self.vault_id,
            node_id = self.node_id,
            is_leader = self.is_leader,
            raft_term = self.raft_term,
            shard_id = self.shard_id,
            is_vip = self.is_vip,
            operations_count = self.operations_count,
            operation_types = ?self.operation_types,
            include_tx_proof = self.include_tx_proof,
            idempotency_hit = self.idempotency_hit,
            batch_coalesced = self.batch_coalesced,
            batch_size = self.batch_size,
            condition_type = self.condition_type.as_deref(),
            key = self.key.as_deref(),
            keys_count = self.keys_count,
            found_count = self.found_count,
            consistency = self.consistency.as_deref(),
            at_height = self.at_height,
            include_proof = self.include_proof,
            proof_size_bytes = self.proof_size_bytes,
            found = self.found,
            value_size_bytes = self.value_size_bytes,
            admin_action = self.admin_action,
            target_namespace_name = self.target_namespace_name.as_deref(),
            retention_mode = self.retention_mode.as_deref(),
            recovery_force = self.recovery_force,
            outcome = outcome_str,
            error_code = error_code,
            error_message = error_message,
            error_key = error_key,
            block_height = self.block_height,
            block_hash = self.block_hash.as_deref(),
            state_root = self.state_root.as_deref(),
            bytes_read = self.bytes_read,
            bytes_written = self.bytes_written,
            raft_round_trips = self.raft_round_trips,
            sdk_version = self.sdk_version.as_deref(),
            source_ip = self.source_ip.as_deref(),
            trace_id = self.trace_id.as_deref(),
            span_id = self.span_id.as_deref(),
            parent_span_id = self.parent_span_id.as_deref(),
            trace_flags = self.trace_flags,
            duration_ms = duration_ms,
            raft_latency_ms = self.timing.raft_latency_ms,
            storage_latency_ms = self.timing.storage_latency_ms,
            "canonical_log_line"
        );

        // Export span to OTEL if enabled
        if crate::otel::is_otel_enabled() {
            let attrs = crate::otel::SpanAttributes {
                request_id: Some(self.request_id.to_string()),
                client_id: self.client_id.clone(),
                sequence: self.sequence,
                namespace_id: self.namespace_id,
                vault_id: self.vault_id,
                service: Some(self.service),
                method: Some(self.method),
                actor: self.actor.clone(),
                node_id: self.node_id,
                is_leader: self.is_leader,
                raft_term: self.raft_term,
                shard_id: self.shard_id,
                is_vip: self.is_vip,
                operations_count: self.operations_count,
                idempotency_hit: self.idempotency_hit,
                batch_coalesced: self.batch_coalesced,
                batch_size: self.batch_size,
                key: self.key.clone(),
                keys_count: self.keys_count,
                found_count: self.found_count,
                consistency: self.consistency.clone(),
                at_height: self.at_height,
                include_proof: self.include_proof,
                found: self.found,
                value_size_bytes: self.value_size_bytes,
                admin_action: self.admin_action.map(String::from),
                outcome: Some(outcome_str.to_string()),
                error_code: error_code.map(String::from),
                error_message: error_message.map(String::from),
                block_height: self.block_height,
                block_hash: self.block_hash.clone(),
                state_root: self.state_root.clone(),
                bytes_read: self.bytes_read,
                bytes_written: self.bytes_written,
                raft_round_trips: self.raft_round_trips,
                sdk_version: self.sdk_version.clone(),
                source_ip: self.source_ip.clone(),
                duration_ms: Some(duration_ms),
                raft_latency_ms: self.timing.raft_latency_ms,
                storage_latency_ms: self.timing.storage_latency_ms,
            };

            crate::otel::export_span(
                self.service,
                self.method,
                attrs,
                self.trace_id.as_deref(),
                self.span_id.as_deref(),
                self.parent_span_id.as_deref(),
            );
        }
    }
}

impl Drop for RequestContext {
    fn drop(&mut self) {
        if !self.emitted {
            self.emitted = true;
            self.emit_event();
        }
    }
}

/// Alias for [`RequestContext`] emphasizing the canonical log line pattern.
///
/// Each request creates one `CanonicalLogLine` that accumulates context throughout
/// its lifecycle and emits a single structured JSON event on completion.
pub type CanonicalLogLine = RequestContext;

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    /// Test layer that counts events emitted to ledger::events target.
    struct CountingLayer {
        count: Arc<AtomicUsize>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CountingLayer {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if event.metadata().target() == "ledger::events" {
                self.count.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    fn with_test_subscriber<F, R>(f: F) -> (R, usize)
    where
        F: FnOnce() -> R,
    {
        let count = Arc::new(AtomicUsize::new(0));
        let layer = CountingLayer { count: count.clone() };
        let subscriber = tracing_subscriber::registry().with(layer);
        let result = tracing::subscriber::with_default(subscriber, f);
        (result, count.load(Ordering::SeqCst))
    }

    #[test]
    fn test_context_creation_populates_request_id_and_start_time() {
        let ctx = RequestContext::new("WriteService", "write");
        assert!(!ctx.request_id.is_nil());
        assert_eq!(ctx.service, "WriteService");
        assert_eq!(ctx.method, "write");
    }

    #[test]
    fn test_context_with_specific_request_id() {
        let id = Uuid::new_v4();
        let ctx = RequestContext::with_request_id("ReadService", "read", id);
        assert_eq!(ctx.request_id(), id);
    }

    #[test]
    fn test_set_client_info() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_client_info("client_123", 42, Some("user@example.com".into()));
        assert_eq!(ctx.client_id.as_deref(), Some("client_123"));
        assert_eq!(ctx.sequence, Some(42));
        assert_eq!(ctx.actor.as_deref(), Some("user@example.com"));
        ctx.suppress_emission();
    }

    #[test]
    fn test_client_id_truncation() {
        let mut ctx = RequestContext::new("WriteService", "write");
        let long_id = "x".repeat(200);
        ctx.set_client_id(&long_id);
        assert!(ctx.client_id.is_some());
        let client_id = ctx.client_id.clone().unwrap_or_default();
        assert!(client_id.len() <= 128);
        assert!(client_id.ends_with("..."));
        ctx.suppress_emission();
    }

    #[test]
    fn test_string_sanitization() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_client_id("test\x00\x01\x02normal");
        assert!(ctx.client_id.is_some());
        let sanitized = ctx.client_id.clone().unwrap_or_default();
        assert!(!sanitized.contains('\x00'));
        assert!(sanitized.contains('\u{FFFD}'));
        ctx.suppress_emission();
    }

    #[test]
    fn test_set_target() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_target(1, 2);
        assert_eq!(ctx.namespace_id, Some(1));
        assert_eq!(ctx.vault_id, Some(2));
        ctx.suppress_emission();
    }

    #[test]
    fn test_set_system_context() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_system_context(100, true, 5, Some(3));
        assert_eq!(ctx.node_id, Some(100));
        assert_eq!(ctx.is_leader, Some(true));
        assert_eq!(ctx.raft_term, Some(5));
        assert_eq!(ctx.shard_id, Some(3));
        ctx.suppress_emission();
    }

    #[test]
    fn test_set_write_operation() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_write_operation(3, vec!["set_entity", "create_relationship"], true);
        assert_eq!(ctx.operations_count, Some(3));
        assert_eq!(ctx.operation_types, Some(vec!["set_entity", "create_relationship"]));
        assert_eq!(ctx.include_tx_proof, Some(true));
        ctx.suppress_emission();
    }

    #[test]
    fn test_set_batch_info() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_batch_info(true, 5);
        assert_eq!(ctx.batch_coalesced, Some(true));
        assert_eq!(ctx.batch_size, Some(5));
        ctx.suppress_emission();
    }

    #[test]
    fn test_set_read_operation() {
        let mut ctx = RequestContext::new("ReadService", "read");
        ctx.set_read_operation("my_key", "linearizable", true);
        assert_eq!(ctx.key.as_deref(), Some("my_key"));
        assert_eq!(ctx.consistency.as_deref(), Some("linearizable"));
        assert_eq!(ctx.include_proof, Some(true));
        ctx.suppress_emission();
    }

    #[test]
    fn test_set_outcome_success() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_success();
        assert!(matches!(ctx.outcome, Some(Outcome::Success)));
        ctx.suppress_emission();
    }

    #[test]
    fn test_set_outcome_error() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_error("INVALID_ARGUMENT", "Missing required field");
        let is_correct = matches!(
            &ctx.outcome,
            Some(Outcome::Error { code, message })
            if code == "INVALID_ARGUMENT" && message == "Missing required field"
        );
        assert!(is_correct);
        ctx.suppress_emission();
    }

    #[test]
    fn test_error_message_truncation() {
        let mut ctx = RequestContext::new("WriteService", "write");
        let long_message = "x".repeat(600);
        ctx.set_error("ERROR", &long_message);
        let is_truncated = matches!(
            &ctx.outcome,
            Some(Outcome::Error { message, .. })
            if message.len() <= 512 && message.ends_with("...")
        );
        assert!(is_truncated);
        ctx.suppress_emission();
    }

    #[test]
    fn test_set_outcome_cached() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_cached();
        assert!(matches!(ctx.outcome, Some(Outcome::Cached)));
        ctx.suppress_emission();
    }

    #[test]
    fn test_set_outcome_rate_limited() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_rate_limited();
        assert!(matches!(ctx.outcome, Some(Outcome::RateLimited)));
        ctx.suppress_emission();
    }

    #[test]
    fn test_set_outcome_precondition_failed() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_precondition_failed(Some("my_key"));
        let is_correct = matches!(
            &ctx.outcome,
            Some(Outcome::PreconditionFailed { key: Some(k) }) if k == "my_key"
        );
        assert!(is_correct);
        ctx.suppress_emission();
    }

    #[test]
    fn test_hash_truncation() {
        let mut ctx = RequestContext::new("WriteService", "write");
        let full_hash = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        ctx.set_block_hash(full_hash);
        ctx.set_state_root(full_hash);
        assert_eq!(ctx.block_hash.as_deref(), Some("abcdef0123456789"));
        assert_eq!(ctx.state_root.as_deref(), Some("abcdef0123456789"));
        ctx.suppress_emission();
    }

    #[test]
    fn test_raft_timing() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.start_raft_timer();
        std::thread::sleep(std::time::Duration::from_millis(10));
        ctx.end_raft_timer();
        let latency = ctx.timing.raft_latency_ms.unwrap_or(0.0);
        assert!(latency >= 10.0);
        ctx.suppress_emission();
    }

    #[test]
    fn test_storage_timing() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.start_storage_timer();
        std::thread::sleep(std::time::Duration::from_millis(10));
        ctx.end_storage_timer();
        let latency = ctx.timing.storage_latency_ms.unwrap_or(0.0);
        assert!(latency >= 10.0);
        ctx.suppress_emission();
    }

    #[test]
    fn test_field_overwrite() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_namespace_id(1);
        ctx.set_namespace_id(2);
        assert_eq!(ctx.namespace_id, Some(2));
        ctx.suppress_emission();
    }

    #[test]
    fn test_drop_emits_event() {
        let (_, count) = with_test_subscriber(|| {
            let mut ctx = RequestContext::new("WriteService", "write");
            ctx.set_success();
            // ctx drops here
        });
        assert_eq!(count, 1);
    }

    #[test]
    fn test_manual_emit_prevents_double_emission() {
        let (_, count) = with_test_subscriber(|| {
            let mut ctx = RequestContext::new("WriteService", "write");
            ctx.set_success();
            ctx.emit();
            // ctx drops here but should not emit again
        });
        assert_eq!(count, 1);
    }

    #[test]
    fn test_suppress_emission_prevents_drop_event() {
        let (_, count) = with_test_subscriber(|| {
            let mut ctx = RequestContext::new("WriteService", "write");
            ctx.set_success();
            ctx.suppress_emission();
            // ctx drops here but should not emit
        });
        assert_eq!(count, 0);
    }

    #[test]
    fn test_outcome_as_str() {
        assert_eq!(Outcome::Success.as_str(), "success");
        assert_eq!(Outcome::Error { code: "x".into(), message: "y".into() }.as_str(), "error");
        assert_eq!(Outcome::Cached.as_str(), "cached");
        assert_eq!(Outcome::RateLimited.as_str(), "rate_limited");
        assert_eq!(Outcome::PreconditionFailed { key: None }.as_str(), "precondition_failed");
    }

    #[test]
    fn test_truncate_string_no_truncation() {
        assert_eq!(truncate_string("hello", 10), "hello");
    }

    #[test]
    fn test_truncate_string_with_truncation() {
        assert_eq!(truncate_string("hello world", 8), "hello...");
    }

    #[test]
    fn test_truncate_hash_short() {
        assert_eq!(truncate_hash("abcd"), "abcd");
    }

    #[test]
    fn test_truncate_hash_long() {
        assert_eq!(truncate_hash("abcdef0123456789abcdef0123456789"), "abcdef0123456789");
    }

    #[test]
    fn test_sanitize_preserves_valid_whitespace() {
        assert_eq!(sanitize_string("hello\nworld\ttab\rcarriage"), "hello\nworld\ttab\rcarriage");
    }

    // JSON Structured Logging Tests

    /// Test layer that captures events as JSON strings for verification.
    struct JsonCapturingLayer {
        events: Arc<parking_lot::Mutex<Vec<String>>>,
    }

    impl<S: tracing::Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>>
        tracing_subscriber::Layer<S> for JsonCapturingLayer
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if event.metadata().target() == "ledger::events" {
                // Capture event fields as a JSON object
                let mut fields = serde_json::Map::new();

                // Add target for verification
                fields.insert(
                    "target".to_string(),
                    serde_json::Value::String(event.metadata().target().to_string()),
                );

                // Collect all fields
                struct FieldVisitor<'a>(&'a mut serde_json::Map<String, serde_json::Value>);

                impl tracing::field::Visit for FieldVisitor<'_> {
                    fn record_debug(
                        &mut self,
                        field: &tracing::field::Field,
                        value: &dyn std::fmt::Debug,
                    ) {
                        self.0.insert(
                            field.name().to_string(),
                            serde_json::Value::String(format!("{:?}", value)),
                        );
                    }

                    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                        self.0.insert(
                            field.name().to_string(),
                            serde_json::Value::String(value.to_string()),
                        );
                    }

                    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
                        self.0.insert(
                            field.name().to_string(),
                            serde_json::Value::Number(value.into()),
                        );
                    }

                    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                        self.0.insert(
                            field.name().to_string(),
                            serde_json::Value::Number(value.into()),
                        );
                    }

                    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
                        if let Some(n) = serde_json::Number::from_f64(value) {
                            self.0.insert(field.name().to_string(), serde_json::Value::Number(n));
                        }
                    }

                    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
                        self.0.insert(field.name().to_string(), serde_json::Value::Bool(value));
                    }
                }

                let mut visitor = FieldVisitor(&mut fields);
                event.record(&mut visitor);

                if let Ok(json) = serde_json::to_string(&serde_json::Value::Object(fields)) {
                    self.events.lock().push(json);
                }
            }
        }
    }

    fn with_json_capturing_subscriber<F, R>(f: F) -> (R, Vec<String>)
    where
        F: FnOnce() -> R,
    {
        let events = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let layer = JsonCapturingLayer { events: events.clone() };
        let subscriber = tracing_subscriber::registry().with(layer);
        let result = tracing::subscriber::with_default(subscriber, f);
        let captured = events.lock().clone();
        (result, captured)
    }

    #[test]
    fn test_json_output_is_valid_json() {
        let (_, events) = with_json_capturing_subscriber(|| {
            let mut ctx = RequestContext::new("WriteService", "write");
            ctx.set_client_id("test_client");
            ctx.set_success();
            // ctx drops here and emits event
        });

        assert_eq!(events.len(), 1);
        let json: serde_json::Value = serde_json::from_str(&events[0]).unwrap_or_default();
        assert!(json.is_object(), "Output should be valid JSON object");
    }

    #[test]
    fn test_json_output_has_top_level_fields() {
        let (_, events) = with_json_capturing_subscriber(|| {
            let mut ctx = RequestContext::new("WriteService", "write");
            ctx.set_client_id("test_client");
            ctx.set_namespace_id(42);
            ctx.set_success();
        });

        assert_eq!(events.len(), 1);
        let json: serde_json::Value = serde_json::from_str(&events[0]).unwrap_or_default();
        let empty_map = serde_json::Map::new();
        let obj = json.as_object().unwrap_or(&empty_map);

        // Verify key fields are at top level (not nested)
        assert!(obj.contains_key("service"), "service field should be at top level");
        assert!(obj.contains_key("method"), "method field should be at top level");
        assert!(obj.contains_key("client_id"), "client_id field should be at top level");
        assert!(obj.contains_key("namespace_id"), "namespace_id field should be at top level");
        assert!(obj.contains_key("outcome"), "outcome field should be at top level");
    }

    #[test]
    fn test_json_output_includes_target() {
        let (_, events) = with_json_capturing_subscriber(|| {
            let mut ctx = RequestContext::new("WriteService", "write");
            ctx.set_success();
        });

        assert_eq!(events.len(), 1);
        let json: serde_json::Value = serde_json::from_str(&events[0]).unwrap_or_default();
        let target = json.get("target").and_then(|v| v.as_str());
        assert_eq!(target, Some("ledger::events"), "target should be ledger::events");
    }

    #[test]
    fn test_json_output_escapes_special_characters() {
        let (_, events) = with_json_capturing_subscriber(|| {
            let mut ctx = RequestContext::new("WriteService", "write");
            // Include special JSON characters: quotes, backslashes, newlines
            ctx.set_client_id("test\"client\\with\nnewline");
            ctx.set_success();
        });

        assert_eq!(events.len(), 1);
        // If we can parse it, the special characters were properly escaped
        let json: serde_json::Value = serde_json::from_str(&events[0]).unwrap_or_default();
        assert!(json.is_object(), "JSON with special characters should be parseable");

        // The client_id should contain the escaped characters
        let client_id = json.get("client_id").and_then(|v| v.as_str()).unwrap_or("");
        assert!(client_id.contains("test"), "client_id should be present");
    }

    #[test]
    fn test_json_output_includes_duration_ms() {
        let (_, events) = with_json_capturing_subscriber(|| {
            let mut ctx = RequestContext::new("WriteService", "write");
            ctx.set_success();
            std::thread::sleep(std::time::Duration::from_millis(5));
        });

        assert_eq!(events.len(), 1);
        let json: serde_json::Value = serde_json::from_str(&events[0]).unwrap_or_default();

        // duration_ms should be present and be a positive number
        let duration = json.get("duration_ms");
        assert!(duration.is_some(), "duration_ms field should be present");
    }

    #[test]
    fn test_json_outcome_serializes_as_lowercase_string() {
        let (_, events) = with_json_capturing_subscriber(|| {
            let mut ctx = RequestContext::new("WriteService", "write");
            ctx.set_success();
        });

        assert_eq!(events.len(), 1);
        let json: serde_json::Value = serde_json::from_str(&events[0]).unwrap_or_default();
        let outcome = json.get("outcome").and_then(|v| v.as_str());
        assert_eq!(outcome, Some("success"), "outcome should be lowercase 'success'");
    }

    // Task-Local Context Tests

    #[test]
    fn test_with_current_context_returns_none_when_no_guard() {
        // When no guard is active, with_current_context should return None (not panic)
        let result = super::with_current_context(|_ctx| 42);
        assert!(result.is_none(), "Should return None when no context is set");
    }

    #[test]
    fn test_try_with_current_context_returns_false_when_no_guard() {
        // try_with_current_context should return false when no context exists
        let executed = super::try_with_current_context(|_ctx| {
            // This should not execute
        });
        assert!(!executed, "Should return false when no context is set");
    }

    #[test]
    fn test_run_with_sync_sets_and_clears_context() {
        // Context should be accessible within run_with_sync scope
        let (_, events) = with_json_capturing_subscriber(|| {
            let ctx = RequestContext::new("TestService", "test");

            super::RequestContextGuard::run_with_sync(ctx, || {
                // Context should be accessible here
                let found = super::with_current_context(|ctx| {
                    ctx.set_client_id("sync_test_client");
                    ctx.set_success();
                    true
                });
                assert!(found.is_some(), "Context should be accessible within scope");
            });

            // After run_with_sync returns, context should be gone
            let after = super::with_current_context(|_ctx| true);
            assert!(after.is_none(), "Context should be cleared after scope exits");
        });

        // One event should be emitted (from context drop)
        assert_eq!(events.len(), 1);
        let json: serde_json::Value = serde_json::from_str(&events[0]).unwrap_or_default();
        let client_id = json.get("client_id").and_then(|v| v.as_str());
        assert_eq!(client_id, Some("sync_test_client"));
    }

    #[tokio::test]
    async fn test_run_with_async_sets_and_clears_context() {
        // Context should be accessible within run_with scope
        let ctx = RequestContext::new("TestService", "test_async");

        // Use current tokio runtime (from #[tokio::test])
        let result = super::RequestContextGuard::run_with(ctx, async {
            // Context should be accessible here
            super::with_current_context(|ctx| {
                ctx.set_client_id("async_test_client");
                ctx.set_success();
                true
            })
        })
        .await;

        assert!(result.is_some(), "Context should be accessible within async scope");

        // After run_with returns, context should be gone
        let after = super::with_current_context(|_ctx| true);
        assert!(after.is_none(), "Context should be cleared after async scope exits");
    }

    #[tokio::test]
    async fn test_context_survives_across_await_points() {
        // Context should remain accessible across .await points
        // We need to capture events in this test, so we use a captured context
        use std::sync::Arc;

        let captured_client_id = Arc::new(parking_lot::Mutex::new(None::<String>));
        let captured_clone = captured_client_id.clone();

        let ctx = RequestContext::new("TestService", "await_test");

        super::RequestContextGuard::run_with(ctx, async move {
            // Set field before await
            super::with_current_context(|ctx| {
                ctx.set_client_id("before_await");
            });

            // Yield to scheduler (simulates real async work)
            tokio::task::yield_now().await;

            // Context should still be accessible after await
            let found = super::with_current_context(|ctx| {
                ctx.set_sequence(42);
                ctx.set_success();
                // Capture the client_id to verify it persisted
                *captured_clone.lock() = ctx.client_id.clone();
                true
            });
            assert!(found.is_some(), "Context should survive across await points");
        })
        .await;

        let client_id = captured_client_id.lock().clone();
        assert_eq!(
            client_id.as_deref(),
            Some("before_await"),
            "Field set before await should persist"
        );
    }

    #[tokio::test]
    async fn test_tokio_spawn_does_not_inherit_context() {
        // Child tasks created with tokio::spawn should NOT inherit parent's context
        let ctx = RequestContext::new("ParentService", "parent");

        super::RequestContextGuard::run_with(ctx, async {
            super::with_current_context(|ctx| {
                ctx.set_client_id("parent_client");
                ctx.set_success();
            });

            // Spawn a child task
            let handle = tokio::spawn(async {
                // Child should NOT have access to parent's context
                let has_context = super::with_current_context(|_ctx| true);
                has_context.is_some()
            });

            let child_had_context = handle.await.unwrap_or(true);
            assert!(!child_had_context, "tokio::spawn child should NOT inherit parent context");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_tasks_have_independent_contexts() {
        // Multiple concurrent tasks should each have their own independent context
        use std::sync::atomic::{AtomicU32, Ordering};

        let counter = Arc::new(AtomicU32::new(0));
        let mut handles = Vec::new();

        for i in 0..5u64 {
            let counter_clone = counter.clone();
            handles.push(tokio::spawn(async move {
                let ctx = RequestContext::new("ConcurrentService", "concurrent");

                super::RequestContextGuard::run_with(ctx, async move {
                    // Each task sets its own client_id
                    super::with_current_context(|ctx| {
                        ctx.set_client_id(format!("client_{}", i).as_str());
                        ctx.set_sequence(i);
                        ctx.set_success();
                    });

                    // Yield to allow interleaving
                    tokio::task::yield_now().await;

                    // Verify our context wasn't contaminated by other tasks
                    let our_sequence = super::with_current_context(|ctx| ctx.sequence).flatten();
                    if our_sequence == Some(i) {
                        counter_clone.fetch_add(1, Ordering::SeqCst);
                    }
                })
                .await;
            }));
        }

        for handle in handles {
            let _ = handle.await;
        }

        // All 5 contexts should have maintained their correct sequence number
        let correct_count = counter.load(Ordering::SeqCst);
        assert_eq!(correct_count, 5, "All 5 tasks should maintain correct sequence");
    }

    #[test]
    fn test_with_current_context_return_value() {
        // with_current_context should return the closure's return value
        let ctx = RequestContext::new("TestService", "test");

        let result = super::RequestContextGuard::run_with_sync(ctx, || {
            super::with_current_context(|ctx| {
                ctx.set_success();
                42 * 2
            })
        });

        assert_eq!(result, Some(84), "Should return closure's return value");
    }

    #[test]
    fn test_context_modification_persists() {
        // Modifications made via with_current_context should persist
        let (_, events) = with_json_capturing_subscriber(|| {
            let ctx = RequestContext::new("TestService", "persist_test");

            super::RequestContextGuard::run_with_sync(ctx, || {
                // First modification
                super::with_current_context(|ctx| {
                    ctx.set_client_id("first_value");
                });

                // Second modification (should overwrite)
                super::with_current_context(|ctx| {
                    ctx.set_client_id("second_value");
                    ctx.set_success();
                });
            });
        });

        assert_eq!(events.len(), 1);
        let json: serde_json::Value = serde_json::from_str(&events[0]).unwrap_or_default();
        let client_id = json.get("client_id").and_then(|v| v.as_str());
        assert_eq!(client_id, Some("second_value"), "Last write should win");
    }

    // Sampling Tests

    #[test]
    fn test_sampling_config_default_values() {
        let config = SamplingConfig::default();
        assert!((config.error_rate - 1.0).abs() < f64::EPSILON);
        assert!((config.slow_rate - 1.0).abs() < f64::EPSILON);
        assert!((config.vip_rate - 0.5).abs() < f64::EPSILON);
        assert!((config.write_rate - 0.1).abs() < f64::EPSILON);
        assert!((config.read_rate - 0.01).abs() < f64::EPSILON);
        assert!((config.slow_threshold_read_ms - 10.0).abs() < f64::EPSILON);
        assert!((config.slow_threshold_write_ms - 100.0).abs() < f64::EPSILON);
        assert!((config.slow_threshold_admin_ms - 1000.0).abs() < f64::EPSILON);
        assert!(config.vip_namespaces.is_empty());
        assert!(config.enabled);
    }

    #[test]
    fn test_sampling_config_disabled() {
        let config = SamplingConfig::disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn test_sampling_config_for_test() {
        let config = SamplingConfig::for_test();
        assert!(!config.enabled);
    }

    #[test]
    fn test_sampler_disabled_always_samples() {
        let sampler = Sampler::disabled();
        let request_id = Uuid::new_v4();

        // Even with 0% rates, disabled sampler should always sample
        assert!(sampler.should_sample(
            &request_id,
            Some(&Outcome::Success),
            1.0,
            OperationType::Read,
            None,
        ));
    }

    #[test]
    fn test_sampler_errors_always_sampled() {
        // Even with very low sampling rates, errors should always be sampled
        let config =
            SamplingConfig::builder().error_rate(1.0).read_rate(0.0).write_rate(0.0).build();
        let sampler = Sampler::new(config);

        // Test 100 different request IDs - all errors should be sampled
        for _ in 0..100 {
            let request_id = Uuid::new_v4();
            let outcome =
                Outcome::Error { code: "INTERNAL".to_string(), message: "Test error".to_string() };
            assert!(
                sampler.should_sample(&request_id, Some(&outcome), 1.0, OperationType::Read, None),
                "Error events should always be sampled at 100%"
            );
        }
    }

    #[test]
    fn test_sampler_slow_requests_always_sampled() {
        // Slow requests should always be sampled at 100%
        let config = SamplingConfig::builder()
            .slow_rate(1.0)
            .slow_threshold_read_ms(10.0)
            .read_rate(0.0)
            .build();
        let sampler = Sampler::new(config);

        // Test 100 different slow read requests
        for _ in 0..100 {
            let request_id = Uuid::new_v4();
            assert!(
                sampler.should_sample(
                    &request_id,
                    Some(&Outcome::Success),
                    15.0, // Above 10ms threshold
                    OperationType::Read,
                    None,
                ),
                "Slow requests should always be sampled at 100%"
            );
        }
    }

    #[test]
    fn test_sampler_slow_threshold_by_operation_type() {
        let config = SamplingConfig::builder()
            .slow_rate(1.0)
            .slow_threshold_read_ms(10.0)
            .slow_threshold_write_ms(100.0)
            .slow_threshold_admin_ms(1000.0)
            .build();
        let sampler = Sampler::new(config);
        let request_id = Uuid::new_v4();

        // 15ms is slow for reads (threshold 10ms)
        assert!(sampler.should_sample(
            &request_id,
            Some(&Outcome::Success),
            15.0,
            OperationType::Read,
            None,
        ));

        // 15ms is NOT slow for writes (threshold 100ms) - will be sampled based on write_rate
        // We test this by checking that a different decision is made

        // 150ms is slow for writes (threshold 100ms)
        assert!(sampler.should_sample(
            &request_id,
            Some(&Outcome::Success),
            150.0,
            OperationType::Write,
            None,
        ));

        // 1500ms is slow for admin (threshold 1000ms)
        assert!(sampler.should_sample(
            &request_id,
            Some(&Outcome::Success),
            1500.0,
            OperationType::Admin,
            None,
        ));
    }

    #[test]
    fn test_sampler_admin_operations_always_sampled() {
        // Admin operations should always be sampled at 100%
        let config = SamplingConfig::builder().read_rate(0.0).write_rate(0.0).build();
        let sampler = Sampler::new(config);

        // Test 100 different admin operations
        for _ in 0..100 {
            let request_id = Uuid::new_v4();
            assert!(
                sampler.should_sample(
                    &request_id,
                    Some(&Outcome::Success),
                    1.0, // Fast request
                    OperationType::Admin,
                    None,
                ),
                "Admin operations should always be sampled at 100%"
            );
        }
    }

    #[test]
    fn test_sampler_vip_namespaces_elevated_rate() {
        // VIP namespaces should have higher sampling rate
        let config = SamplingConfig::builder()
            .vip_rate(1.0) // 100% for VIP
            .read_rate(0.0) // 0% for normal
            .vip_namespaces(vec![42, 100])
            .build();
        let sampler = Sampler::new(config);

        // VIP namespace should always be sampled
        for _ in 0..100 {
            let request_id = Uuid::new_v4();
            assert!(
                sampler.should_sample(
                    &request_id,
                    Some(&Outcome::Success),
                    1.0,
                    OperationType::Read,
                    Some(42),
                ),
                "VIP namespace should be sampled at 100%"
            );
        }
    }

    #[test]
    fn test_sampler_non_vip_namespace_normal_rate() {
        // Non-VIP namespaces should use normal sampling rates
        let config = SamplingConfig::builder()
            .vip_rate(1.0)
            .read_rate(0.0) // 0% for normal reads
            .vip_namespaces(vec![42])
            .build();
        let sampler = Sampler::new(config);

        // Non-VIP namespace (99) should NOT be sampled at 0% rate
        let mut sampled_count = 0;
        for _ in 0..100 {
            let request_id = Uuid::new_v4();
            if sampler.should_sample(
                &request_id,
                Some(&Outcome::Success),
                1.0,
                OperationType::Read,
                Some(99), // Not in VIP list
            ) {
                sampled_count += 1;
            }
        }
        assert_eq!(sampled_count, 0, "Non-VIP namespace at 0% rate should never be sampled");
    }

    #[test]
    fn test_sampler_deterministic_with_same_request_id() {
        // Same request_id should always produce same sampling decision
        let config = SamplingConfig::builder()
            .read_rate(0.5) // 50% rate for clear determinism test
            .build();
        let sampler = Sampler::new(config);
        let request_id = Uuid::new_v4();

        let first_decision = sampler.should_sample(
            &request_id,
            Some(&Outcome::Success),
            1.0,
            OperationType::Read,
            None,
        );

        // Same request_id should produce same decision every time
        for _ in 0..100 {
            let decision = sampler.should_sample(
                &request_id,
                Some(&Outcome::Success),
                1.0,
                OperationType::Read,
                None,
            );
            assert_eq!(
                decision, first_decision,
                "Same request_id should always produce same sampling decision"
            );
        }
    }

    #[test]
    fn test_sampler_convergence_to_configured_rate() {
        // Over many requests, sampling rate should converge to configured rate (±5%)
        let config = SamplingConfig::builder().read_rate(0.5).build(); // 50%
        let sampler = Sampler::new(config);

        let mut sampled = 0;
        let total = 10000;

        for _ in 0..total {
            let request_id = Uuid::new_v4();
            if sampler.should_sample(
                &request_id,
                Some(&Outcome::Success),
                1.0,
                OperationType::Read,
                None,
            ) {
                sampled += 1;
            }
        }

        let rate = sampled as f64 / total as f64;
        assert!(
            (rate - 0.5).abs() < 0.05,
            "Sampling rate should converge to 50% (actual: {:.1}%)",
            rate * 100.0
        );
    }

    #[test]
    fn test_sampler_rate_zero_never_samples() {
        let config = SamplingConfig::builder().read_rate(0.0).build();
        let sampler = Sampler::new(config);

        let mut sampled = 0;
        for _ in 0..1000 {
            let request_id = Uuid::new_v4();
            if sampler.should_sample(
                &request_id,
                Some(&Outcome::Success),
                1.0,
                OperationType::Read,
                None,
            ) {
                sampled += 1;
            }
        }

        assert_eq!(sampled, 0, "0% rate should never sample");
    }

    #[test]
    fn test_sampler_rate_one_always_samples() {
        let config = SamplingConfig::builder().read_rate(1.0).build();
        let sampler = Sampler::new(config);

        let mut sampled = 0;
        for _ in 0..1000 {
            let request_id = Uuid::new_v4();
            if sampler.should_sample(
                &request_id,
                Some(&Outcome::Success),
                1.0,
                OperationType::Read,
                None,
            ) {
                sampled += 1;
            }
        }

        assert_eq!(sampled, 1000, "100% rate should always sample");
    }

    #[test]
    fn test_sampled_event_is_emitted() {
        // When sampled, event should be emitted
        let (_, events) = with_test_subscriber(|| {
            let mut ctx = RequestContext::new("WriteService", "write");
            ctx.set_sampler(Sampler::disabled()); // Disabled = always sample
            ctx.set_success();
            drop(ctx);
        });

        assert_eq!(events, 1, "Sampled event should be emitted");
    }

    #[test]
    fn test_unsampled_event_not_emitted() {
        // When not sampled, event should not be emitted
        let (_, events) = with_test_subscriber(|| {
            let config = SamplingConfig::builder()
                .write_rate(0.0) // 0% sampling
                .build();
            let mut ctx = RequestContext::new("WriteService", "write");
            ctx.set_sampler(Sampler::new(config));
            ctx.set_operation_type(OperationType::Write);
            ctx.set_success();
            drop(ctx);
        });

        assert_eq!(events, 0, "Unsampled event should not be emitted");
    }

    #[test]
    fn test_operation_type_setter() {
        let mut ctx = RequestContext::new("WriteService", "write");
        assert!(matches!(ctx.operation_type, OperationType::Write)); // Default

        ctx.set_operation_type(OperationType::Read);
        assert!(matches!(ctx.operation_type, OperationType::Read));

        ctx.set_operation_type(OperationType::Admin);
        assert!(matches!(ctx.operation_type, OperationType::Admin));

        ctx.suppress_emission();
    }

    // --- Admin-specific field tests ---

    #[test]
    fn test_set_admin_action() {
        let mut ctx = RequestContext::new("AdminService", "create_namespace");
        assert!(ctx.admin_action.is_none());

        ctx.set_admin_action("create_namespace");
        assert!(matches!(ctx.admin_action, Some("create_namespace")));

        // Overwrite works
        ctx.set_admin_action("delete_namespace");
        assert!(matches!(ctx.admin_action, Some("delete_namespace")));

        ctx.suppress_emission();
    }

    #[test]
    fn test_set_target_namespace_name() {
        let mut ctx = RequestContext::new("AdminService", "create_namespace");
        assert!(ctx.target_namespace_name.is_none());

        ctx.set_target_namespace_name("my-namespace");
        assert!(matches!(ctx.target_namespace_name, Some(ref n) if n == "my-namespace"));

        ctx.suppress_emission();
    }

    #[test]
    fn test_target_namespace_name_truncation() {
        let mut ctx = RequestContext::new("AdminService", "create_namespace");
        let long_name = "x".repeat(200);

        ctx.set_target_namespace_name(&long_name);
        // Should truncate to 125 chars + "..." = 128 chars total (max_len - 3 + 3)
        assert!(matches!(ctx.target_namespace_name, Some(ref n) if n.len() == 128));
        assert!(matches!(ctx.target_namespace_name, Some(ref n) if n.ends_with("...")));

        ctx.suppress_emission();
    }

    #[test]
    fn test_set_retention_mode() {
        let mut ctx = RequestContext::new("AdminService", "create_vault");
        assert!(ctx.retention_mode.is_none());

        ctx.set_retention_mode("full");
        assert!(matches!(ctx.retention_mode, Some(ref m) if m == "full"));

        ctx.set_retention_mode("compacted");
        assert!(matches!(ctx.retention_mode, Some(ref m) if m == "compacted"));

        ctx.suppress_emission();
    }

    #[test]
    fn test_set_recovery_force() {
        let mut ctx = RequestContext::new("AdminService", "recover_vault");
        assert!(ctx.recovery_force.is_none());

        ctx.set_recovery_force(true);
        assert!(matches!(ctx.recovery_force, Some(true)));

        ctx.set_recovery_force(false);
        assert!(matches!(ctx.recovery_force, Some(false)));

        ctx.suppress_emission();
    }

    #[test]
    fn test_admin_context_with_all_fields() {
        let mut ctx = RequestContext::new("AdminService", "create_namespace");
        ctx.set_operation_type(OperationType::Admin);
        ctx.set_admin_action("create_namespace");
        ctx.set_target_namespace_name("test-namespace");
        ctx.set_node_id(12345);
        ctx.set_is_leader(true);
        ctx.set_raft_term(42);

        assert!(matches!(ctx.admin_action, Some("create_namespace")));
        assert!(matches!(ctx.target_namespace_name, Some(ref n) if n == "test-namespace"));
        assert!(matches!(ctx.node_id, Some(12345)));
        assert!(matches!(ctx.is_leader, Some(true)));
        assert!(matches!(ctx.raft_term, Some(42)));
        assert!(matches!(ctx.operation_type, OperationType::Admin));

        ctx.suppress_emission();
    }

    #[test]
    fn test_admin_operation_convenience_method() {
        let mut ctx = RequestContext::new("AdminService", "delete_vault");
        ctx.set_admin_operation("delete_vault");

        assert!(matches!(ctx.admin_action, Some("delete_vault")));

        ctx.suppress_emission();
    }

    // === VIP Status Tests ===

    #[test]
    fn test_set_is_vip_true() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_is_vip(true);

        assert_eq!(ctx.is_vip, Some(true));

        ctx.suppress_emission();
    }

    #[test]
    fn test_set_is_vip_false() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_is_vip(false);

        assert_eq!(ctx.is_vip, Some(false));

        ctx.suppress_emission();
    }

    #[test]
    fn test_is_vip_in_json_output() {
        let (_, events) = with_json_capturing_subscriber(|| {
            let mut ctx = RequestContext::new("WriteService", "write");
            ctx.set_namespace_id(42);
            ctx.set_is_vip(true);
            ctx.set_success();
        });

        assert!(!events.is_empty(), "Expected at least one event");
        let json: serde_json::Value = serde_json::from_str(&events[0]).unwrap_or_default();
        assert_eq!(json["is_vip"], serde_json::Value::Bool(true));
        assert_eq!(json["namespace_id"], serde_json::Value::Number(42.into()));
    }

    // === Trace Context Tests ===

    #[test]
    fn test_set_trace_id() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_trace_id("00112233445566778899aabbccddeeff".to_string());

        assert_eq!(ctx.trace_id, Some("00112233445566778899aabbccddeeff".to_string()));

        ctx.suppress_emission();
    }

    #[test]
    fn test_set_span_id() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_span_id("0011223344556677".to_string());

        assert_eq!(ctx.span_id, Some("0011223344556677".to_string()));

        ctx.suppress_emission();
    }

    #[test]
    fn test_set_parent_span_id() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_parent_span_id("aabbccddeeff0011".to_string());

        assert_eq!(ctx.parent_span_id, Some("aabbccddeeff0011".to_string()));

        ctx.suppress_emission();
    }

    #[test]
    fn test_set_trace_flags() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_trace_flags(0x01); // Sampled bit

        assert_eq!(ctx.trace_flags, Some(0x01));

        ctx.suppress_emission();
    }

    #[test]
    fn test_set_trace_context() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_trace_context(
            "00112233445566778899aabbccddeeff",
            "0011223344556677",
            Some("aabbccddeeff0011"),
            0x01,
        );

        assert_eq!(ctx.trace_id, Some("00112233445566778899aabbccddeeff".to_string()));
        assert_eq!(ctx.span_id, Some("0011223344556677".to_string()));
        assert_eq!(ctx.parent_span_id, Some("aabbccddeeff0011".to_string()));
        assert_eq!(ctx.trace_flags, Some(0x01));

        ctx.suppress_emission();
    }

    #[test]
    fn test_set_trace_context_without_parent() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.set_trace_context("00112233445566778899aabbccddeeff", "0011223344556677", None, 0x00);

        assert_eq!(ctx.trace_id, Some("00112233445566778899aabbccddeeff".to_string()));
        assert_eq!(ctx.span_id, Some("0011223344556677".to_string()));
        assert!(ctx.parent_span_id.is_none());
        assert_eq!(ctx.trace_flags, Some(0x00));

        ctx.suppress_emission();
    }

    #[test]
    fn test_trace_context_in_json_output() {
        let (_, events) = with_json_capturing_subscriber(|| {
            let mut ctx = RequestContext::new("WriteService", "write");
            ctx.set_trace_context(
                "00112233445566778899aabbccddeeff",
                "0011223344556677",
                Some("aabbccddeeff0011"),
                0x01,
            );
            ctx.set_success();
            // Drop emits event
        });

        assert_eq!(events.len(), 1);
        let event = &events[0];

        // Verify trace context fields are in JSON
        assert!(event.contains("\"trace_id\":\"00112233445566778899aabbccddeeff\""));
        assert!(event.contains("\"span_id\":\"0011223344556677\""));
        assert!(event.contains("\"parent_span_id\":\"aabbccddeeff0011\""));
        assert!(event.contains("\"trace_flags\":1"));
    }

    // === Canonical Log Line Field Tests (Task 16) ===

    #[test]
    fn test_bytes_read_in_json_output() {
        let (_, events) = with_json_capturing_subscriber(|| {
            let mut ctx = RequestContext::new("ReadService", "read");
            ctx.set_bytes_read(4096);
            ctx.set_success();
        });

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert!(event.contains("\"bytes_read\":4096"), "bytes_read field missing: {event}");
    }

    #[test]
    fn test_bytes_written_in_json_output() {
        let (_, events) = with_json_capturing_subscriber(|| {
            let mut ctx = RequestContext::new("WriteService", "write");
            ctx.set_bytes_written(2048);
            ctx.set_success();
        });

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert!(event.contains("\"bytes_written\":2048"), "bytes_written field missing: {event}");
    }

    #[test]
    fn test_raft_round_trips_in_json_output() {
        let (_, events) = with_json_capturing_subscriber(|| {
            let mut ctx = RequestContext::new("WriteService", "write");
            ctx.set_raft_round_trips(3);
            ctx.set_success();
        });

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert!(
            event.contains("\"raft_round_trips\":3"),
            "raft_round_trips field missing: {event}"
        );
    }

    #[test]
    fn test_sdk_version_in_json_output() {
        let (_, events) = with_json_capturing_subscriber(|| {
            let mut ctx = RequestContext::new("ReadService", "read");
            ctx.set_sdk_version("rust-sdk/0.1.0");
            ctx.set_success();
        });

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert!(
            event.contains("\"sdk_version\":\"rust-sdk/0.1.0\""),
            "sdk_version field missing: {event}"
        );
    }

    #[test]
    fn test_source_ip_in_json_output() {
        let (_, events) = with_json_capturing_subscriber(|| {
            let mut ctx = RequestContext::new("ReadService", "read");
            ctx.set_source_ip("192.168.1.100");
            ctx.set_success();
        });

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert!(
            event.contains("\"source_ip\":\"192.168.1.100\""),
            "source_ip field missing: {event}"
        );
    }

    #[test]
    fn test_end_raft_timer_auto_increments_round_trips() {
        let mut ctx = RequestContext::new("WriteService", "write");

        assert!(ctx.raft_round_trips.is_none());

        // First raft round trip
        ctx.start_raft_timer();
        ctx.end_raft_timer();
        assert_eq!(ctx.raft_round_trips, Some(1));

        // Second raft round trip
        ctx.start_raft_timer();
        ctx.end_raft_timer();
        assert_eq!(ctx.raft_round_trips, Some(2));

        // Manual set overrides
        ctx.set_raft_round_trips(10);
        assert_eq!(ctx.raft_round_trips, Some(10));

        ctx.suppress_emission();
    }

    #[test]
    fn test_increment_raft_round_trips_without_timer() {
        let mut ctx = RequestContext::new("WriteService", "write");

        ctx.increment_raft_round_trips();
        assert_eq!(ctx.raft_round_trips, Some(1));

        ctx.increment_raft_round_trips();
        assert_eq!(ctx.raft_round_trips, Some(2));

        ctx.suppress_emission();
    }

    #[test]
    fn test_extract_transport_metadata() {
        let mut ctx = RequestContext::new("WriteService", "write");

        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata
            .insert("x-sdk-version", tonic::metadata::MetadataValue::from_static("rust-sdk/0.2.0"));
        metadata.insert(
            "x-forwarded-for",
            tonic::metadata::MetadataValue::from_static("10.0.0.1, 192.168.1.1"),
        );

        ctx.extract_transport_metadata(&metadata);

        assert_eq!(ctx.sdk_version.as_deref(), Some("rust-sdk/0.2.0"));
        // extract_transport_metadata takes the first IP from x-forwarded-for (client IP)
        assert_eq!(ctx.source_ip.as_deref(), Some("10.0.0.1"));

        ctx.suppress_emission();
    }

    #[test]
    fn test_extract_transport_metadata_empty() {
        let mut ctx = RequestContext::new("WriteService", "write");

        let metadata = tonic::metadata::MetadataMap::new();
        ctx.extract_transport_metadata(&metadata);

        assert!(ctx.sdk_version.is_none());
        assert!(ctx.source_ip.is_none());

        ctx.suppress_emission();
    }

    #[test]
    fn test_canonical_log_line_type_alias() {
        // CanonicalLogLine is just a type alias for RequestContext
        let mut ctx: CanonicalLogLine = CanonicalLogLine::new("WriteService", "write");
        ctx.set_bytes_written(512);
        ctx.set_sdk_version("rust-sdk/0.1.0");
        ctx.set_source_ip("127.0.0.1");
        ctx.set_success();

        // If it compiles and runs, the alias works
        ctx.suppress_emission();
    }

    #[test]
    fn test_canonical_log_line_completeness() {
        // Verify that a fully-populated canonical log line emits all field groups.
        let (_, events) = with_json_capturing_subscriber(|| {
            let mut ctx = RequestContext::new("WriteService", "write");

            // Identity fields
            ctx.set_client_info("client_123", 42, Some("user@example.com".into()));

            // Target fields
            ctx.set_target(1, 2);

            // System fields
            ctx.set_node_id(100);
            ctx.set_is_leader(true);
            ctx.set_raft_term(5);
            ctx.set_shard_id(3);

            // Write metrics
            ctx.set_operations_count(10);
            ctx.set_bytes_written(4096);
            ctx.set_raft_round_trips(2);

            // Client transport metadata
            ctx.set_sdk_version("rust-sdk/0.3.0");
            ctx.set_source_ip("10.0.0.42");

            // Tracing context
            ctx.set_trace_context(
                "aabbccdd11223344aabbccdd11223344",
                "1122334455667788",
                None,
                0x01,
            );

            // Outcome
            ctx.set_success();
        });

        assert_eq!(events.len(), 1);
        let event = &events[0];

        // Identity
        assert!(event.contains("\"service\":\"WriteService\""), "missing service");
        assert!(event.contains("\"method\":\"write\""), "missing method");
        assert!(event.contains("\"client_id\":\"client_123\""), "missing client_id");

        // Target
        assert!(event.contains("\"namespace_id\":1"), "missing namespace_id");
        assert!(event.contains("\"vault_id\":2"), "missing vault_id");

        // System
        assert!(event.contains("\"node_id\":100"), "missing node_id");
        assert!(event.contains("\"is_leader\":true"), "missing is_leader");
        assert!(event.contains("\"raft_term\":5"), "missing raft_term");
        assert!(event.contains("\"shard_id\":3"), "missing shard_id");

        // Write metrics
        assert!(event.contains("\"operations_count\":10"), "missing operations_count");
        assert!(event.contains("\"bytes_written\":4096"), "missing bytes_written");
        assert!(event.contains("\"raft_round_trips\":2"), "missing raft_round_trips");

        // Client transport
        assert!(event.contains("\"sdk_version\":\"rust-sdk/0.3.0\""), "missing sdk_version");
        assert!(event.contains("\"source_ip\":\"10.0.0.42\""), "missing source_ip");

        // Tracing
        assert!(
            event.contains("\"trace_id\":\"aabbccdd11223344aabbccdd11223344\""),
            "missing trace_id"
        );

        // Outcome
        assert!(event.contains("\"outcome\":\"success\""), "missing outcome");

        // Timing (always present)
        assert!(event.contains("\"duration_ms\":"), "missing duration_ms");

        // Message format
        assert!(event.contains("canonical_log_line"), "missing canonical_log_line message");
    }
}
