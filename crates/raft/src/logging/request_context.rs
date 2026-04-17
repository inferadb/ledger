//! Request context management for canonical log line emission.
//!
//! The core abstraction is [`RequestContext`] (aliased as [`CanonicalLogLine`]),
//! a builder that accumulates fields throughout the request lifecycle. On drop,
//! it emits a single structured event to the `ledger::events` tracing target.

use std::{cell::RefCell, sync::Arc, time::Instant};

use inferadb_ledger_types::Region;
use tracing::info;
use uuid::Uuid;

use super::sampling::{OperationType, Outcome, Sampler};

// Task-local storage for the current request context.
// Uses tokio::task_local! instead of thread_local! because tokio's work-stealing
// scheduler moves tasks between OS threads at await points.
tokio::task_local! {
    static CURRENT_CONTEXT: RefCell<RequestContext>;
}

/// Timing breakdown for request phases.
#[derive(Debug, Default)]
pub(crate) struct TimingBreakdown {
    /// When Raft consensus started.
    pub(crate) raft_start: Option<Instant>,
    /// Duration in Raft consensus (milliseconds).
    pub(crate) raft_latency_ms: Option<f64>,
    /// When storage operation started.
    pub(crate) storage_start: Option<Instant>,
    /// Duration in storage layer (milliseconds).
    pub(crate) storage_latency_ms: Option<f64>,
}

/// Truncates a string to a maximum length, appending "..." if truncated.
pub(super) fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

/// Sanitizes a string by replacing control characters with the replacement character.
pub(super) fn sanitize_string(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_control() && c != '\n' && c != '\r' && c != '\t' { '\u{FFFD}' } else { c })
        .collect()
}

/// Truncates a hex hash to 16 characters.
pub(super) fn truncate_hash(hash: &str) -> String {
    if hash.len() <= 16 { hash.to_string() } else { hash[..16].to_string() }
}

/// Accesses the current task-local request context, if one exists.
///
/// Provides safe access to the request context from anywhere
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
/// use inferadb_ledger_raft::logging::with_current_context;
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
/// instead of `None`. Useful when you want to perform an action if context
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
/// canonical log line (via the context's Drop implementation).
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
/// use inferadb_ledger_raft::logging::{RequestContext, RequestContextGuard, with_current_context};
///
/// async fn handle_request() {
///     let ctx = RequestContext::new("WriteService", "write");
///     RequestContextGuard::run_with(ctx, async {
///         // Context is accessible here and across await points
///         with_current_context(|ctx| {
///             ctx.set_client_info("client_123", 42);
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
    /// canonical log line is emitted.
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
    /// closure's execution. When the closure returns, the context's canonical log line is emitted.
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

/// Context builder for canonical log line emission.
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
pub struct RequestContext {
    // Request metadata
    pub(crate) request_id: Uuid,
    pub(crate) service: &'static str,
    pub(crate) method: &'static str,
    pub(crate) start_time: Instant,

    // Client info
    pub(crate) client_id: Option<String>,
    pub(crate) sequence: Option<u64>,

    // Caller identity (UserSlug from gRPC request)
    pub(crate) caller: Option<u64>,

    // Target
    pub(crate) organization: Option<u64>,
    pub(crate) vault: Option<u64>,

    // System context (populated externally)
    pub(crate) node_id: Option<u64>,
    pub(crate) is_leader: Option<bool>,
    pub(crate) raft_term: Option<u64>,
    pub(crate) region: Option<Region>,

    // Operation-specific (write)
    pub(crate) operations_count: Option<usize>,
    pub(crate) operation_types: Option<Vec<&'static str>>,
    pub(crate) include_tx_proof: Option<bool>,
    pub(crate) idempotency_hit: Option<bool>,
    pub(crate) batch_coalesced: Option<bool>,
    pub(crate) batch_size: Option<usize>,
    pub(crate) condition_type: Option<String>,

    // Operation-specific (read)
    pub(crate) key: Option<String>,
    pub(crate) keys_count: Option<usize>,
    pub(crate) found_count: Option<usize>,
    pub(crate) consistency: Option<String>,
    pub(crate) at_height: Option<u64>,
    pub(crate) include_proof: Option<bool>,
    pub(crate) proof_size_bytes: Option<usize>,
    pub(crate) found: Option<bool>,
    pub(crate) value_size_bytes: Option<usize>,

    // Operation-specific (admin)
    pub(crate) admin_action: Option<&'static str>,
    pub(crate) retention_mode: Option<String>,
    pub(crate) recovery_force: Option<bool>,

    // Outcome
    pub(crate) outcome: Option<Outcome>,
    pub(crate) block_height: Option<u64>,
    pub(crate) block_hash: Option<String>,
    pub(crate) state_root: Option<String>,

    // I/O metrics
    pub(crate) bytes_read: Option<usize>,
    pub(crate) bytes_written: Option<usize>,
    pub(crate) raft_round_trips: Option<u32>,

    // Client transport metadata
    pub(crate) sdk_version: Option<String>,
    pub(crate) source_ip: Option<String>,

    // Tracing context (W3C Trace Context)
    pub(crate) trace_id: Option<String>,
    pub(crate) span_id: Option<String>,
    pub(crate) parent_span_id: Option<String>,
    pub(crate) trace_flags: Option<u8>,

    // Timing
    pub(crate) timing: TimingBreakdown,

    // Emission control
    pub(crate) emitted: bool,

    // Sampling
    pub(crate) operation_type: OperationType,
    pub(crate) sampler: Option<Sampler>,

    // Metric emission — true for gRPC handlers, false for background jobs
    pub(crate) emit_grpc_metric: bool,

    // Event handle for handler-phase business event emission (not Debug — trait object)
    pub(crate) event_handle: Option<Arc<dyn crate::event_writer::EventEmitter>>,
}

// Manual Debug because `dyn EventEmitter` is not Debug.
impl std::fmt::Debug for RequestContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestContext")
            .field("request_id", &self.request_id)
            .field("service", &self.service)
            .field("method", &self.method)
            .field("emit_grpc_metric", &self.emit_grpc_metric)
            .field("event_handle", &self.event_handle.as_ref().map(|_| "..."))
            .finish_non_exhaustive()
    }
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
            caller: None,
            organization: None,
            vault: None,
            node_id: None,
            is_leader: None,
            raft_term: None,
            region: None,
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
            emit_grpc_metric: false,
            event_handle: None,
        }
    }

    /// Creates a new request context with a specific request ID (for testing).
    #[must_use]
    pub fn with_request_id(service: &'static str, method: &'static str, request_id: Uuid) -> Self {
        let mut ctx = Self::new(service, method);
        ctx.request_id = request_id;
        ctx
    }

    /// Creates a context for a gRPC handler.
    ///
    /// Extracts transport metadata (SDK version, source IP) and trace context
    /// (W3C traceparent) from the request. Emits a gRPC request metric and
    /// the canonical log line on Drop.
    ///
    /// # Arguments
    ///
    /// * `service` - The gRPC service name (e.g., `"WriteService"`).
    /// * `method` - The gRPC method name (e.g., `"write"`).
    /// * `request` - The tonic request to extract metadata from.
    /// * `event_handle` - Optional trait-erased event handle for business event emission.
    #[must_use]
    pub fn from_request<T>(
        service: &'static str,
        method: &'static str,
        request: &tonic::Request<T>,
        event_handle: Option<Arc<dyn crate::event_writer::EventEmitter>>,
    ) -> Self {
        let mut ctx = Self::new(service, method);
        ctx.emit_grpc_metric = true;
        ctx.event_handle = event_handle;
        ctx.extract_transport_metadata(request.metadata());

        let trace_ctx = crate::trace_context::extract_or_generate(request.metadata());
        ctx.set_trace_context_from(&trace_ctx);

        ctx
    }

    /// Copies trace fields from a [`TraceContext`](crate::trace_context::TraceContext).
    pub fn set_trace_context_from(&mut self, trace_ctx: &crate::trace_context::TraceContext) {
        self.trace_id = Some(trace_ctx.trace_id.clone());
        self.span_id = Some(trace_ctx.span_id.clone());
        self.parent_span_id = trace_ctx.parent_span_id.clone();
        self.trace_flags = Some(trace_ctx.trace_flags);
    }

    /// Returns the request ID.
    #[must_use]
    pub fn request_id(&self) -> Uuid {
        self.request_id
    }

    /// Returns the W3C trace ID if one was set or extracted from request metadata.
    ///
    /// Use this to pass the trace ID to correlation helpers such as
    /// `response_with_correlation` and `status_with_correlation` after calling
    /// [`from_request`](Self::from_request) instead of keeping a separate
    /// `trace_ctx` variable.
    #[must_use]
    pub fn trace_id(&self) -> &str {
        self.trace_id.as_deref().unwrap_or("")
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
    pub fn set_client_info(&mut self, client_id: &str, sequence: u64) {
        self.client_id = Some(truncate_string(&sanitize_string(client_id), 128));
        self.sequence = Some(sequence);
    }

    /// Sets the client ID only.
    pub fn set_client_id(&mut self, client_id: &str) {
        self.client_id = Some(truncate_string(&sanitize_string(client_id), 128));
    }

    /// Sets the sequence number only.
    pub fn set_sequence(&mut self, sequence: u64) {
        self.sequence = Some(sequence);
    }

    /// Sets the caller's user slug (external Snowflake identifier).
    ///
    /// Extracted from the `UserSlug caller` field on gRPC request messages.
    /// Logged as an opaque numeric identifier — never contains PII.
    pub fn set_caller(&mut self, slug: u64) {
        self.caller = Some(slug);
    }

    /// Returns the caller if set, or 0 for system-initiated requests.
    pub fn caller_or_zero(&self) -> u64 {
        self.caller.unwrap_or(0)
    }

    // =========================================================================
    // Target setters
    // =========================================================================

    /// Sets the target organization and vault.
    pub fn set_target(&mut self, organization: u64, vault: u64) {
        self.organization = Some(organization);
        self.vault = Some(vault);
    }

    /// Sets the organization only.
    pub fn set_organization(&mut self, organization: u64) {
        self.organization = Some(organization);
    }

    /// Sets the vault only.
    pub fn set_vault(&mut self, vault: u64) {
        self.vault = Some(vault);
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
        region: Option<Region>,
    ) {
        self.node_id = Some(node_id);
        self.is_leader = Some(is_leader);
        self.raft_term = Some(raft_term);
        self.region = region;
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

    /// Sets the region ID.
    pub fn set_region(&mut self, region: Region) {
        self.region = Some(region);
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
    /// Example values: "create_organization", "delete_vault", "recover_vault"
    pub fn set_admin_action(&mut self, action: &'static str) {
        self.admin_action = Some(action);
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
    // Event emission
    // =========================================================================

    /// Records a handler-phase business event using this context's scope fields.
    ///
    /// Organization, vault, caller, and trace ID are read from `self` and
    /// merged into the event entry. Additional key-value details can be supplied
    /// via the `details` slice.
    ///
    /// This is a no-op if no event handle was set (e.g., background jobs or
    /// contexts created via [`new()`](Self::new) rather than
    /// [`from_request()`](Self::from_request)).
    ///
    /// Named `record_event` (not `emit_event`) because `emit_event` is the
    /// internal method that emits the canonical log line on Drop.
    pub fn record_event(
        &self,
        action: inferadb_ledger_types::events::EventAction,
        outcome: inferadb_ledger_types::events::EventOutcome,
        details: &[(&str, &str)],
    ) {
        let Some(handle) = &self.event_handle else {
            return;
        };

        let org_id =
            inferadb_ledger_types::OrganizationId::new(self.organization.unwrap_or(0) as i64);
        let org_slug = self.organization.map(inferadb_ledger_types::OrganizationSlug::new);
        let vault_slug = self.vault.map(inferadb_ledger_types::VaultSlug::new);
        let node_id = self.node_id.unwrap_or(0);

        let principal = match self.caller {
            Some(slug) => format!("user:{slug}"),
            None => String::new(),
        };

        let mut emitter = crate::event_writer::HandlerPhaseEmitter::for_organization(
            action, org_id, org_slug, node_id,
        )
        .principal(&principal)
        .outcome(outcome);

        if let Some(slug) = vault_slug {
            emitter = emitter.vault(slug);
        }

        if let Some(ref tid) = self.trace_id {
            emitter = emitter.trace_id(tid);
        }

        for &(k, v) in details {
            emitter = emitter.detail(k, v);
        }

        // Use 90-day TTL, matching existing handler-phase patterns.
        let entry = emitter.build(90);
        handle.record_event(entry);
    }

    /// Sets the event handle for handler-phase event emission.
    pub fn set_event_handle(&mut self, handle: Arc<dyn crate::event_writer::EventEmitter>) {
        self.event_handle = Some(handle);
    }

    // =========================================================================
    // Metric helpers (used by Drop)
    // =========================================================================

    /// Fields emitted as metric labels. ONLY bounded-enum values.
    ///
    /// Entity IDs (org, vault, caller) are deliberately excluded —
    /// they appear in logs and events where high cardinality is cheap.
    /// Region is excluded because it is a scraper-implicit label added
    /// by the metrics infrastructure, not by the instrumented code.
    ///
    /// Future label changes happen HERE, in one method, once.
    #[allow(dead_code)]
    pub(crate) fn metric_labels(&self) -> Vec<(&'static str, String)> {
        vec![
            (super::fields::SERVICE, self.service.to_string()),
            (super::fields::METHOD, self.method.to_string()),
            (super::fields::STATUS, self.grpc_status_str().to_string()),
        ]
    }

    /// Maps the request outcome to a gRPC status string for metrics.
    fn grpc_status_str(&self) -> &str {
        match &self.outcome {
            Some(Outcome::Success | Outcome::Cached) => "OK",
            Some(Outcome::Error { code, .. }) => code.as_str(),
            Some(Outcome::RateLimited) => "RESOURCE_EXHAUSTED",
            Some(Outcome::PreconditionFailed { .. }) => "FAILED_PRECONDITION",
            None => "UNKNOWN",
        }
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
                self.organization,
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
            caller = self.caller,
            organization = self.organization,
            vault = self.vault,
            node_id = self.node_id,
            is_leader = self.is_leader,
            raft_term = self.raft_term,
            region = self.region.map(|r| r.as_str()),
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
        #[cfg(feature = "observability")]
        if crate::otel::is_otel_enabled() {
            let attrs = crate::otel::SpanAttributes {
                request_id: Some(self.request_id.to_string()),
                client_id: self.client_id.clone(),
                sequence: self.sequence,
                organization: self.organization,
                vault: self.vault,
                service: Some(self.service),
                method: Some(self.method),
                caller: self.caller,
                node_id: self.node_id,
                is_leader: self.is_leader,
                raft_term: self.raft_term,
                region: self.region,
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

            if self.emit_grpc_metric {
                let status = self.grpc_status_str();
                crate::metrics::record_grpc_request(
                    self.service,
                    self.method,
                    status,
                    self.start_time.elapsed().as_secs_f64(),
                );
            }
        }
    }
}

/// Alias for [`RequestContext`] emphasizing the canonical log line pattern.
///
/// Each request creates one `CanonicalLogLine` that accumulates context throughout
/// its lifecycle and emits a single structured JSON event on completion.
pub type CanonicalLogLine = RequestContext;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use uuid::Uuid;

    use super::*;

    // ── Helper function tests ──

    #[test]
    fn truncate_string_short_unchanged() {
        assert_eq!(truncate_string("hello", 10), "hello");
    }

    #[test]
    fn truncate_string_exact_length() {
        assert_eq!(truncate_string("hello", 5), "hello");
    }

    #[test]
    fn truncate_string_long_truncated() {
        let result = truncate_string("hello world", 8);
        assert_eq!(result, "hello...");
    }

    #[test]
    fn truncate_string_very_short_max() {
        // max_len < 3 triggers saturating_sub
        let result = truncate_string("abcdef", 2);
        assert_eq!(result, "...");
    }

    #[test]
    fn sanitize_string_replaces_control_chars() {
        let input = "hello\x00world\x01test";
        let result = sanitize_string(input);
        assert_eq!(result, "hello\u{FFFD}world\u{FFFD}test");
    }

    #[test]
    fn sanitize_string_preserves_newline_cr_tab() {
        let input = "line1\nline2\rline3\ttab";
        let result = sanitize_string(input);
        assert_eq!(result, input);
    }

    #[test]
    fn sanitize_string_normal_text_unchanged() {
        let input = "normal text 123";
        assert_eq!(sanitize_string(input), input);
    }

    #[test]
    fn truncate_hash_short_unchanged() {
        assert_eq!(truncate_hash("abcdef"), "abcdef");
    }

    #[test]
    fn truncate_hash_exact_16() {
        let hash = "0123456789abcdef";
        assert_eq!(truncate_hash(hash), hash);
    }

    #[test]
    fn truncate_hash_long_truncated() {
        let hash = "0123456789abcdef0123456789abcdef";
        assert_eq!(truncate_hash(hash), "0123456789abcdef");
    }

    // ── RequestContext construction tests ──

    #[test]
    fn new_sets_service_and_method() {
        let ctx = RequestContext::new("WriteService", "write");
        assert_eq!(ctx.service, "WriteService");
        assert_eq!(ctx.method, "write");
        assert!(!ctx.emitted);
    }

    #[test]
    fn with_request_id_uses_given_id() {
        let id = Uuid::new_v4();
        let ctx = RequestContext::with_request_id("ReadService", "get", id);
        assert_eq!(ctx.request_id(), id);
        assert_eq!(ctx.service, "ReadService");
    }

    // ── Client info setters ──

    #[test]
    fn set_client_info_stores_truncated_sanitized() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_client_info("client\x00id", 42);
        assert_eq!(ctx.client_id.as_deref(), Some("client\u{FFFD}id"));
        assert_eq!(ctx.sequence, Some(42));
    }

    #[test]
    fn set_client_id_only() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_client_id("c1");
        assert_eq!(ctx.client_id.as_deref(), Some("c1"));
        assert!(ctx.sequence.is_none());
    }

    #[test]
    fn set_sequence_only() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_sequence(99);
        assert_eq!(ctx.sequence, Some(99));
        assert!(ctx.client_id.is_none());
    }

    #[test]
    fn set_caller_and_caller_or_zero() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        assert_eq!(ctx.caller_or_zero(), 0);
        ctx.set_caller(12345);
        assert_eq!(ctx.caller_or_zero(), 12345);
    }

    // ── Target setters ──

    #[test]
    fn set_target_sets_both() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_target(1, 2);
        assert_eq!(ctx.organization, Some(1));
        assert_eq!(ctx.vault, Some(2));
    }

    #[test]
    fn set_organization_only() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_organization(10);
        assert_eq!(ctx.organization, Some(10));
        assert!(ctx.vault.is_none());
    }

    #[test]
    fn set_vault_only() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_vault(20);
        assert_eq!(ctx.vault, Some(20));
        assert!(ctx.organization.is_none());
    }

    // ── System context setters ──

    #[test]
    fn set_system_context_all_fields() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_system_context(1, true, 5, Some(Region::GLOBAL));
        assert_eq!(ctx.node_id, Some(1));
        assert_eq!(ctx.is_leader, Some(true));
        assert_eq!(ctx.raft_term, Some(5));
        assert_eq!(ctx.region, Some(Region::GLOBAL));
    }

    #[test]
    fn set_node_id_only() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_node_id(99);
        assert_eq!(ctx.node_id, Some(99));
    }

    #[test]
    fn set_is_leader() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_is_leader(false);
        assert_eq!(ctx.is_leader, Some(false));
    }

    #[test]
    fn set_raft_term() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_raft_term(100);
        assert_eq!(ctx.raft_term, Some(100));
    }

    #[test]
    fn set_region() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_region(Region::DE_CENTRAL_FRANKFURT);
        assert_eq!(ctx.region, Some(Region::DE_CENTRAL_FRANKFURT));
    }

    // ── Write operation setters ──

    #[test]
    fn set_write_operation_fields() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_write_operation(3, vec!["Set", "Delete"], true);
        assert_eq!(ctx.operations_count, Some(3));
        assert_eq!(ctx.operation_types.as_ref().unwrap().len(), 2);
        assert_eq!(ctx.include_tx_proof, Some(true));
    }

    #[test]
    fn set_operations_count() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_operations_count(5);
        assert_eq!(ctx.operations_count, Some(5));
    }

    #[test]
    fn set_operation_types() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_operation_types(vec!["Set"]);
        assert_eq!(ctx.operation_types.as_ref().unwrap(), &["Set"]);
    }

    #[test]
    fn set_include_tx_proof() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_include_tx_proof(false);
        assert_eq!(ctx.include_tx_proof, Some(false));
    }

    #[test]
    fn set_idempotency_hit() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_idempotency_hit(true);
        assert_eq!(ctx.idempotency_hit, Some(true));
    }

    #[test]
    fn set_batch_info() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_batch_info(true, 10);
        assert_eq!(ctx.batch_coalesced, Some(true));
        assert_eq!(ctx.batch_size, Some(10));
    }

    #[test]
    fn set_condition_type() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_condition_type("MustNotExist");
        assert_eq!(ctx.condition_type.as_deref(), Some("MustNotExist"));
    }

    // ── Read operation setters ──

    #[test]
    fn set_read_operation_fields() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_read_operation("my_key", "strong", true);
        assert_eq!(ctx.key.as_deref(), Some("my_key"));
        assert_eq!(ctx.consistency.as_deref(), Some("strong"));
        assert_eq!(ctx.include_proof, Some(true));
    }

    #[test]
    fn set_key_truncates() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        let long_key = "x".repeat(300);
        ctx.set_key(&long_key);
        assert!(ctx.key.as_ref().unwrap().len() <= 256);
    }

    #[test]
    fn set_keys_count() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_keys_count(50);
        assert_eq!(ctx.keys_count, Some(50));
    }

    #[test]
    fn set_found_count() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_found_count(25);
        assert_eq!(ctx.found_count, Some(25));
    }

    #[test]
    fn set_consistency() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_consistency("eventual");
        assert_eq!(ctx.consistency.as_deref(), Some("eventual"));
    }

    #[test]
    fn set_at_height() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_at_height(42);
        assert_eq!(ctx.at_height, Some(42));
    }

    #[test]
    fn set_include_proof() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_include_proof(true);
        assert_eq!(ctx.include_proof, Some(true));
    }

    #[test]
    fn set_proof_size_bytes() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_proof_size_bytes(1024);
        assert_eq!(ctx.proof_size_bytes, Some(1024));
    }

    #[test]
    fn set_found() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_found(false);
        assert_eq!(ctx.found, Some(false));
    }

    #[test]
    fn set_value_size_bytes() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_value_size_bytes(512);
        assert_eq!(ctx.value_size_bytes, Some(512));
    }

    // ── Admin operation setters ──

    #[test]
    fn set_admin_action() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_admin_action("create_organization");
        assert_eq!(ctx.admin_action, Some("create_organization"));
    }

    #[test]
    fn set_retention_mode() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_retention_mode("full");
        assert_eq!(ctx.retention_mode.as_deref(), Some("full"));
    }

    #[test]
    fn set_recovery_force() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_recovery_force(true);
        assert_eq!(ctx.recovery_force, Some(true));
    }

    #[test]
    fn set_admin_operation() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_admin_operation("delete_vault");
        assert_eq!(ctx.admin_action, Some("delete_vault"));
    }

    // ── Outcome setters ──

    #[test]
    fn set_outcome_directly() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_outcome(Outcome::Success);
        assert_eq!(ctx.outcome.as_ref().unwrap().as_str(), "success");
    }

    #[test]
    fn set_success() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_success();
        assert!(matches!(ctx.outcome, Some(Outcome::Success)));
    }

    #[test]
    fn set_error() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_error("NOT_FOUND", "entity missing");
        match &ctx.outcome {
            Some(Outcome::Error { code, message }) => {
                assert_eq!(code, "NOT_FOUND");
                assert_eq!(message, "entity missing");
            },
            _ => panic!("expected Error outcome"),
        }
    }

    #[test]
    fn set_cached() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_cached();
        assert!(matches!(ctx.outcome, Some(Outcome::Cached)));
    }

    #[test]
    fn set_rate_limited() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_rate_limited();
        assert!(matches!(ctx.outcome, Some(Outcome::RateLimited)));
    }

    #[test]
    fn set_precondition_failed_with_key() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_precondition_failed(Some("my_key"));
        match &ctx.outcome {
            Some(Outcome::PreconditionFailed { key }) => {
                assert_eq!(key.as_deref(), Some("my_key"));
            },
            _ => panic!("expected PreconditionFailed outcome"),
        }
    }

    #[test]
    fn set_precondition_failed_without_key() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_precondition_failed(None);
        match &ctx.outcome {
            Some(Outcome::PreconditionFailed { key }) => {
                assert!(key.is_none());
            },
            _ => panic!("expected PreconditionFailed outcome"),
        }
    }

    #[test]
    fn set_block_height() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_block_height(500);
        assert_eq!(ctx.block_height, Some(500));
    }

    #[test]
    fn set_block_hash_truncates() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_block_hash("0123456789abcdef0123456789abcdef");
        assert_eq!(ctx.block_hash.as_deref(), Some("0123456789abcdef"));
    }

    #[test]
    fn set_state_root_truncates() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_state_root("fedcba9876543210fedcba9876543210");
        assert_eq!(ctx.state_root.as_deref(), Some("fedcba9876543210"));
    }

    // ── I/O metrics setters ──

    #[test]
    fn set_bytes_read() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_bytes_read(1024);
        assert_eq!(ctx.bytes_read, Some(1024));
    }

    #[test]
    fn set_bytes_written() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_bytes_written(2048);
        assert_eq!(ctx.bytes_written, Some(2048));
    }

    #[test]
    fn increment_raft_round_trips() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.increment_raft_round_trips();
        ctx.increment_raft_round_trips();
        assert_eq!(ctx.raft_round_trips, Some(2));
    }

    #[test]
    fn set_raft_round_trips() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_raft_round_trips(5);
        assert_eq!(ctx.raft_round_trips, Some(5));
    }

    // ── Transport metadata setters ──

    #[test]
    fn set_sdk_version() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_sdk_version("1.2.3");
        assert_eq!(ctx.sdk_version.as_deref(), Some("1.2.3"));
    }

    #[test]
    fn set_source_ip() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_source_ip("192.168.1.1");
        assert_eq!(ctx.source_ip.as_deref(), Some("192.168.1.1"));
    }

    #[test]
    fn extract_transport_metadata_from_grpc() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("x-sdk-version", "2.0.0".parse().unwrap());
        metadata.insert("x-forwarded-for", "10.0.0.1, 10.0.0.2".parse().unwrap());
        ctx.extract_transport_metadata(&metadata);
        assert_eq!(ctx.sdk_version.as_deref(), Some("2.0.0"));
        assert_eq!(ctx.source_ip.as_deref(), Some("10.0.0.1"));
    }

    #[test]
    fn extract_transport_metadata_missing_headers() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        let metadata = tonic::metadata::MetadataMap::new();
        ctx.extract_transport_metadata(&metadata);
        assert!(ctx.sdk_version.is_none());
        assert!(ctx.source_ip.is_none());
    }

    // ── Trace context setters ──

    #[test]
    fn set_trace_context_all_fields() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_trace_context(
            "0123456789abcdef0123456789abcdef",
            "abcdef0123456789",
            Some("parent123"),
            1,
        );
        assert_eq!(ctx.trace_id.as_deref(), Some("0123456789abcdef0123456789abcdef"));
        assert_eq!(ctx.span_id.as_deref(), Some("abcdef0123456789"));
        assert_eq!(ctx.parent_span_id.as_deref(), Some("parent123"));
        assert_eq!(ctx.trace_flags, Some(1));
    }

    #[test]
    fn set_trace_id() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_trace_id("traceid".to_string());
        assert_eq!(ctx.trace_id.as_deref(), Some("traceid"));
    }

    #[test]
    fn set_span_id() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_span_id("spanid".to_string());
        assert_eq!(ctx.span_id.as_deref(), Some("spanid"));
    }

    #[test]
    fn set_parent_span_id() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_parent_span_id("parentid".to_string());
        assert_eq!(ctx.parent_span_id.as_deref(), Some("parentid"));
    }

    #[test]
    fn set_trace_flags() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_trace_flags(0x01);
        assert_eq!(ctx.trace_flags, Some(1));
    }

    // ── Timing tests ──

    #[test]
    fn timing_raft_start_end() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.start_raft_timer();
        assert!(ctx.timing.raft_start.is_some());
        ctx.end_raft_timer();
        assert!(ctx.timing.raft_start.is_none());
        assert!(ctx.timing.raft_latency_ms.is_some());
        assert_eq!(ctx.raft_round_trips, Some(1));
    }

    #[test]
    fn end_raft_timer_without_start_is_noop() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.end_raft_timer();
        assert!(ctx.timing.raft_latency_ms.is_none());
    }

    #[test]
    fn timing_storage_start_end() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.start_storage_timer();
        assert!(ctx.timing.storage_start.is_some());
        ctx.end_storage_timer();
        assert!(ctx.timing.storage_start.is_none());
        assert!(ctx.timing.storage_latency_ms.is_some());
    }

    #[test]
    fn end_storage_timer_without_start_is_noop() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.end_storage_timer();
        assert!(ctx.timing.storage_latency_ms.is_none());
    }

    // ── Emission control ──

    #[test]
    fn suppress_emission_prevents_emit() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        assert!(ctx.emitted);
    }

    #[test]
    fn set_operation_type() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_operation_type(OperationType::Read);
        assert_eq!(ctx.operation_type, OperationType::Read);
    }

    #[test]
    fn elapsed_secs_positive() {
        let ctx = RequestContext::new("S", "m");
        assert!(ctx.elapsed_secs() >= 0.0);
        // suppress emission before drop
        let mut ctx = ctx;
        ctx.suppress_emission();
    }

    #[test]
    fn emit_then_drop_does_not_double_emit() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.set_success();
        ctx.emit();
        assert!(ctx.emitted);
        // second emit is a no-op
        ctx.emit();
    }

    // ── Task-local context ──

    #[test]
    fn try_with_current_context_returns_false_without_scope() {
        assert!(!try_with_current_context(|_ctx| {}));
    }

    #[test]
    fn with_current_context_returns_none_without_scope() {
        let result: Option<()> = with_current_context(|_ctx| {});
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn guard_run_with_sets_context() {
        let ctx = RequestContext::new("TestService", "test");
        let id = ctx.request_id();
        RequestContextGuard::run_with(ctx, async {
            let found_id = with_current_context(|ctx| ctx.request_id());
            assert_eq!(found_id, Some(id));
        })
        .await;
    }

    #[test]
    fn guard_run_with_sync_sets_context() {
        let ctx = RequestContext::new("SyncService", "sync");
        let id = ctx.request_id();
        RequestContextGuard::run_with_sync(ctx, || {
            let found_id = with_current_context(|ctx| ctx.request_id());
            assert_eq!(found_id, Some(id));
        });
    }

    // ── gRPC metric emission ──

    #[test]
    fn grpc_context_emits_metric_on_drop() {
        // Verify that a context with emit_grpc_metric = true calls
        // record_grpc_request without panicking. The metrics crate is safe
        // to call without a recorder installed — calls become no-ops.
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.emit_grpc_metric = true;
        ctx.set_success();
        ctx.set_region(Region::GLOBAL);
        drop(ctx); // should not panic
    }

    #[test]
    fn non_grpc_context_skips_metric_emission() {
        // Default context has emit_grpc_metric = false.
        let mut ctx = RequestContext::new("BackgroundJob", "compact");
        ctx.set_success();
        assert!(!ctx.emit_grpc_metric);
        drop(ctx); // should not panic, and should not attempt metric emission
    }

    #[test]
    fn grpc_status_str_maps_outcomes() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();

        // No outcome -> UNKNOWN
        assert_eq!(ctx.grpc_status_str(), "UNKNOWN");

        ctx.set_success();
        assert_eq!(ctx.grpc_status_str(), "OK");

        ctx.set_cached();
        assert_eq!(ctx.grpc_status_str(), "OK");

        ctx.set_rate_limited();
        assert_eq!(ctx.grpc_status_str(), "RESOURCE_EXHAUSTED");

        ctx.set_precondition_failed(Some("key"));
        assert_eq!(ctx.grpc_status_str(), "FAILED_PRECONDITION");

        ctx.set_error("NOT_FOUND", "not found");
        assert_eq!(ctx.grpc_status_str(), "NOT_FOUND");
    }

    // ── Event handle ──

    #[test]
    fn record_event_noop_when_no_event_handle() {
        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.suppress_emission();
        ctx.set_organization(100);
        ctx.set_caller(42);

        // Should not panic even without an event handle
        ctx.record_event(
            inferadb_ledger_types::events::EventAction::RequestRateLimited,
            inferadb_ledger_types::events::EventOutcome::Denied {
                reason: "rate_limited".to_string(),
            },
            &[("level", "organization")],
        );
    }

    #[test]
    fn record_event_reads_scope_from_self() {
        use std::sync::{Arc, Mutex};

        use inferadb_ledger_types::events::{EventAction, EventEntry, EventOutcome};

        /// Test event emitter that captures the entry for assertion.
        struct TestEmitter {
            captured: Mutex<Option<EventEntry>>,
        }

        impl crate::event_writer::EventEmitter for TestEmitter {
            fn record_event(&self, entry: EventEntry) {
                *self.captured.lock().unwrap() = Some(entry);
            }
        }

        let emitter = Arc::new(TestEmitter { captured: Mutex::new(None) });

        let mut ctx = RequestContext::new("WriteService", "write");
        ctx.suppress_emission();
        ctx.set_organization(100);
        ctx.set_vault(200);
        ctx.set_caller(42);
        ctx.set_trace_id("abc123".to_string());
        ctx.set_node_id(7);
        ctx.event_handle = Some(emitter.clone());

        ctx.record_event(
            EventAction::RequestRateLimited,
            EventOutcome::Denied { reason: "rate_limited".to_string() },
            &[("level", "organization")],
        );

        let entry = emitter.captured.lock().unwrap().take().unwrap();
        assert_eq!(entry.organization_id, inferadb_ledger_types::OrganizationId::new(100));
        assert_eq!(entry.organization, Some(inferadb_ledger_types::OrganizationSlug::new(100)));
        assert_eq!(entry.vault, Some(inferadb_ledger_types::VaultSlug::new(200)));
        assert_eq!(entry.principal, "user:42");
        assert_eq!(entry.trace_id.as_deref(), Some("abc123"));
        assert_eq!(entry.details.get("level").map(String::as_str), Some("organization"));
        assert_eq!(entry.action, EventAction::RequestRateLimited);
    }

    // ── from_request constructor ──

    #[test]
    fn from_request_extracts_metadata() {
        use tonic::metadata::MetadataValue;

        let mut request = tonic::Request::new(());
        request.metadata_mut().insert("x-sdk-version", MetadataValue::from_static("3.0.0"));
        request.metadata_mut().insert(
            "traceparent",
            MetadataValue::from_static("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
        );

        let mut ctx = RequestContext::from_request("ReadService", "get", &request, None);
        assert_eq!(ctx.service, "ReadService");
        assert_eq!(ctx.method, "get");
        assert!(ctx.emit_grpc_metric);
        assert!(ctx.event_handle.is_none());
        assert_eq!(ctx.sdk_version.as_deref(), Some("3.0.0"));
        assert_eq!(ctx.trace_id.as_deref(), Some("0af7651916cd43dd8448eb211c80319c"),);
        assert!(ctx.span_id.is_some());
        assert_eq!(ctx.parent_span_id.as_deref(), Some("b7ad6b7169203331"),);
        assert_eq!(ctx.trace_flags, Some(0x01));

        ctx.suppress_emission();
    }

    #[test]
    fn set_trace_context_from_copies_fields() {
        let trace_ctx = crate::trace_context::TraceContext {
            trace_id: "aaaa".repeat(8),
            span_id: "bbbb".repeat(4),
            parent_span_id: Some("cccc".repeat(4)),
            trace_flags: 0x01,
            trace_state: None,
        };

        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();
        ctx.set_trace_context_from(&trace_ctx);

        assert_eq!(ctx.trace_id.as_deref(), Some(trace_ctx.trace_id.as_str()));
        assert_eq!(ctx.span_id.as_deref(), Some(trace_ctx.span_id.as_str()));
        assert_eq!(ctx.parent_span_id, trace_ctx.parent_span_id);
        assert_eq!(ctx.trace_flags, Some(0x01));
    }

    // ── metric_labels() partition enforcement ──

    #[test]
    fn metric_labels_returns_service_method_status() {
        let mut ctx = RequestContext::new("TestService", "test_method");
        ctx.suppress_emission();
        ctx.set_success();

        let labels = ctx.metric_labels();

        assert_eq!(labels.len(), 3);

        let service_label = labels.iter().find(|(k, _)| *k == crate::logging::fields::SERVICE);
        let method_label = labels.iter().find(|(k, _)| *k == crate::logging::fields::METHOD);
        let status_label = labels.iter().find(|(k, _)| *k == crate::logging::fields::STATUS);

        assert_eq!(service_label.map(|(_, v)| v.as_str()), Some("TestService"));
        assert_eq!(method_label.map(|(_, v)| v.as_str()), Some("test_method"));
        assert_eq!(status_label.map(|(_, v)| v.as_str()), Some("OK"));
    }

    #[test]
    fn metric_labels_never_contain_entity_ids_or_region() {
        let mut ctx = RequestContext::new("TestService", "test_method");
        ctx.suppress_emission();
        ctx.set_target(12345, 67890);
        ctx.set_caller(99999);
        ctx.set_region(Region::US_EAST_VA);
        ctx.set_success();

        let labels = ctx.metric_labels();
        let label_keys: Vec<&str> = labels.iter().map(|(k, _)| *k).collect();

        // High-cardinality entity IDs must never appear as metric labels.
        assert!(
            !label_keys.contains(&crate::logging::fields::ORGANIZATION),
            "organization (slug) must not be a metric label"
        );
        assert!(
            !label_keys.contains(&crate::logging::fields::VAULT),
            "vault (slug) must not be a metric label"
        );
        assert!(
            !label_keys.contains(&crate::logging::fields::CALLER),
            "caller must not be a metric label"
        );
        // Region is scraper-implicit — the metrics infrastructure adds it;
        // application code must not duplicate it.
        assert!(
            !label_keys.contains(&crate::logging::fields::REGION),
            "region must not be a metric label"
        );
        // Internal sequential IDs are also excluded.
        assert!(
            !label_keys.contains(&crate::logging::fields::ORGANIZATION_ID),
            "organization_id must not be a metric label"
        );
        assert!(
            !label_keys.contains(&crate::logging::fields::VAULT_ID),
            "vault_id must not be a metric label"
        );

        // Bounded-enum fields MUST appear.
        assert!(
            label_keys.contains(&crate::logging::fields::SERVICE),
            "service must be a metric label"
        );
        assert!(
            label_keys.contains(&crate::logging::fields::METHOD),
            "method must be a metric label"
        );
        assert!(
            label_keys.contains(&crate::logging::fields::STATUS),
            "status must be a metric label"
        );
    }

    #[test]
    fn metric_labels_status_reflects_outcome() {
        let mut ctx = RequestContext::new("S", "m");
        ctx.suppress_emission();

        // No outcome set -> UNKNOWN
        let labels = ctx.metric_labels();
        let status = labels
            .iter()
            .find(|(k, _)| *k == crate::logging::fields::STATUS)
            .map(|(_, v)| v.as_str());
        assert_eq!(status, Some("UNKNOWN"));

        ctx.set_rate_limited();
        let labels = ctx.metric_labels();
        let status = labels
            .iter()
            .find(|(k, _)| *k == crate::logging::fields::STATUS)
            .map(|(_, v)| v.as_str());
        assert_eq!(status, Some("RESOURCE_EXHAUSTED"));

        ctx.set_error("NOT_FOUND", "missing");
        let labels = ctx.metric_labels();
        let status = labels
            .iter()
            .find(|(k, _)| *k == crate::logging::fields::STATUS)
            .map(|(_, v)| v.as_str());
        assert_eq!(status, Some("NOT_FOUND"));
    }
}
