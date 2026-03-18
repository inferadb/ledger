//! Request context management for canonical log line emission.
//!
//! The core abstraction is [`RequestContext`] (aliased as [`CanonicalLogLine`]),
//! a builder that accumulates fields throughout the request lifecycle. On drop,
//! it emits a single structured event to the `ledger::events` tracing target.

use std::{cell::RefCell, time::Instant};

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
#[derive(Debug)]
pub struct RequestContext {
    // Request metadata
    pub(crate) request_id: Uuid,
    pub(crate) service: &'static str,
    pub(crate) method: &'static str,
    pub(crate) start_time: Instant,

    // Client info
    pub(crate) client_id: Option<String>,
    pub(crate) sequence: Option<u64>,
    pub(crate) actor: Option<String>,

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
    /// * `actor` - Identity performing the operation. Must be an opaque identifier (slug, numeric
    ///   ID), never an email or display name. Canonical log lines may be shipped to external
    ///   aggregators without data residency controls.
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
    ///
    /// Must be an opaque identifier (slug, numeric ID), never an email or
    /// display name. Canonical log lines may be shipped to external aggregators
    /// without data residency controls.
    pub fn set_actor(&mut self, actor: &str) {
        self.actor = Some(sanitize_string(actor));
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
            actor = self.actor.as_deref(),
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
        if crate::otel::is_otel_enabled() {
            let attrs = crate::otel::SpanAttributes {
                request_id: Some(self.request_id.to_string()),
                client_id: self.client_id.clone(),
                sequence: self.sequence,
                organization: self.organization,
                vault: self.vault,
                service: Some(self.service),
                method: Some(self.method),
                actor: self.actor.clone(),
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
        }
    }
}

/// Alias for [`RequestContext`] emphasizing the canonical log line pattern.
///
/// Each request creates one `CanonicalLogLine` that accumulates context throughout
/// its lifecycle and emits a single structured JSON event on completion.
pub type CanonicalLogLine = RequestContext;
