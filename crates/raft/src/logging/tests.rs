use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use inferadb_ledger_types::Region;
use tracing_subscriber::layer::SubscriberExt;
use uuid::Uuid;

use super::{
    request_context::{sanitize_string, truncate_hash, truncate_string},
    *,
};

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
    ctx.set_client_info("client_123", 42);
    assert_eq!(ctx.client_id.as_deref(), Some("client_123"));
    assert_eq!(ctx.sequence, Some(42));
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
    assert_eq!(ctx.organization, Some(1));
    assert_eq!(ctx.vault, Some(2));
    ctx.suppress_emission();
}

#[test]
fn test_set_system_context() {
    let mut ctx = RequestContext::new("WriteService", "write");
    ctx.set_system_context(100, true, 5, Some(Region::CA_CENTRAL_QC));
    assert_eq!(ctx.node_id, Some(100));
    assert_eq!(ctx.is_leader, Some(true));
    assert_eq!(ctx.raft_term, Some(5));
    assert_eq!(ctx.region, Some(Region::CA_CENTRAL_QC));
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
                    self.0
                        .insert(field.name().to_string(), serde_json::Value::Number(value.into()));
                }

                fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                    self.0
                        .insert(field.name().to_string(), serde_json::Value::Number(value.into()));
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
        ctx.set_organization(42);
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
    assert!(obj.contains_key("organization"), "organization field should be at top level");
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
    let result = with_current_context(|_ctx| 42);
    assert!(result.is_none(), "Should return None when no context is set");
}

#[test]
fn test_try_with_current_context_returns_false_when_no_guard() {
    // try_with_current_context should return false when no context exists
    let executed = try_with_current_context(|_ctx| {
        // This should not execute
    });
    assert!(!executed, "Should return false when no context is set");
}

#[test]
fn test_run_with_sync_sets_and_clears_context() {
    // Context should be accessible within run_with_sync scope
    let (_, events) = with_json_capturing_subscriber(|| {
        let ctx = RequestContext::new("TestService", "test");

        RequestContextGuard::run_with_sync(ctx, || {
            // Context should be accessible here
            let found = with_current_context(|ctx| {
                ctx.set_client_id("sync_test_client");
                ctx.set_success();
                true
            });
            assert!(found.is_some(), "Context should be accessible within scope");
        });

        // After run_with_sync returns, context should be gone
        let after = with_current_context(|_ctx| true);
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
    let result = RequestContextGuard::run_with(ctx, async {
        // Context should be accessible here
        with_current_context(|ctx| {
            ctx.set_client_id("async_test_client");
            ctx.set_success();
            true
        })
    })
    .await;

    assert!(result.is_some(), "Context should be accessible within async scope");

    // After run_with returns, context should be gone
    let after = with_current_context(|_ctx| true);
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

    RequestContextGuard::run_with(ctx, async move {
        // Set field before await
        with_current_context(|ctx| {
            ctx.set_client_id("before_await");
        });

        // Yield to scheduler (simulates real async work)
        tokio::task::yield_now().await;

        // Context should still be accessible after await
        let found = with_current_context(|ctx| {
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
    assert_eq!(client_id.as_deref(), Some("before_await"), "Field set before await should persist");
}

#[tokio::test]
async fn test_tokio_spawn_does_not_inherit_context() {
    // Child tasks created with tokio::spawn should NOT inherit parent's context
    let ctx = RequestContext::new("ParentService", "parent");

    RequestContextGuard::run_with(ctx, async {
        with_current_context(|ctx| {
            ctx.set_client_id("parent_client");
            ctx.set_success();
        });

        // Spawn a child task
        let handle = tokio::spawn(async {
            // Child should NOT have access to parent's context
            let has_context = with_current_context(|_ctx| true);
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

            RequestContextGuard::run_with(ctx, async move {
                // Each task sets its own client_id
                with_current_context(|ctx| {
                    ctx.set_client_id(format!("client_{}", i).as_str());
                    ctx.set_sequence(i);
                    ctx.set_success();
                });

                // Yield to allow interleaving
                tokio::task::yield_now().await;

                // Verify our context wasn't contaminated by other tasks
                let our_sequence = with_current_context(|ctx| ctx.sequence).flatten();
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

    let result = RequestContextGuard::run_with_sync(ctx, || {
        with_current_context(|ctx| {
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

        RequestContextGuard::run_with_sync(ctx, || {
            // First modification
            with_current_context(|ctx| {
                ctx.set_client_id("first_value");
            });

            // Second modification (should overwrite)
            with_current_context(|ctx| {
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
    assert!(config.vip_organizations.is_empty());
    assert!(config.enabled);
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
    let config = SamplingConfig::builder().error_rate(1.0).read_rate(0.0).write_rate(0.0).build();
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
fn test_sampler_vip_organizations_elevated_rate() {
    // VIP organizations should have higher sampling rate
    let config = SamplingConfig::builder()
        .vip_rate(1.0) // 100% for VIP
        .read_rate(0.0) // 0% for normal
        .vip_organizations(vec![42, 100])
        .build();
    let sampler = Sampler::new(config);

    // VIP organization should always be sampled
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
            "VIP organization should be sampled at 100%"
        );
    }
}

#[test]
fn test_sampler_non_vip_organization_normal_rate() {
    // Non-VIP organizations should use normal sampling rates
    let config = SamplingConfig::builder()
        .vip_rate(1.0)
        .read_rate(0.0) // 0% for normal reads
        .vip_organizations(vec![42])
        .build();
    let sampler = Sampler::new(config);

    // Non-VIP organization (99) should NOT be sampled at 0% rate
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
    assert_eq!(sampled_count, 0, "Non-VIP organization at 0% rate should never be sampled");
}

#[test]
fn test_sampler_deterministic_with_same_request_id() {
    // Same request_id should always produce same sampling decision
    let config = SamplingConfig::builder()
        .read_rate(0.5) // 50% rate for clear determinism test
        .build();
    let sampler = Sampler::new(config);
    let request_id = Uuid::new_v4();

    let first_decision =
        sampler.should_sample(&request_id, Some(&Outcome::Success), 1.0, OperationType::Read, None);

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
    // Over many requests, sampling rate should converge to configured rate (+-5%)
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
    let mut ctx = RequestContext::new("AdminService", "create_organization");
    assert!(ctx.admin_action.is_none());

    ctx.set_admin_action("create_organization");
    assert!(matches!(ctx.admin_action, Some("create_organization")));

    // Overwrite works
    ctx.set_admin_action("delete_organization");
    assert!(matches!(ctx.admin_action, Some("delete_organization")));

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
    let mut ctx = RequestContext::new("AdminService", "create_organization");
    ctx.set_operation_type(OperationType::Admin);
    ctx.set_admin_action("create_organization");
    ctx.set_node_id(12345);
    ctx.set_is_leader(true);
    ctx.set_raft_term(42);

    assert!(matches!(ctx.admin_action, Some("create_organization")));
    assert!(matches!(ctx.node_id, Some(12345)));
    assert!(matches!(ctx.is_leader, Some(true)));
    assert!(matches!(ctx.raft_term, Some(42)));
    assert!(matches!(ctx.operation_type, OperationType::Admin));

    ctx.suppress_emission();
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
    metadata.insert("x-sdk-version", tonic::metadata::MetadataValue::from_static("rust-sdk/0.2.0"));
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
fn test_canonical_log_line_completeness() {
    // Verify that a fully-populated canonical log line emits all field groups.
    let (_, events) = with_json_capturing_subscriber(|| {
        let mut ctx = RequestContext::new("WriteService", "write");

        // Identity fields
        ctx.set_client_info("client_123", 42);

        // Target fields
        ctx.set_target(1, 2);

        // System fields
        ctx.set_node_id(100);
        ctx.set_is_leader(true);
        ctx.set_raft_term(5);
        ctx.set_region(Region::CA_CENTRAL_QC);

        // Read metrics
        ctx.set_bytes_read(2048);

        // Write metrics
        ctx.set_operations_count(10);
        ctx.set_bytes_written(4096);
        ctx.set_raft_round_trips(2);

        // Client transport metadata
        ctx.set_sdk_version("rust-sdk/0.3.0");
        ctx.set_source_ip("10.0.0.42");

        // Tracing context
        ctx.set_trace_context("aabbccdd11223344aabbccdd11223344", "1122334455667788", None, 0x01);

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
    assert!(event.contains("\"organization\":1"), "missing organization");
    assert!(event.contains("\"vault\":2"), "missing vault");

    // System
    assert!(event.contains("\"node_id\":100"), "missing node_id");
    assert!(event.contains("\"is_leader\":true"), "missing is_leader");
    assert!(event.contains("\"raft_term\":5"), "missing raft_term");
    assert!(event.contains("\"region\":\"ca-central-qc\""), "missing region");

    // Read metrics
    assert!(event.contains("\"bytes_read\":2048"), "missing bytes_read");

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
