//! Wide events performance benchmarks.
//!
//! These benchmarks validate that wide events meet the performance targets
//! specified in the PRD:
//!
//! - Context creation: <1μs
//! - Full field population: <10μs
//! - Sampling decision: <1μs
//! - Total overhead: <100μs p99

#![allow(clippy::expect_used, missing_docs)]

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use inferadb_ledger_raft::wide_events::{Outcome, RequestContext, Sampler, SamplingConfig};

/// Benchmark RequestContext::new() creation time.
///
/// Target: <1μs
fn bench_context_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("wide_events/context_creation");
    group.throughput(Throughput::Elements(1));

    group.bench_function("new", |b| {
        b.iter(|| {
            let ctx = RequestContext::new("WriteService", "write");
            black_box(ctx)
        });
    });

    group.finish();
}

/// Benchmark populating all wide event fields.
///
/// Target: <10μs
fn bench_field_population(c: &mut Criterion) {
    let mut group = c.benchmark_group("wide_events/field_population");
    group.throughput(Throughput::Elements(1));

    // Benchmark populating a few critical fields
    group.bench_function("minimal_fields", |b| {
        b.iter(|| {
            let mut ctx = RequestContext::new("WriteService", "write");
            ctx.set_client_info("client_123", 42, Some("user@example.com".to_string()));
            ctx.set_target(1, 2);
            ctx.set_outcome(Outcome::Success);
            ctx.suppress_emission(); // Prevent event emission in benchmark
            black_box(ctx)
        });
    });

    // Benchmark populating all 50+ fields
    group.bench_function("all_fields", |b| {
        b.iter(|| {
            let mut ctx = RequestContext::new("WriteService", "write");

            // Request metadata
            ctx.set_client_info("client_123", 42, Some("user@example.com".to_string()));
            ctx.set_target(1, 2);

            // System context
            ctx.set_system_context(1, true, 5, Some(0));

            // Write operation fields
            ctx.set_write_operation(
                5,
                vec!["set_entity", "create_relationship", "delete_entity"],
                true,
            );
            ctx.set_batch_info(true, 10);
            ctx.set_condition_type("version_match");

            // Read operation fields (for completeness - normally wouldn't set both)
            ctx.set_read_operation("user:123", "linearizable", false);
            ctx.set_value_size_bytes(100);
            ctx.set_at_height(1000);
            ctx.set_found_count(5);
            ctx.set_proof_size_bytes(256);

            // Outcome fields
            ctx.set_outcome(Outcome::Success);
            ctx.set_block_height(100);
            ctx.set_block_hash("abcd1234efgh5678");
            ctx.set_state_root("1234abcd5678efgh");

            // Timing
            ctx.start_raft_timer();
            ctx.end_raft_timer();
            ctx.start_storage_timer();
            ctx.end_storage_timer();

            ctx.suppress_emission(); // Prevent event emission in benchmark
            black_box(ctx)
        });
    });

    group.finish();
}

/// Benchmark sampling decision overhead.
///
/// Target: <1μs
fn bench_sampling_decision(c: &mut Criterion) {
    let mut group = c.benchmark_group("wide_events/sampling");
    group.throughput(Throughput::Elements(1));

    let config = SamplingConfig::builder()
        .error_rate(1.0)
        .slow_rate(1.0)
        .vip_rate(0.5)
        .write_rate(0.1)
        .read_rate(0.01)
        .build();
    let sampler = Sampler::new(config);

    // Generate a stable request_id for benchmarking
    let request_id = uuid::Uuid::new_v4();

    group.bench_function("should_sample_success", |b| {
        b.iter(|| {
            let decision = sampler.should_sample(
                &request_id,
                Some(&Outcome::Success),
                50.0, // duration_ms
                inferadb_ledger_raft::wide_events::OperationType::Write,
                Some(1), // organization_id
            );
            black_box(decision)
        });
    });

    group.bench_function("should_sample_error", |b| {
        let error_outcome =
            Outcome::Error { code: "INTERNAL".to_string(), message: "test".to_string() };
        b.iter(|| {
            let decision = sampler.should_sample(
                &request_id,
                Some(&error_outcome),
                50.0,
                inferadb_ledger_raft::wide_events::OperationType::Write,
                Some(1),
            );
            black_box(decision)
        });
    });

    group.finish();
}

/// Benchmark end-to-end overhead: context creation through field population.
///
/// Target: <100μs p99 total overhead
fn bench_end_to_end(c: &mut Criterion) {
    let mut group = c.benchmark_group("wide_events/end_to_end");
    group.throughput(Throughput::Elements(1));

    let config = SamplingConfig::builder().build();
    let sampler = Sampler::new(config);

    // Benchmark the complete wide events overhead for a typical write request
    group.bench_function("write_request", |b| {
        b.iter(|| {
            // 1. Create context
            let mut ctx = RequestContext::new("WriteService", "write");

            // 2. Set sampler
            ctx.set_sampler(sampler.clone());

            // 3. Populate typical write fields
            ctx.set_client_info("client_123", 42, Some("user@example.com".to_string()));
            ctx.set_target(1, 2);
            ctx.set_system_context(1, true, 5, Some(0));
            ctx.set_write_operation(3, vec!["set_entity"], false);

            // 4. Record timing
            ctx.start_raft_timer();
            // (simulated Raft operation would happen here)
            ctx.end_raft_timer();

            ctx.start_storage_timer();
            // (simulated storage operation would happen here)
            ctx.end_storage_timer();

            // 5. Set outcome
            ctx.set_outcome(Outcome::Success);
            ctx.set_block_height(100);
            ctx.set_block_hash("abcd1234efgh5678");
            ctx.set_state_root("1234abcd5678efgh");

            // 6. Suppress emission for benchmark (context would emit on drop)
            ctx.suppress_emission();
            black_box(ctx)
        });
    });

    // Benchmark for a typical read request (simpler)
    group.bench_function("read_request", |b| {
        b.iter(|| {
            let mut ctx = RequestContext::new("ReadService", "read");
            ctx.set_sampler(sampler.clone());
            ctx.set_client_info("client_123", 42, Some("user@example.com".to_string()));
            ctx.set_target(1, 2);
            ctx.set_system_context(1, false, 5, Some(0));
            ctx.set_read_operation("user:123", "linearizable", false);

            ctx.start_storage_timer();
            ctx.end_storage_timer();

            ctx.set_outcome(Outcome::Success);

            ctx.suppress_emission();
            black_box(ctx)
        });
    });

    group.finish();
}

/// Benchmark with different numbers of operation types.
///
/// Validates that operation_types Vec doesn't cause significant overhead.
fn bench_operation_types_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("wide_events/operation_types");

    for count in [1, 5, 10, 20] {
        group.bench_with_input(BenchmarkId::new("count", count), &count, |b, &count| {
            b.iter(|| {
                let mut ctx = RequestContext::new("WriteService", "batch_write");
                let op_types: Vec<&'static str> = (0..count)
                    .map(|i| match i % 4 {
                        0 => "set_entity",
                        1 => "create_relationship",
                        2 => "delete_entity",
                        _ => "delete_relationship",
                    })
                    .collect();
                ctx.set_operation_types(op_types);
                ctx.suppress_emission();
                black_box(ctx)
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_context_creation,
    bench_field_population,
    bench_sampling_decision,
    bench_end_to_end,
    bench_operation_types_scaling,
);

criterion_main!(benches);
