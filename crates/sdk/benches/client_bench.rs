//! Performance benchmarks for the Ledger SDK.
//!
//! These benchmarks measure:
//! - SDK overhead (serialization, idempotency key generation)
//! - Throughput (ops/sec for writes, reads)
//! - Latency (single operation timing)
//! - Batch amortization (1 vs 10 vs 100 keys)
//!
//! Run with: `cargo bench -p inferadb-ledger-sdk`
//!
//! # Baseline Numbers (M3 MacBook Pro)
//!
//! These are reference numbers; actual results depend on hardware.
//!
//! | Benchmark                    | Baseline     | Notes                          |
//! |------------------------------|--------------|--------------------------------|
//! | read_single_key              | ~500 µs      | Full roundtrip to mock server  |
//! | read_batch_1                 | ~500 µs      | Same as single (baseline)      |
//! | read_batch_10                | ~600 µs      | 10x keys, ~1.2x latency        |
//! | read_batch_100               | ~1.2 ms      | 100x keys, ~2.4x latency       |
//! | write_single_operation       | ~600 µs      | Includes idempotency key gen   |

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::{hint::black_box, time::Duration};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use inferadb_ledger_sdk::{
    ClientConfig, LedgerClient, Operation, OrganizationSlug, RetryPolicy, ServerSource,
    mock::MockLedgerServer,
};
use tokio::runtime::Runtime;

/// Organization slug used across all benchmarks.
const ORG: OrganizationSlug = OrganizationSlug::new(1);

/// Creates a runtime for async benchmarks.
fn create_runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create runtime")
}

/// Creates a client connected to the mock server.
async fn create_client_for_mock(endpoint: &str) -> LedgerClient {
    let config = ClientConfig::builder()
        .servers(ServerSource::from_static([endpoint]))
        .client_id("bench-client")
        .timeout(Duration::from_secs(5))
        .connect_timeout(Duration::from_secs(2))
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(1) // No retry for benchmarks
                .build(),
        )
        .build()
        .expect("Failed to build config");

    LedgerClient::new(config).await.expect("Failed to create client")
}

// ============================================================================
// Read Operation Benchmarks
// ============================================================================

/// Benchmark single key read latency.
///
/// Measures full roundtrip time for reading a single key from the mock server.
fn bench_read_single(c: &mut Criterion) {
    let rt = create_runtime();

    // Setup: start mock server and create client
    let (client, _server) = rt.block_on(async {
        let server = MockLedgerServer::start().await.expect("Failed to start mock server");
        server.set_entity(ORG, 0, "test-key", b"test-value-data");
        let client = create_client_for_mock(server.endpoint()).await;
        (client, server)
    });

    c.bench_function("read_single_key", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(client.read(ORG, Some(0), "test-key").await.expect("read failed"))
        })
    });
}

/// Benchmark batch read with varying batch sizes.
///
/// Measures how latency scales with batch size (amortization).
fn bench_read_batch(c: &mut Criterion) {
    let rt = create_runtime();

    // Setup: start mock server with many keys
    let (client, _server) = rt.block_on(async {
        let server = MockLedgerServer::start().await.expect("Failed to start mock server");

        // Pre-populate 100 keys
        for i in 0..100 {
            server.set_entity(ORG, 0, &format!("key-{:03}", i), format!("value-{}", i).as_bytes());
        }

        let client = create_client_for_mock(server.endpoint()).await;
        (client, server)
    });

    let mut group = c.benchmark_group("read_batch");

    for batch_size in [1, 10, 100] {
        let keys: Vec<String> = (0..batch_size).map(|i| format!("key-{:03}", i)).collect();

        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(batch_size), &keys, |b, keys| {
            b.to_async(&rt).iter(|| async {
                black_box(
                    client.batch_read(ORG, Some(0), keys.clone()).await.expect("batch_read failed"),
                )
            })
        });
    }
    group.finish();
}

// ============================================================================
// Write Operation Benchmarks
// ============================================================================

/// Benchmark single write operation latency.
///
/// Measures full roundtrip time for a single write including idempotency key generation.
fn bench_write_single(c: &mut Criterion) {
    let rt = create_runtime();

    // Setup: start mock server and create client
    let (client, _server) = rt.block_on(async {
        let server = MockLedgerServer::start().await.expect("Failed to start mock server");
        let client = create_client_for_mock(server.endpoint()).await;
        (client, server)
    });

    let mut key_counter = 0u64;

    c.bench_function("write_single_operation", |b| {
        b.to_async(&rt).iter(|| {
            key_counter += 1;
            let key = format!("bench-key-{}", key_counter);
            let ops = vec![Operation::set_entity(&key, b"bench-value".to_vec())];

            async { black_box(client.write(ORG, Some(0), ops).await.expect("write failed")) }
        })
    });
}

/// Benchmark write with multiple operations per request.
///
/// Measures amortization when batching multiple operations in a single write.
fn bench_write_multi_op(c: &mut Criterion) {
    let rt = create_runtime();

    // Setup: start mock server and create client
    let (client, _server) = rt.block_on(async {
        let server = MockLedgerServer::start().await.expect("Failed to start mock server");
        let client = create_client_for_mock(server.endpoint()).await;
        (client, server)
    });

    let mut group = c.benchmark_group("write_multi_op");
    let mut key_counter = 0u64;

    for op_count in [1, 5, 10] {
        group.throughput(Throughput::Elements(op_count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(op_count), &op_count, |b, &count| {
            b.to_async(&rt).iter(|| {
                key_counter += 1;
                let ops: Vec<Operation> = (0..count)
                    .map(|i| {
                        Operation::set_entity(
                            format!("batch-{}-key-{}", key_counter, i),
                            format!("value-{}", i).into_bytes(),
                        )
                    })
                    .collect();

                async { black_box(client.write(ORG, Some(0), ops).await.expect("write failed")) }
            })
        });
    }
    group.finish();
}

/// Benchmark batch write with multiple operation groups.
///
/// Each group is a Vec<Operation> committed atomically.
fn bench_batch_write(c: &mut Criterion) {
    let rt = create_runtime();

    // Setup: start mock server and create client
    let (client, _server) = rt.block_on(async {
        let server = MockLedgerServer::start().await.expect("Failed to start mock server");
        let client = create_client_for_mock(server.endpoint()).await;
        (client, server)
    });

    let mut group = c.benchmark_group("batch_write");
    let mut key_counter = 0u64;

    for group_count in [1, 5, 10] {
        group.throughput(Throughput::Elements(group_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(group_count),
            &group_count,
            |b, &count| {
                b.to_async(&rt).iter(|| {
                    key_counter += 1;
                    let batches: Vec<Vec<Operation>> = (0..count)
                        .map(|g| {
                            vec![Operation::set_entity(
                                format!("batch-{}-group-{}", key_counter, g),
                                format!("group-value-{}", g).into_bytes(),
                            )]
                        })
                        .collect();

                    async {
                        black_box(
                            client
                                .batch_write(ORG, Some(0), batches)
                                .await
                                .expect("batch_write failed"),
                        )
                    }
                })
            },
        );
    }
    group.finish();
}

// ============================================================================
// Mixed Workload Benchmarks
// ============================================================================

/// Benchmark realistic read-heavy workload (90% reads, 10% writes).
fn bench_mixed_read_heavy(c: &mut Criterion) {
    let rt = create_runtime();

    // Setup: start mock server with some data
    let (client, _server) = rt.block_on(async {
        let server = MockLedgerServer::start().await.expect("Failed to start mock server");

        // Pre-populate some keys
        for i in 0..10 {
            server.set_entity(ORG, 0, &format!("static-key-{}", i), b"static-value");
        }

        let client = create_client_for_mock(server.endpoint()).await;
        (client, server)
    });

    let mut op_counter = 0u64;

    c.bench_function("mixed_90read_10write", |b| {
        b.to_async(&rt).iter(|| {
            op_counter += 1;
            let is_write = op_counter.is_multiple_of(10);
            let key_num = op_counter;
            let client = client.clone();

            async move {
                if is_write {
                    let ops = vec![Operation::set_entity(
                        format!("dynamic-key-{}", key_num),
                        b"dynamic-value".to_vec(),
                    )];
                    black_box(client.write(ORG, Some(0), ops).await.expect("write failed"));
                } else {
                    let key = format!("static-key-{}", key_num % 10);
                    black_box(client.read(ORG, Some(0), &key).await.expect("read failed"));
                }
            }
        })
    });
}

// ============================================================================
// Criterion Configuration
// ============================================================================

criterion_group!(
    name = read_benches;
    config = Criterion::default().sample_size(100);
    targets = bench_read_single, bench_read_batch
);

criterion_group!(
    name = write_benches;
    config = Criterion::default().sample_size(100);
    targets = bench_write_single, bench_write_multi_op, bench_batch_write
);

criterion_group!(
    name = mixed_benches;
    config = Criterion::default().sample_size(100);
    targets = bench_mixed_read_heavy
);

criterion_main!(read_benches, write_benches, mixed_benches);
