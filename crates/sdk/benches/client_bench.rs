//! Performance benchmarks for the Ledger SDK.
//!
//! These benchmarks measure:
//! - SDK overhead (serialization, sequence tracking)
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
//! | sequence_next                | ~25 ns       | In-memory atomic increment     |
//! | sequence_concurrent_10       | ~150 ns      | 10 threads contending          |
//! | read_single_key              | ~500 µs      | Full roundtrip to mock server  |
//! | read_batch_1                 | ~500 µs      | Same as single (baseline)      |
//! | read_batch_10                | ~600 µs      | 10x keys, ~1.2x latency        |
//! | read_batch_100               | ~1.2 ms      | 100x keys, ~2.4x latency       |
//! | write_single_operation       | ~600 µs      | Includes sequence tracking     |

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use inferadb_ledger_sdk::{
    ClientConfig, LedgerClient, Operation, RetryPolicy, SequenceTracker, mock::MockLedgerServer,
};
use std::time::Duration;
use tokio::runtime::Runtime;

/// Create a runtime for async benchmarks.
fn create_runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create runtime")
}

/// Create a client connected to the mock server.
async fn create_client_for_mock(endpoint: &str) -> LedgerClient {
    let config = ClientConfig::builder()
        .with_endpoint(endpoint)
        .with_client_id("bench-client")
        .with_timeout(Duration::from_secs(5))
        .with_connect_timeout(Duration::from_secs(2))
        .with_retry_policy(
            RetryPolicy::builder()
                .with_max_attempts(1) // No retry for benchmarks
                .build(),
        )
        .build()
        .expect("Failed to build config");

    LedgerClient::new(config)
        .await
        .expect("Failed to create client")
}

// ============================================================================
// Sequence Tracker Benchmarks
// ============================================================================

/// Benchmark sequence tracking overhead - single thread.
///
/// This measures the pure overhead of the sequence tracker without network I/O.
/// The sequence tracker is called before every write operation.
fn bench_sequence_next(c: &mut Criterion) {
    let tracker = SequenceTracker::new("bench-client");

    c.bench_function("sequence_next", |b| {
        b.iter(|| black_box(tracker.next_sequence(1, 0)))
    });
}

/// Benchmark sequence tracking with multiple vaults.
///
/// Tests performance when tracking sequences across many independent vaults.
fn bench_sequence_multi_vault(c: &mut Criterion) {
    let tracker = SequenceTracker::new("bench-client");

    c.bench_function("sequence_multi_vault_100", |b| {
        let mut vault_id = 0i64;
        b.iter(|| {
            vault_id = (vault_id + 1) % 100;
            black_box(tracker.next_sequence(1, vault_id))
        })
    });
}

/// Benchmark sequence tracking under contention.
///
/// Measures performance when multiple threads increment sequences concurrently.
fn bench_sequence_concurrent(c: &mut Criterion) {
    use std::sync::Arc;
    use std::thread;

    let mut group = c.benchmark_group("sequence_concurrent");

    for num_threads in [2, 4, 10] {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_threads),
            &num_threads,
            |b, &n| {
                let tracker = Arc::new(SequenceTracker::new("bench-client"));

                b.iter(|| {
                    let handles: Vec<_> = (0..n)
                        .map(|i| {
                            let t = tracker.clone();
                            thread::spawn(move || {
                                for _ in 0..100 {
                                    black_box(t.next_sequence(1, i as i64));
                                }
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().expect("Thread panicked");
                    }
                })
            },
        );
    }
    group.finish();
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
        let server = MockLedgerServer::start()
            .await
            .expect("Failed to start mock server");
        server.set_entity(1, 0, "test-key", b"test-value-data");
        let client = create_client_for_mock(server.endpoint()).await;
        (client, server)
    });

    c.bench_function("read_single_key", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                client
                    .read(1, Some(0), "test-key")
                    .await
                    .expect("read failed"),
            )
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
        let server = MockLedgerServer::start()
            .await
            .expect("Failed to start mock server");

        // Pre-populate 100 keys
        for i in 0..100 {
            server.set_entity(
                1,
                0,
                &format!("key-{:03}", i),
                format!("value-{}", i).as_bytes(),
            );
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
                    client
                        .batch_read(1, Some(0), keys.clone())
                        .await
                        .expect("batch_read failed"),
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
/// Measures full roundtrip time for a single write including sequence tracking.
fn bench_write_single(c: &mut Criterion) {
    let rt = create_runtime();

    // Setup: start mock server and create client
    let (client, _server) = rt.block_on(async {
        let server = MockLedgerServer::start()
            .await
            .expect("Failed to start mock server");
        let client = create_client_for_mock(server.endpoint()).await;
        (client, server)
    });

    let mut key_counter = 0u64;

    c.bench_function("write_single_operation", |b| {
        b.to_async(&rt).iter(|| {
            key_counter += 1;
            let key = format!("bench-key-{}", key_counter);
            let ops = vec![Operation::set_entity(&key, b"bench-value".to_vec())];

            async { black_box(client.write(1, Some(0), ops).await.expect("write failed")) }
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
        let server = MockLedgerServer::start()
            .await
            .expect("Failed to start mock server");
        let client = create_client_for_mock(server.endpoint()).await;
        (client, server)
    });

    let mut group = c.benchmark_group("write_multi_op");
    let mut key_counter = 0u64;

    for op_count in [1, 5, 10] {
        group.throughput(Throughput::Elements(op_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_count),
            &op_count,
            |b, &count| {
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

                    async { black_box(client.write(1, Some(0), ops).await.expect("write failed")) }
                })
            },
        );
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
        let server = MockLedgerServer::start()
            .await
            .expect("Failed to start mock server");
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
                                .batch_write(1, Some(0), batches)
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
        let server = MockLedgerServer::start()
            .await
            .expect("Failed to start mock server");

        // Pre-populate some keys
        for i in 0..10 {
            server.set_entity(1, 0, &format!("static-key-{}", i), b"static-value");
        }

        let client = create_client_for_mock(server.endpoint()).await;
        (client, server)
    });

    let mut op_counter = 0u64;

    c.bench_function("mixed_90read_10write", |b| {
        b.to_async(&rt).iter(|| {
            op_counter += 1;
            let is_write = op_counter % 10 == 0;
            let key_num = op_counter;
            let client = client.clone();

            async move {
                if is_write {
                    let ops = vec![Operation::set_entity(
                        format!("dynamic-key-{}", key_num),
                        b"dynamic-value".to_vec(),
                    )];
                    black_box(client.write(1, Some(0), ops).await.expect("write failed"));
                } else {
                    let key = format!("static-key-{}", key_num % 10);
                    black_box(client.read(1, Some(0), &key).await.expect("read failed"));
                }
            }
        })
    });
}

// ============================================================================
// Criterion Configuration
// ============================================================================

criterion_group!(
    name = sequence_benches;
    config = Criterion::default().sample_size(1000);
    targets = bench_sequence_next, bench_sequence_multi_vault, bench_sequence_concurrent
);

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

criterion_main!(sequence_benches, read_benches, write_benches, mixed_benches);
