//! Whitepaper performance benchmarks.
//!
//! These benchmarks validate the performance claims in WHITEPAPER.md against
//! actual measurements. Results are output in a format suitable for documentation.
//!
//! # Performance Targets (from DESIGN.md)
//!
//! | Metric              | Target          | Measurement Condition       |
//! |---------------------|-----------------|----------------------------|
//! | Read latency (p99)  | < 2 ms          | Single key lookup          |
//! | Write latency (p99) | < 50 ms         | Batch committed            |
//! | Write throughput    | 5,000 tx/sec    | 3-node cluster             |
//! | Read throughput     | 100,000 req/sec | Eventually consistent      |
//!
//! # Running
//!
//! ```bash
//! cargo bench -p inferadb-ledger-server --bench whitepaper_bench
//! ```
//!
//! For detailed output with percentiles:
//!
//! ```bash
//! cargo bench -p inferadb-ledger-server --bench whitepaper_bench -- --verbose
//! ```

#![allow(clippy::expect_used, missing_docs)]

use std::{
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant},
};

use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::{Database, FileBackend};
use inferadb_ledger_types::{Operation, VaultId};
use parking_lot::RwLock;
use tempfile::TempDir;

// =============================================================================
// Configuration
// =============================================================================

/// Optimal batch size for throughput benchmarks (matches server default).
const OPTIMAL_BATCH_SIZE: usize = 100;

/// Number of entities for scale testing (matches whitepaper "10 million keys" scenario).
const SCALE_ENTITY_COUNT: usize = 100_000; // Reduced for CI; full test uses 10M

// =============================================================================
// Helpers
// =============================================================================

/// Create a state layer with optimal configuration.
fn create_state_layer(temp_dir: &TempDir) -> StateLayer<FileBackend> {
    let db =
        Database::<FileBackend>::create(temp_dir.path().join("bench.db")).expect("create database");
    StateLayer::new(Arc::new(db))
}

/// Pre-populate a vault with entities for read benchmarks.
fn populate_vault(state: &StateLayer<FileBackend>, vault_id: VaultId, count: usize) {
    // Use batched writes for faster population
    const BATCH_SIZE: usize = 1000;

    for (sequence, batch_start) in (0..count).step_by(BATCH_SIZE).enumerate() {
        let batch_end = (batch_start + BATCH_SIZE).min(count);
        let operations: Vec<Operation> = (batch_start..batch_end)
            .map(|i| Operation::SetEntity {
                key: format!("key-{:08}", i),
                value: format!("value-{}", i).into_bytes(),
                expires_at: None,
                condition: None,
            })
            .collect();

        state.apply_operations(vault_id, &operations, sequence as u64).expect("populate");
    }
}

/// Collect latency samples and compute percentiles.
struct LatencyCollector {
    samples: Vec<Duration>,
}

impl LatencyCollector {
    fn new(capacity: usize) -> Self {
        Self { samples: Vec::with_capacity(capacity) }
    }

    fn record(&mut self, duration: Duration) {
        self.samples.push(duration);
    }

    fn percentile(&mut self, p: f64) -> Duration {
        self.samples.sort();
        let idx = ((self.samples.len() as f64) * p / 100.0).ceil() as usize;
        let idx = idx.saturating_sub(1).min(self.samples.len() - 1);
        self.samples[idx]
    }

    fn mean(&self) -> Duration {
        let total: Duration = self.samples.iter().sum();
        total / self.samples.len() as u32
    }
}

// =============================================================================
// Whitepaper Benchmark: Read Latency
// =============================================================================

/// Benchmark single-key read latency at various dataset sizes.
///
/// Target: p99 < 2ms
fn bench_read_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("whitepaper/read_latency");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(1000);
    group.measurement_time(Duration::from_secs(10));

    for entity_count in [10_000, 100_000] {
        let temp_dir = TempDir::new().expect("create temp dir");
        let vault_id = VaultId::new(1);
        let state = create_state_layer(&temp_dir);

        // Pre-populate
        populate_vault(&state, vault_id, entity_count);

        let state = Arc::new(RwLock::new(state));

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("single_key", format!("{}k_entities", entity_count / 1000)),
            &entity_count,
            |b, &entity_count| {
                let mut counter = 0usize;
                b.iter(|| {
                    counter = (counter + 1) % entity_count;
                    let key = format!("key-{:08}", counter);

                    let state = state.read();
                    let result = state.get_entity(vault_id, key.as_bytes());
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark random-access read latency (worst-case cache behavior).
fn bench_read_latency_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("whitepaper/read_latency_random");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(1000);
    group.measurement_time(Duration::from_secs(10));

    let temp_dir = TempDir::new().expect("create temp dir");
    let vault_id = VaultId::new(1);
    let entity_count = SCALE_ENTITY_COUNT;
    let state = create_state_layer(&temp_dir);

    populate_vault(&state, vault_id, entity_count);

    // Pre-generate random indices
    use rand::Rng;
    let mut rng = rand::rng();
    let random_indices: Vec<usize> =
        (0..10000).map(|_| rng.random_range(0..entity_count)).collect();

    let state = Arc::new(RwLock::new(state));

    group.throughput(Throughput::Elements(1));
    group.bench_function("random_access", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            idx = (idx + 1) % random_indices.len();
            let key = format!("key-{:08}", random_indices[idx]);

            let state = state.read();
            let result = state.get_entity(vault_id, key.as_bytes());
            black_box(result)
        });
    });

    group.finish();
}

// =============================================================================
// Whitepaper Benchmark: Write Latency
// =============================================================================

/// Benchmark write latency for individual operations.
///
/// Note: This measures StateLayer directly, not including Raft consensus.
/// Full end-to-end latency (with Raft) is measured separately.
fn bench_write_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("whitepaper/write_latency");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(500);
    group.measurement_time(Duration::from_secs(10));

    let temp_dir = TempDir::new().expect("create temp dir");
    let state = Arc::new(RwLock::new(create_state_layer(&temp_dir)));

    group.throughput(Throughput::Elements(1));
    group.bench_function("single_write", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            counter += 1;
            let vault_id = VaultId::new(1);
            let key = format!("key-{}", counter);
            let value = format!("value-{}", counter);

            let operations = vec![Operation::SetEntity {
                key,
                value: value.into_bytes(),
                expires_at: None,
                condition: None,
            }];

            let state = state.read();
            let result = state.apply_operations(vault_id, &operations, counter);
            black_box(result)
        });
    });

    group.finish();
}

// =============================================================================
// Whitepaper Benchmark: Write Throughput
// =============================================================================

/// Benchmark batch write throughput at optimal batch size.
///
/// Target: >5,000 tx/sec (note: this is StateLayer only, not full Raft)
fn bench_write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("whitepaper/write_throughput");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(15));

    let temp_dir = TempDir::new().expect("create temp dir");
    let state = Arc::new(RwLock::new(create_state_layer(&temp_dir)));

    group.throughput(Throughput::Elements(OPTIMAL_BATCH_SIZE as u64));
    group.bench_function(format!("batch_{}", OPTIMAL_BATCH_SIZE), |b| {
        let mut batch_counter = 0u64;
        b.iter(|| {
            batch_counter += 1;
            let vault_id = VaultId::new(1);

            let operations: Vec<Operation> = (0..OPTIMAL_BATCH_SIZE)
                .map(|i| {
                    let key = format!("batch-{}-key-{}", batch_counter, i);
                    let value = format!("value-{}", i);
                    Operation::SetEntity {
                        key,
                        value: value.into_bytes(),
                        expires_at: None,
                        condition: None,
                    }
                })
                .collect();

            let state = state.read();
            let result = state.apply_operations(vault_id, &operations, batch_counter);
            black_box(result)
        });
    });

    group.finish();
}

// =============================================================================
// Whitepaper Benchmark: Read Throughput
// =============================================================================

/// Benchmark read throughput with sequential access pattern.
///
/// Target: >100,000 req/sec
fn bench_read_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("whitepaper/read_throughput");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(500);
    group.measurement_time(Duration::from_secs(15));

    let temp_dir = TempDir::new().expect("create temp dir");
    let vault_id = VaultId::new(1);
    let entity_count = SCALE_ENTITY_COUNT;
    let state = create_state_layer(&temp_dir);

    populate_vault(&state, vault_id, entity_count);

    let state = Arc::new(RwLock::new(state));

    // Batch read throughput (100 keys per iteration)
    const READ_BATCH_SIZE: usize = 100;
    group.throughput(Throughput::Elements(READ_BATCH_SIZE as u64));
    group.bench_function(format!("batch_{}", READ_BATCH_SIZE), |b| {
        let mut start = 0usize;
        b.iter(|| {
            start = (start + READ_BATCH_SIZE) % entity_count;
            let state = state.read();
            for i in 0..READ_BATCH_SIZE {
                let idx = (start + i) % entity_count;
                let key = format!("key-{:08}", idx);
                let _ = state.get_entity(vault_id, key.as_bytes());
            }
            black_box(start)
        });
    });

    group.finish();
}

// =============================================================================
// Whitepaper Benchmark: State Root Computation
// =============================================================================

/// Benchmark state root computation complexity.
///
/// Per whitepaper: O(k) where k = modified keys, independent of total database size.
fn bench_state_root(c: &mut Criterion) {
    let mut group = c.benchmark_group("whitepaper/state_root");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(10));

    // Test at different database sizes to validate O(k) claim
    for entity_count in [10_000, 50_000, 100_000] {
        let temp_dir = TempDir::new().expect("create temp dir");
        let vault_id = VaultId::new(1);
        let state = create_state_layer(&temp_dir);

        populate_vault(&state, vault_id, entity_count);

        let state = Arc::new(RwLock::new(state));

        group.bench_with_input(
            BenchmarkId::new("compute", format!("{}k_entities", entity_count / 1000)),
            &entity_count,
            |b, _| {
                b.iter(|| {
                    let state = state.read();
                    let commitment = state.compute_state_root(vault_id);
                    black_box(commitment)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark incremental state root update cost.
///
/// Validates that update cost is O(k) not O(n).
fn bench_state_root_incremental(c: &mut Criterion) {
    let mut group = c.benchmark_group("whitepaper/state_root_incremental");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(10));

    // Fixed large dataset
    let temp_dir = TempDir::new().expect("create temp dir");
    let vault_id = VaultId::new(1);
    let state = create_state_layer(&temp_dir);

    populate_vault(&state, vault_id, SCALE_ENTITY_COUNT);

    let state = Arc::new(RwLock::new(state));

    // Test update cost at different batch sizes (k)
    for update_count in [1, 10, 100] {
        group.bench_with_input(
            BenchmarkId::new("update", format!("{}_keys", update_count)),
            &update_count,
            |b, &update_count| {
                let mut batch_counter = 0u64;
                b.iter(|| {
                    batch_counter += 1;
                    let operations: Vec<Operation> = (0..update_count)
                        .map(|i| {
                            let key = format!("update-{}-key-{}", batch_counter, i);
                            let value = format!("updated-value-{}", i);
                            Operation::SetEntity {
                                key,
                                value: value.into_bytes(),
                                expires_at: None,
                                condition: None,
                            }
                        })
                        .collect();

                    let state = state.read();
                    let _ = state.apply_operations(vault_id, &operations, batch_counter);

                    // Measure state root computation after update
                    let commitment = state.compute_state_root(vault_id);
                    black_box(commitment)
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Whitepaper Benchmark: Hash Operation Count
// =============================================================================

/// Standalone benchmark to measure and print whitepaper-ready statistics.
///
/// Run with: `cargo bench -p inferadb-ledger-server --bench whitepaper_bench -- --verbose`
fn bench_whitepaper_summary(c: &mut Criterion) {
    let mut group = c.benchmark_group("whitepaper/summary");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(1000);
    group.measurement_time(Duration::from_secs(10));

    // Single comprehensive benchmark that outputs summary statistics
    let temp_dir = TempDir::new().expect("create temp dir");
    let vault_id = VaultId::new(1);
    let state = create_state_layer(&temp_dir);

    // Populate with significant data
    let entity_count = 50_000;
    populate_vault(&state, vault_id, entity_count);

    let state = Arc::new(RwLock::new(state));

    group.throughput(Throughput::Elements(1));
    group.bench_function("single_read_50k_dataset", |b| {
        let mut counter = 0usize;
        b.iter(|| {
            counter = (counter + 1) % entity_count;
            let key = format!("key-{:08}", counter);

            let state = state.read();
            let result = state.get_entity(vault_id, key.as_bytes());
            black_box(result)
        });
    });

    group.finish();
}

// =============================================================================
// Custom Percentile Benchmark (for whitepaper reporting)
// =============================================================================

/// Manual latency measurement with explicit percentile calculation.
///
/// This provides exact p50/p95/p99/p999 values for whitepaper documentation.
fn bench_latency_percentiles(c: &mut Criterion) {
    let mut group = c.benchmark_group("whitepaper/latency_percentiles");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10); // We do our own sampling
    group.measurement_time(Duration::from_secs(5));

    let temp_dir = TempDir::new().expect("create temp dir");
    let vault_id = VaultId::new(1);
    let state = create_state_layer(&temp_dir);

    let entity_count = 50_000;
    populate_vault(&state, vault_id, entity_count);

    let state = Arc::new(RwLock::new(state));

    group.bench_function("read_percentiles", |b| {
        b.iter_custom(|iters| {
            let mut collector = LatencyCollector::new(iters as usize);
            let mut counter = 0usize;

            for _ in 0..iters {
                counter = (counter + 1) % entity_count;
                let key = format!("key-{:08}", counter);

                let start = Instant::now();
                {
                    let state = state.read();
                    let _ = state.get_entity(vault_id, key.as_bytes());
                }
                collector.record(start.elapsed());
            }

            // Print percentiles on first run
            if iters > 100 {
                let p50 = collector.percentile(50.0);
                let p95 = collector.percentile(95.0);
                let p99 = collector.percentile(99.0);
                let p999 = collector.percentile(99.9);
                let mean = collector.mean();

                eprintln!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                eprintln!("  READ LATENCY ({}k entities, {} samples)", entity_count / 1000, iters);
                eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                eprintln!("  p50:  {:>8.2} µs", p50.as_nanos() as f64 / 1000.0);
                eprintln!("  p95:  {:>8.2} µs", p95.as_nanos() as f64 / 1000.0);
                eprintln!(
                    "  p99:  {:>8.2} µs  (target: <2,000 µs)",
                    p99.as_nanos() as f64 / 1000.0
                );
                eprintln!("  p999: {:>8.2} µs", p999.as_nanos() as f64 / 1000.0);
                eprintln!("  mean: {:>8.2} µs", mean.as_nanos() as f64 / 1000.0);
                eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
            }

            collector.mean() * iters as u32
        });
    });

    // Write latency percentiles
    let temp_dir2 = TempDir::new().expect("create temp dir");
    let state2 = Arc::new(RwLock::new(create_state_layer(&temp_dir2)));

    group.bench_function("write_percentiles", |b| {
        b.iter_custom(|iters| {
            let mut collector = LatencyCollector::new(iters as usize);
            let mut counter = 0u64;
            let vault_id = VaultId::new(1);

            for _ in 0..iters {
                counter += 1;
                let key = format!("key-{}", counter);
                let value = format!("value-{}", counter);

                let operations = vec![Operation::SetEntity {
                    key,
                    value: value.into_bytes(),
                    expires_at: None,
                    condition: None,
                }];

                let start = Instant::now();
                {
                    let state = state2.read();
                    let _ = state.apply_operations(vault_id, &operations, counter);
                }
                collector.record(start.elapsed());
            }

            // Print percentiles on first run
            if iters > 100 {
                let p50 = collector.percentile(50.0);
                let p95 = collector.percentile(95.0);
                let p99 = collector.percentile(99.0);
                let p999 = collector.percentile(99.9);
                let mean = collector.mean();

                eprintln!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                eprintln!("  WRITE LATENCY (single op, {} samples)", iters);
                eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                eprintln!("  p50:  {:>8.2} µs", p50.as_nanos() as f64 / 1000.0);
                eprintln!("  p95:  {:>8.2} µs", p95.as_nanos() as f64 / 1000.0);
                eprintln!("  p99:  {:>8.2} µs", p99.as_nanos() as f64 / 1000.0);
                eprintln!("  p999: {:>8.2} µs", p999.as_nanos() as f64 / 1000.0);
                eprintln!("  mean: {:>8.2} µs", mean.as_nanos() as f64 / 1000.0);
                eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
            }

            collector.mean() * iters as u32
        });
    });

    group.finish();
}

// =============================================================================
// Criterion Configuration
// =============================================================================

criterion_group! {
    name = read_benches;
    config = Criterion::default()
        .with_output_color(true)
        .significance_level(0.01)
        .noise_threshold(0.03);
    targets = bench_read_latency, bench_read_latency_random, bench_read_throughput
}

criterion_group! {
    name = write_benches;
    config = Criterion::default()
        .with_output_color(true)
        .significance_level(0.01)
        .noise_threshold(0.03);
    targets = bench_write_latency, bench_write_throughput
}

criterion_group! {
    name = state_root_benches;
    config = Criterion::default()
        .with_output_color(true)
        .significance_level(0.01)
        .noise_threshold(0.03);
    targets = bench_state_root, bench_state_root_incremental
}

criterion_group! {
    name = summary_benches;
    config = Criterion::default()
        .with_output_color(true);
    targets = bench_whitepaper_summary, bench_latency_percentiles
}

criterion_main!(read_benches, write_benches, state_root_benches, summary_benches);
