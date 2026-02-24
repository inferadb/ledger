//! B+ tree engine benchmarks.
//!
//! These benchmarks measure raw storage engine performance for key operations:
//! single-key lookups, batch inserts, range iteration, and mixed read/write
//! workloads. Results feed into CI regression detection.

#![allow(clippy::expect_used, missing_docs)]

use std::{hint::black_box, time::Duration};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use inferadb_ledger_store::{Database, FileBackend, InMemoryBackend, tables::Entities};
use tempfile::TempDir;

// =============================================================================
// Helpers
// =============================================================================

/// Populate a table with `count` sequential key-value pairs in batches.
fn populate_entities(db: &Database<FileBackend>, count: usize, batch_size: usize) {
    for batch_start in (0..count).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(count);
        let mut txn = db.write().expect("write txn");
        for i in batch_start..batch_end {
            let key = format!("key-{:08}", i).into_bytes();
            let value = format!("value-{}", i).into_bytes();
            txn.insert::<Entities>(&key, &value).expect("insert");
        }
        txn.commit().expect("commit");
    }
}

// =============================================================================
// Single-Key Lookups
// =============================================================================

/// Benchmark point lookups at various dataset sizes.
///
/// Measures B+ tree traversal cost as the tree grows deeper.
fn bench_point_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree/point_lookup");
    group.throughput(Throughput::Elements(1));

    for entity_count in [1_000, 10_000, 100_000] {
        let temp_dir = TempDir::new().expect("create temp dir");
        let db = Database::<FileBackend>::create(temp_dir.path().join("bench.db"))
            .expect("create database");
        populate_entities(&db, entity_count, 1000);

        group.bench_with_input(
            BenchmarkId::new("sequential", format!("{}k", entity_count / 1000)),
            &entity_count,
            |b, &entity_count| {
                let mut counter = 0usize;
                b.iter(|| {
                    counter = (counter + 1) % entity_count;
                    let key = format!("key-{:08}", counter).into_bytes();
                    let txn = db.read().expect("read txn");
                    let result = txn.get::<Entities>(&key);
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark missing-key lookups (worst case: full tree traversal, not found).
fn bench_missing_key_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree/missing_key");
    group.throughput(Throughput::Elements(1));

    let temp_dir = TempDir::new().expect("create temp dir");
    let db =
        Database::<FileBackend>::create(temp_dir.path().join("bench.db")).expect("create database");
    populate_entities(&db, 10_000, 1000);

    group.bench_function("10k_entries", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            counter += 1;
            let key = format!("missing-{}", counter).into_bytes();
            let txn = db.read().expect("read txn");
            let result = txn.get::<Entities>(&key);
            black_box(result)
        });
    });

    group.finish();
}

// =============================================================================
// Insertions
// =============================================================================

/// Benchmark sequential inserts with commit per batch.
fn bench_batch_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree/batch_insert");

    for batch_size in [10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("size", batch_size),
            &batch_size,
            |b, &batch_size| {
                let temp_dir = TempDir::new().expect("create temp dir");
                let db = Database::<FileBackend>::create(temp_dir.path().join("bench.db"))
                    .expect("create database");

                let mut counter = 0u64;
                b.iter(|| {
                    let mut txn = db.write().expect("write txn");
                    for _ in 0..batch_size {
                        counter += 1;
                        let key = format!("key-{:012}", counter).into_bytes();
                        let value = format!("val-{}", counter).into_bytes();
                        txn.insert::<Entities>(&key, &value).expect("insert");
                    }
                    txn.commit().expect("commit");
                });
            },
        );
    }

    group.finish();
}

/// Benchmark in-memory backend insert throughput (no fsync overhead).
fn bench_insert_in_memory(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree/insert_memory");
    group.throughput(Throughput::Elements(100));

    group.bench_function("batch_100", |b| {
        let db = Database::<InMemoryBackend>::open_in_memory().expect("create database");

        let mut counter = 0u64;
        b.iter(|| {
            let mut txn = db.write().expect("write txn");
            for _ in 0..100 {
                counter += 1;
                let key = format!("key-{:012}", counter).into_bytes();
                let value = format!("val-{}", counter).into_bytes();
                txn.insert::<Entities>(&key, &value).expect("insert");
            }
            txn.commit().expect("commit");
        });
    });

    group.finish();
}

// =============================================================================
// Range Iteration
// =============================================================================

/// Benchmark full-table iteration at various dataset sizes.
///
/// Measures the streaming iterator's buffer-refill performance.
/// The 1M entry benchmark validates that leaf linked list traversal (O(1)
/// per leaf boundary) scales linearly. Target: ≥2x improvement over the
/// pre-linked-list O(depth) per-leaf-boundary approach.
fn bench_iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree/iteration");

    for entity_count in [1_000, 10_000, 1_000_000] {
        let label = if entity_count >= 1_000_000 {
            format!("{}M", entity_count / 1_000_000)
        } else {
            format!("{}k", entity_count / 1000)
        };

        group.throughput(Throughput::Elements(entity_count as u64));

        let temp_dir = TempDir::new().expect("create temp dir");
        let db = Database::<FileBackend>::create(temp_dir.path().join("bench.db"))
            .expect("create database");
        populate_entities(&db, entity_count, 10_000);

        group.bench_with_input(BenchmarkId::new("full_scan", label), &entity_count, |b, _| {
            b.iter(|| {
                let txn = db.read().expect("read txn");
                let iter = txn.iter::<Entities>().expect("iter");
                let count = iter.count();
                black_box(count)
            });
        });
    }

    group.finish();
}

/// Benchmark range scans (10% of dataset).
fn bench_range_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree/range_scan");

    let entity_count = 10_000usize;
    let range_size = entity_count / 10; // 10% of dataset
    group.throughput(Throughput::Elements(range_size as u64));

    let temp_dir = TempDir::new().expect("create temp dir");
    let db =
        Database::<FileBackend>::create(temp_dir.path().join("bench.db")).expect("create database");
    populate_entities(&db, entity_count, 1000);

    group.bench_function("10pct_of_10k", |b| {
        let mut start = 0usize;
        b.iter(|| {
            start = (start + range_size) % entity_count;
            let start_key = format!("key-{:08}", start).into_bytes();
            let end_key = format!("key-{:08}", start + range_size).into_bytes();

            let txn = db.read().expect("read txn");
            let iter = txn.range::<Entities>(Some(&start_key), Some(&end_key)).expect("range iter");
            let count = iter.count();
            black_box(count)
        });
    });

    group.finish();
}

// =============================================================================
// Mixed Workload
// =============================================================================

// =============================================================================
// Compaction
// =============================================================================

/// Benchmark compaction on a 100K-entry tree with 30% fragmentation.
///
/// Uses `iter_custom` because compaction is destructive — each iteration
/// requires a fresh fragmented tree. Measures the O(N) forward-only
/// compaction via leaf linked list pointers.
fn bench_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree/compaction");
    group.throughput(Throughput::Elements(100_000));

    group.bench_function("100k_30pct_frag", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let temp_dir = TempDir::new().expect("create temp dir");
                let db = Database::<FileBackend>::create(temp_dir.path().join("bench.db"))
                    .expect("create database");

                // Populate 100K entries
                populate_entities(&db, 100_000, 10_000);

                // Delete 30% to create distributed fragmentation
                {
                    let mut txn = db.write().expect("write txn");
                    for i in (0..100_000usize).step_by(3) {
                        let key = format!("key-{:08}", i).into_bytes();
                        txn.delete::<Entities>(&key).expect("delete");
                    }
                    txn.commit().expect("commit");
                }

                // Measure compaction only
                let start = std::time::Instant::now();
                {
                    let mut txn = db.write().expect("write txn");
                    let stats = txn.compact_table::<Entities>(0.4).expect("compact");
                    txn.commit().expect("commit");
                    black_box(stats);
                }
                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

/// Benchmark a mixed read/write workload (90% reads, 10% writes).
fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree/mixed_workload");
    group.throughput(Throughput::Elements(100));

    let temp_dir = TempDir::new().expect("create temp dir");
    let db =
        Database::<FileBackend>::create(temp_dir.path().join("bench.db")).expect("create database");
    populate_entities(&db, 10_000, 1000);

    group.bench_function("90r_10w", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            // 90 reads
            for i in 0..90u64 {
                let key = format!("key-{:08}", (counter + i) % 10_000).into_bytes();
                let txn = db.read().expect("read txn");
                let _ = txn.get::<Entities>(&key);
            }

            // 10 writes in a single transaction
            let mut txn = db.write().expect("write txn");
            for _ in 0..10 {
                counter += 1;
                let key = format!("mixed-{:012}", counter).into_bytes();
                let value = format!("val-{}", counter).into_bytes();
                txn.insert::<Entities>(&key, &value).expect("insert");
            }
            txn.commit().expect("commit");

            black_box(counter)
        });
    });

    group.finish();
}

// =============================================================================
// Criterion Configuration
// =============================================================================

criterion_group! {
    name = lookup_benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(5))
        .sample_size(100);
    targets = bench_point_lookup, bench_missing_key_lookup
}

criterion_group! {
    name = insert_benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(5))
        .sample_size(50);
    targets = bench_batch_insert, bench_insert_in_memory
}

criterion_group! {
    name = scan_benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .sample_size(20);
    targets = bench_iteration, bench_range_scan
}

criterion_group! {
    name = mixed_benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(5))
        .sample_size(50);
    targets = bench_mixed_workload
}

criterion_group! {
    name = compaction_benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(30))
        .sample_size(10);
    targets = bench_compaction
}

criterion_main!(lookup_benches, insert_benches, scan_benches, mixed_benches, compaction_benches);
