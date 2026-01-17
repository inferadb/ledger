//! Read operation benchmarks.
//!
//! These benchmarks measure the performance of read operations
//! to validate against DESIGN.md performance targets.
//!
//! Target: <1ms p99 latency for single-key reads.

#![allow(clippy::expect_used, missing_docs)]

use std::{hint::black_box, sync::Arc, time::Duration};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::{Database, FileBackend};
use inferadb_ledger_types::Operation;
use parking_lot::RwLock;
use tempfile::TempDir;

/// Create a test state layer pre-populated with data.
fn create_populated_state_layer(
    temp_dir: &TempDir,
    vault_id: i64,
    entity_count: usize,
) -> StateLayer<FileBackend> {
    let db =
        Database::<FileBackend>::create(temp_dir.path().join("test.db")).expect("create database");
    let state = StateLayer::new(Arc::new(db));

    // Populate with entities
    for i in 0..entity_count {
        let operations = vec![Operation::SetEntity {
            key: format!("key-{:08}", i),
            value: format!("value-{}", i).into_bytes(),
            expires_at: None,
            condition: None,
        }];
        state.apply_operations(vault_id, &operations, i as u64).expect("apply");
    }

    state
}

/// Benchmark single key reads.
fn bench_single_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_reads");
    group.throughput(Throughput::Elements(1));

    for entity_count in [1000, 10000, 100000] {
        let temp_dir = TempDir::new().expect("create temp dir");
        let vault_id = 1i64;
        let state =
            Arc::new(RwLock::new(create_populated_state_layer(&temp_dir, vault_id, entity_count)));

        group.bench_with_input(
            BenchmarkId::new("entities", entity_count),
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

/// Benchmark sequential reads (cache-friendly access pattern).
fn bench_sequential_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_reads");
    group.throughput(Throughput::Elements(100)); // 100 reads per iteration

    let temp_dir = TempDir::new().expect("create temp dir");
    let vault_id = 1i64;
    let entity_count = 10000;
    let state =
        Arc::new(RwLock::new(create_populated_state_layer(&temp_dir, vault_id, entity_count)));

    group.bench_function("100_keys", |b| {
        let mut start = 0usize;
        b.iter(|| {
            start = (start + 100) % entity_count;
            let state = state.read();
            for i in 0..100 {
                let idx = (start + i) % entity_count;
                let key = format!("key-{:08}", idx);
                let _ = state.get_entity(vault_id, key.as_bytes());
            }
            black_box(start)
        });
    });

    group.finish();
}

/// Benchmark random reads (cache-unfriendly access pattern).
fn bench_random_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_reads");
    group.throughput(Throughput::Elements(100)); // 100 reads per iteration

    let temp_dir = TempDir::new().expect("create temp dir");
    let vault_id = 1i64;
    let entity_count = 10000;
    let state =
        Arc::new(RwLock::new(create_populated_state_layer(&temp_dir, vault_id, entity_count)));

    // Pre-generate random indices
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let random_indices: Vec<usize> = (0..10000).map(|_| rng.gen_range(0..entity_count)).collect();

    group.bench_function("100_random_keys", |b| {
        let mut batch = 0usize;
        b.iter(|| {
            batch = (batch + 1) % 100;
            let start = batch * 100;
            let state = state.read();
            for i in 0..100 {
                let idx = random_indices[(start + i) % random_indices.len()];
                let key = format!("key-{:08}", idx);
                let _ = state.get_entity(vault_id, key.as_bytes());
            }
            black_box(batch)
        });
    });

    group.finish();
}

/// Benchmark reads from different vaults.
fn bench_multi_vault_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_vault_reads");
    group.throughput(Throughput::Elements(10)); // 10 vaults

    let temp_dir = TempDir::new().expect("create temp dir");
    let db =
        Database::<FileBackend>::create(temp_dir.path().join("test.db")).expect("create database");
    let state = StateLayer::new(Arc::new(db));

    // Populate 10 vaults with 1000 entities each
    for vault_id in 1..=10i64 {
        for i in 0..1000 {
            let operations = vec![Operation::SetEntity {
                key: format!("vault-{}-key-{:08}", vault_id, i),
                value: format!("value-{}", i).into_bytes(),
                expires_at: None,
                condition: None,
            }];
            state.apply_operations(vault_id, &operations, i as u64).expect("apply");
        }
    }

    let state = Arc::new(RwLock::new(state));

    group.bench_function("10_vaults", |b| {
        let mut counter = 0usize;
        b.iter(|| {
            counter = (counter + 1) % 1000;
            let state = state.read();
            for vault_id in 1..=10i64 {
                let key = format!("vault-{}-key-{:08}", vault_id, counter);
                let _ = state.get_entity(vault_id, key.as_bytes());
            }
            black_box(counter)
        });
    });

    group.finish();
}

/// Benchmark missing key reads.
fn bench_missing_key_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("missing_key_reads");
    group.throughput(Throughput::Elements(1));

    let temp_dir = TempDir::new().expect("create temp dir");
    let vault_id = 1i64;
    let state = Arc::new(RwLock::new(create_populated_state_layer(&temp_dir, vault_id, 10000)));

    group.bench_function("missing_key", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            counter += 1;
            let key = format!("nonexistent-{}", counter);

            let state = state.read();
            let result = state.get_entity(vault_id, key.as_bytes());
            black_box(result)
        });
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(5))
        .sample_size(100);
    targets = bench_single_reads, bench_sequential_reads, bench_random_reads, bench_multi_vault_reads, bench_missing_key_reads
}

criterion_main!(benches);
