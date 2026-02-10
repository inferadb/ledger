//! Write operation benchmarks.
//!
//! These benchmarks measure the performance of write operations
//! to validate against DESIGN.md performance targets.
//!
//! Target: >10K writes/second per vault.

#![allow(clippy::expect_used, missing_docs)]

use std::{hint::black_box, sync::Arc, time::Duration};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::{Database, FileBackend};
use inferadb_ledger_types::{Operation, VaultId};
use parking_lot::RwLock;
use tempfile::TempDir;

/// Creates a test state layer.
fn create_state_layer(temp_dir: &TempDir) -> StateLayer<FileBackend> {
    let db =
        Database::<FileBackend>::create(temp_dir.path().join("test.db")).expect("create database");
    StateLayer::new(Arc::new(db))
}

/// Benchmark single entity writes via apply_operations.
fn bench_single_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_writes");
    group.throughput(Throughput::Elements(1));

    let temp_dir = TempDir::new().expect("create temp dir");
    let state = Arc::new(RwLock::new(create_state_layer(&temp_dir)));

    for vault_id in [VaultId::new(1), VaultId::new(10), VaultId::new(100)] {
        group.bench_with_input(
            BenchmarkId::new("vault", vault_id.value()),
            &vault_id,
            |b, &vault_id| {
                let mut counter = 0u64;
                b.iter(|| {
                    counter += 1;
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
            },
        );
    }

    group.finish();
}

/// Benchmark batch writes.
fn bench_batch_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_writes");
    group.sample_size(50);

    for batch_size in [10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        let temp_dir = TempDir::new().expect("create temp dir");
        let state = Arc::new(RwLock::new(create_state_layer(&temp_dir)));

        group.bench_with_input(
            BenchmarkId::new("batch_size", batch_size),
            &batch_size,
            |b, &batch_size| {
                let mut batch_counter = 0u64;
                b.iter(|| {
                    batch_counter += 1;
                    let vault_id = VaultId::new(1);

                    let operations: Vec<Operation> = (0..batch_size)
                        .map(|i| {
                            let key = format!("batch-{}-key-{}", batch_counter, i);
                            let value = format!("batch-{}-value-{}", batch_counter, i);
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
            },
        );
    }

    group.finish();
}

/// Benchmark state root computation.
fn bench_state_root(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_root");

    // Pre-populate with different numbers of entities
    for entity_count in [100, 1000, 10000] {
        let temp_dir = TempDir::new().expect("create temp dir");
        let state = create_state_layer(&temp_dir);
        let vault_id = VaultId::new(1);

        // Populate
        for i in 0..entity_count {
            let operations = vec![Operation::SetEntity {
                key: format!("key-{}", i),
                value: format!("value-{}", i).into_bytes(),
                expires_at: None,
                condition: None,
            }];
            state.apply_operations(vault_id, &operations, i as u64).expect("apply");
        }

        let state = Arc::new(RwLock::new(state));

        group.bench_with_input(
            BenchmarkId::new("entities", entity_count),
            &entity_count,
            |b, _| {
                b.iter(|| {
                    let state = state.read();
                    // Compute state root by getting all bucket roots
                    let commitment = state.compute_state_root(vault_id);
                    black_box(commitment)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark concurrent writes to different vaults.
fn bench_concurrent_vault_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_vault_writes");
    group.throughput(Throughput::Elements(10)); // 10 vaults

    let temp_dir = TempDir::new().expect("create temp dir");
    let state = Arc::new(RwLock::new(create_state_layer(&temp_dir)));

    group.bench_function("10_vaults", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            counter += 1;
            for vid in 1..=10i64 {
                let vault_id = VaultId::new(vid);
                let key = format!("vault-{}-key-{}", vid, counter);
                let value = format!("value-{}", counter);

                let operations = vec![Operation::SetEntity {
                    key,
                    value: value.into_bytes(),
                    expires_at: None,
                    condition: None,
                }];

                let state = state.read();
                let _ = state.apply_operations(vault_id, &operations, counter);
            }
            black_box(counter)
        });
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(5))
        .sample_size(100);
    targets = bench_single_writes, bench_batch_writes, bench_state_root, bench_concurrent_vault_writes
}

criterion_main!(benches);
