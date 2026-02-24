//! File I/O benchmarks for `FileBackend` lock-free pread/pwrite.
//!
//! Measures concurrent read throughput scaling to verify that position-based
//! I/O eliminates read serialization. The benchmark spawns 1, 4, 8, and 16
//! concurrent readers, each reading distinct pages, and measures wall-clock
//! time to verify near-linear scaling.

#![allow(clippy::expect_used, missing_docs)]

use std::{hint::black_box, sync::Arc};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use inferadb_ledger_store::{FileBackend, StorageBackend};
use tempfile::TempDir;

// =============================================================================
// Helpers
// =============================================================================

/// Create a `FileBackend` with `page_count` pre-written pages of distinct data.
fn create_populated_backend(page_count: u64) -> (TempDir, Arc<FileBackend>) {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("bench.db");
    let backend = FileBackend::create(&path, 4096).expect("create");
    let page_size = backend.page_size();

    for page_id in 0..page_count {
        let mut data = vec![(page_id & 0xFF) as u8; page_size];
        data[0] = 0xBE;
        data[1] = (page_id >> 8) as u8;
        backend.write_page(page_id, &data).expect("write_page");
    }
    backend.sync().expect("sync");

    (dir, Arc::new(backend))
}

// =============================================================================
// Concurrent Read Throughput
// =============================================================================

/// Benchmark: 8 concurrent `read_page()` calls on different pages — verify
/// no serialization (wall-clock time ≈ single read, not 8x).
fn bench_concurrent_read_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_io/concurrent_reads");
    let (_dir, backend) = create_populated_backend(64);

    for thread_count in [1, 4, 8, 16] {
        group.bench_with_input(
            BenchmarkId::new("readers", thread_count),
            &thread_count,
            |b, &threads| {
                b.iter(|| {
                    let mut handles = Vec::with_capacity(threads);
                    for t in 0..threads {
                        let b = Arc::clone(&backend);
                        let page_id = (t as u64) % 64;
                        handles.push(std::thread::spawn(move || {
                            // Each reader performs multiple reads to amortize
                            // thread spawn overhead.
                            for _ in 0..100 {
                                let data = b.read_page(page_id).expect("read");
                                black_box(&data);
                            }
                        }));
                    }
                    for h in handles {
                        h.join().expect("join");
                    }
                });
            },
        );
    }
    group.finish();
}

/// Benchmark: single-threaded read_page throughput baseline.
fn bench_single_read_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_io/single_read");
    let (_dir, backend) = create_populated_backend(64);

    group.bench_function("read_page_4k", |b| {
        b.iter(|| {
            for page_id in 0..64 {
                let data = backend.read_page(page_id).expect("read");
                black_box(&data);
            }
        });
    });
    group.finish();
}

criterion_group!(benches, bench_concurrent_read_scaling, bench_single_read_throughput);
criterion_main!(benches);
