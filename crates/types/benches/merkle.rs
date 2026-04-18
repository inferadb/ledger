//! Criterion benchmark: Merkle root computation across representative leaf counts.
//!
//! Run: `cargo +1.92 bench -p inferadb-ledger-types --bench merkle`
//! Profile: `just profile-bench types merkle`
//!
//! The workload is pure-CPU hashing (SHA-256 via `rs_merkle`), which makes it a
//! good first target for flamegraph comparison: shape is stable across runs and
//! the hot path is almost entirely in user code.

// Criterion's `criterion_group!` / `criterion_main!` macros expand into public
// items (e.g. `pub fn benches()`) without doc comments. Benches are dev-only
// targets, not part of the public API, so silence missing-docs here.
#![allow(missing_docs)]

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use inferadb_ledger_types::{Hash, merkle::merkle_root};

/// Deterministic leaf generator. The first 8 bytes of each 32-byte leaf encode
/// the leaf index in little-endian; remaining bytes are zero. No RNG, no system
/// time — shape stability across runs is required for flamegraph-to-flamegraph
/// comparison.
fn leaves(count: usize) -> Vec<Hash> {
    (0..count)
        .map(|i| {
            let mut buf: Hash = [0u8; 32];
            buf[..8].copy_from_slice(&(i as u64).to_le_bytes());
            buf
        })
        .collect()
}

fn bench_merkle_root(c: &mut Criterion) {
    let mut group = c.benchmark_group("merkle_root");
    for count in [16usize, 256, 4096] {
        let inputs = leaves(count);
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &inputs, |b, leaves| {
            b.iter(|| {
                let root = merkle_root(black_box(leaves));
                black_box(root);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_merkle_root);
criterion_main!(benches);
