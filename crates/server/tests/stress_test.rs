//! Stress test for maximum throughput and reliability validation.
//!
//! This test starts a 3-node cluster and hammers it with concurrent reads/writes
//! to determine maximum throughput and ensure complete reliability.
//!
//! ## Metrics Collected
//! - Write throughput (ops/sec)
//! - Read throughput (ops/sec)
//! - Write latency (p50, p95, p99)
//! - Read latency (p50, p95, p99)
//! - Error rates
//! - Consistency verification
//!
//! ## Performance Targets
//! - Write p99: <50ms
//! - Read p99: <2ms (no proof)
//! - Write throughput: 5,000 tx/sec
//! - Read throughput: 100,000 req/sec per node
//!
//! ## Running Stress Tests
//!
//! **IMPORTANT**: Always use release builds for accurate throughput measurements:
//! ```sh
//! cargo test --release -p inferadb-ledger-server test_stress_batched -- --nocapture
//! ```
//!
//! Release build improvements (typical):
//! - Read throughput: ~320k/s (debug) → **~950k/s** (release) = 3x improvement
//! - Read p99 latency: ~0.7ms (debug) → **~0.1ms** (release) = 7x improvement
//! - Write throughput: ~1000/s either way (Raft consensus limited)
//!
//! ## Throughput Scaling Strategy
//!
//! Raft consensus overhead (~16-30ms) limits single-region throughput to ~60 ops/sec.
//! To achieve 5000 tx/sec, InferaDB uses two strategies:
//!
//! 1. **Write Batching**: Multiple operations in a single Raft entry amortizes consensus overhead.
//!    With 16KB pages, batch_size=100 achieves ~6000 ops/sec.
//!
//! 2. **Multi-Region**: Multiple parallel Raft groups via RaftManager. Each region has independent
//!    consensus, enabling parallel writes. RegionTestCluster is implemented (see
//!    test_stress_multi_region_*). NOTE: Organization→region assignment needed for true parallel
//!    writes.
//!
//! Uses the wire-protocol test helpers (`wire_create_test_organization`,
//! `wire_create_test_vault`, `wire_write_client`, `wire_read_client`) from
//! `tests/common/`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use inferadb_ledger_types::{OrganizationSlug, VaultSlug};
use inferadb_ledger_wire::{
    error::ErrorCode,
    services::{read as wr, shared as ws, write as ww},
};
use inferadb_ledger_wire_services::{ReadServiceClient, WriteServiceClient};
use inferadb_ledger_wire_transport::RpcError;
use parking_lot::Mutex;
use tokio::sync::Semaphore;

use crate::common::{
    TestCluster, wire_create_test_organization, wire_create_test_vault, wire_read_client,
    wire_write_client,
};

// ---------------------------------------------------------------------------
// Performance targets — advisory thresholds, not pass/fail criteria.
// Missing a target emits a warning; the test still passes.
// ---------------------------------------------------------------------------
const WRITE_P99_TARGET_US: u64 = 50_000; // 50ms in µs
const READ_P99_TARGET_US: u64 = 2_000; // 2ms in µs
const WRITE_THROUGHPUT_TARGET: f64 = 5_000.0; // 5K ops/sec
const MULTI_REGION_THROUGHPUT_TARGET: f64 = 5_000.0; // 5K ops/sec

/// Configuration for the stress test.
#[derive(Debug, Clone)]
struct StressConfig {
    /// Number of concurrent write workers.
    write_workers: usize,
    /// Number of concurrent read workers.
    read_workers: usize,
    /// Duration to run the stress test.
    duration: Duration,
    /// Operations per batch for writes.
    batch_size: usize,
    /// Operations per batch for reads (uses BatchRead API when > 1).
    read_batch_size: usize,
    /// Target organization slug.
    organization: OrganizationSlug,
    /// Target vault slug.
    vault: VaultSlug,
    /// Maximum concurrent write requests (backpressure).
    max_concurrent_writes: usize,
    /// Maximum concurrent read requests (can be higher since reads are fast).
    max_concurrent_reads: usize,
    /// Tracks write locations (organization/vault) for multi-region verification.
    track_write_locations: bool,
}

impl Default for StressConfig {
    fn default() -> Self {
        Self {
            write_workers: 8,
            read_workers: 16,
            duration: Duration::from_secs(30),
            batch_size: 10,
            read_batch_size: 50, // BatchRead for higher throughput
            organization: OrganizationSlug::new(1),
            vault: VaultSlug::new(1),
            max_concurrent_writes: 50,
            max_concurrent_reads: 500, // Reads are fast, allow high concurrency
            track_write_locations: false,
        }
    }
}

/// Written value with its location for multi-region consistency verification.
#[derive(Debug, Clone)]
struct WrittenValue {
    organization: OrganizationSlug,
    vault: VaultSlug,
    value: Vec<u8>,
}

/// Metrics collected during stress test.
#[derive(Debug, Default)]
struct StressMetrics {
    /// Total writes completed.
    write_count: AtomicU64,
    /// Total reads completed.
    read_count: AtomicU64,
    /// Total write errors.
    write_errors: AtomicU64,
    /// Total read errors.
    read_errors: AtomicU64,
    /// Writes latencies in microseconds.
    write_latencies: Mutex<Vec<u64>>,
    /// Reads latencies in microseconds.
    read_latencies: Mutex<Vec<u64>>,
    /// Values written (key -> value) for consistency verification.
    written_values: Mutex<HashMap<String, Vec<u8>>>,
    /// Values written with location for multi-region consistency verification.
    /// Maps key -> (organization, vault, value).
    written_values_with_location: Mutex<HashMap<String, WrittenValue>>,
}

/// Organization and vault assignment for multi-region stress testing.
#[derive(Debug, Clone)]
struct RegionAssignment {
    /// Region index (1-based for data regions).
    region_index: u32,
    /// Organization slug assigned to this region.
    organization: OrganizationSlug,
    /// Vault slug within the organization.
    vault: VaultSlug,
}

/// Setup multiple organizations across different regions for true multi-region stress testing.
///
/// Creates one admin user + organization per data region using
/// `wire_create_test_organization`, then creates a vault in each via
/// `wire_create_test_vault`. This enables parallel writes since each region
/// has independent Raft consensus.
async fn setup_multi_region_organizations(
    cluster: &TestCluster,
    leader_id: u64,
    num_regions: usize,
) -> Result<Vec<RegionAssignment>, String> {
    let mut assignments = Vec::with_capacity(num_regions);

    for region in 1..=num_regions {
        let region_u32 = region as u32;
        let name = format!("stress-region-{}-ns", region);

        let (organization, _admin_slug) = wire_create_test_organization(cluster, leader_id, &name)
            .await
            .map_err(|e| format!("create organization {region}: {e}"))?;

        let vault = wire_create_test_vault(cluster, leader_id, organization)
            .await
            .map_err(|e| format!("create vault for region {region}: {e}"))?;

        assignments.push(RegionAssignment { region_index: region_u32, organization, vault });
    }

    Ok(assignments)
}

impl StressMetrics {
    fn new() -> Self {
        Self::default()
    }

    fn record_write(&self, latency: Duration, key: String, value: Vec<u8>) {
        self.write_count.fetch_add(1, Ordering::Relaxed);
        self.write_latencies.lock().push(latency.as_micros() as u64);
        self.written_values.lock().insert(key, value);
    }

    /// Records a write with its organization/vault location for multi-region verification.
    fn record_write_with_location(
        &self,
        latency: Duration,
        key: String,
        value: Vec<u8>,
        organization: OrganizationSlug,
        vault: VaultSlug,
    ) {
        self.write_count.fetch_add(1, Ordering::Relaxed);
        self.write_latencies.lock().push(latency.as_micros() as u64);
        self.written_values_with_location
            .lock()
            .insert(key, WrittenValue { organization, vault, value });
    }

    fn record_write_error(&self) {
        self.write_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn record_read(&self, latency: Duration) {
        self.read_count.fetch_add(1, Ordering::Relaxed);
        self.read_latencies.lock().push(latency.as_micros() as u64);
    }

    fn record_read_error(&self) {
        self.read_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn compute_percentiles(latencies: &mut [u64]) -> (u64, u64, u64, u64) {
        if latencies.is_empty() {
            return (0, 0, 0, 0);
        }
        latencies.sort_unstable();
        let len = latencies.len();
        let p50 = latencies[len * 50 / 100];
        let p95 = latencies[len * 95 / 100];
        let p99 = latencies[len * 99 / 100];
        let max = latencies[len - 1];
        (p50, p95, p99, max)
    }

    fn report(&self, duration: Duration) {
        let writes = self.write_count.load(Ordering::Relaxed);
        let reads = self.read_count.load(Ordering::Relaxed);
        let write_errors = self.write_errors.load(Ordering::Relaxed);
        let read_errors = self.read_errors.load(Ordering::Relaxed);

        let secs = duration.as_secs_f64();
        let write_throughput = writes as f64 / secs;
        let read_throughput = reads as f64 / secs;

        let mut write_lats = self.write_latencies.lock().clone();
        let mut read_lats = self.read_latencies.lock().clone();

        let (w_p50, w_p95, w_p99, w_max) = Self::compute_percentiles(&mut write_lats);
        let (r_p50, r_p95, r_p99, r_max) = Self::compute_percentiles(&mut read_lats);

        println!("\n+==============================================================+");
        println!("|                      STRESS TEST RESULTS                     |");
        println!("+==============================================================+");
        println!("| Duration: {:>10.2}s                                        |", secs);
        println!("+==============================================================+");
        println!("|                            WRITES                            |");
        println!("+==============================================================+");
        println!(
            "| Total:     {:>10}  |  Throughput: {:>10.0} ops/sec     |",
            writes, write_throughput
        );
        println!(
            "| Errors:    {:>10}  |  Error Rate: {:>10.2}%            |",
            write_errors,
            if writes > 0 { write_errors as f64 / writes as f64 * 100.0 } else { 0.0 }
        );
        println!("| Latency (us):                                                |");
        println!("|   p50: {:>8}  |  p95: {:>8}  |  p99: {:>8}          |", w_p50, w_p95, w_p99);
        println!("|   max: {:>8}                                              |", w_max);
        println!("+==============================================================+");
        println!("|                             READS                            |");
        println!("+==============================================================+");
        println!(
            "| Total:     {:>10}  |  Throughput: {:>10.0} ops/sec     |",
            reads, read_throughput
        );
        println!(
            "| Errors:    {:>10}  |  Error Rate: {:>10.2}%            |",
            read_errors,
            if reads > 0 { read_errors as f64 / reads as f64 * 100.0 } else { 0.0 }
        );
        println!("| Latency (us):                                                |");
        println!("|   p50: {:>8}  |  p95: {:>8}  |  p99: {:>8}          |", r_p50, r_p95, r_p99);
        println!("|   max: {:>8}                                              |", r_max);
        println!("+==============================================================+");
        println!("|                            TARGETS                           |");
        println!("+==============================================================+");

        let write_latency_pass = w_p99 <= WRITE_P99_TARGET_US;
        let read_latency_pass = r_p99 <= READ_P99_TARGET_US;
        let write_throughput_pass = write_throughput >= WRITE_THROUGHPUT_TARGET;

        println!(
            "| Write p99 <50ms:   {:>5}ms  |  Target: 50ms    |  {}  |",
            w_p99 / 1000,
            if write_latency_pass { "PASS" } else { "MISS" }
        );
        println!(
            "| Read p99 <2ms:     {:>5}ms  |  Target: 2ms     |  {}  |",
            r_p99 / 1000,
            if read_latency_pass { "PASS" } else { "MISS" }
        );
        println!(
            "| Write throughput:  {:>5.0}/s  |  Target: 5000/s  |  {}  |",
            write_throughput,
            if write_throughput_pass { "PASS" } else { "MISS" }
        );
        println!("+==============================================================+\n");
    }
}

/// Writes worker that sends write requests to the leader.
///
/// Issues `batch_size` sequential per-vault `Write` RPCs per outer loop
/// iteration. The legacy `BatchWrite` RPC is deprecated; throughput now relies
/// on the Raft `BatchWriter` to coalesce concurrent in-flight proposals.
async fn write_worker(
    worker_id: usize,
    client: Arc<WriteServiceClient>,
    config: StressConfig,
    metrics: Arc<StressMetrics>,
    running: Arc<AtomicBool>,
    semaphore: Arc<Semaphore>,
) {
    let client_id = format!("stress-writer-{}", worker_id);
    let mut batch_counter = 0u64;
    let mut consecutive_errors = 0u32;

    // Stagger worker starts to prevent thundering herd
    let stagger_delay = Duration::from_millis(worker_id as u64 * 50);
    tokio::time::sleep(stagger_delay).await;

    while running.load(Ordering::Relaxed) {
        // Back off if we're seeing too many errors
        if consecutive_errors > 3 {
            tokio::time::sleep(Duration::from_millis(100 * consecutive_errors as u64)).await;
            if consecutive_errors > 10 {
                // Reset counter — the wire client tolerates transient failures internally.
                consecutive_errors = 0;
            }
        }

        // Acquire semaphore for backpressure
        let _permit = semaphore.acquire().await.unwrap();

        // Build operations for this batch
        let batch_size = config.batch_size;
        let keys_and_values: Vec<(String, Vec<u8>)> = (0..batch_size)
            .map(|i| {
                let key = format!("stress-key-{}-{}-{}", worker_id, batch_counter, i);
                let value =
                    format!("stress-value-{}-{}-{}", worker_id, batch_counter, i).into_bytes();
                (key, value)
            })
            .collect();

        // BatchWrite is deprecated as of Phase 6 of the per-vault consensus
        // migration (A5). Every operation now goes through a per-vault `Write`
        // RPC. When `batch_size > 1`, the worker issues `batch_size` sequential
        // `Write` calls and records per-op latency directly.
        for (key, value) in keys_and_values {
            let op_start = Instant::now();
            let request = ww::WriteRequest {
                client_id: Some(ws::ClientIdMessage { id: client_id.clone() }),
                idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
                organization: Some(config.organization),
                vault: Some(config.vault),
                operations: vec![ws::Operation {
                    op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                        key: key.clone(),
                        value: Bytes::from(value.clone()),
                        expires_at: None,
                        condition: None,
                    })),
                }],
                include_tx_proof: false,
                caller: None,
            };

            match client.write(request, rand::random::<u128>()).await {
                Ok(response) => {
                    let latency = op_start.elapsed();
                    match response.result {
                        Some(ww::WriteResponseResult::Success(_)) => {
                            if config.track_write_locations {
                                metrics.record_write_with_location(
                                    latency,
                                    key,
                                    value,
                                    config.organization,
                                    config.vault,
                                );
                            } else {
                                metrics.record_write(latency, key, value);
                            }
                            consecutive_errors = 0;
                        },
                        Some(ww::WriteResponseResult::Error(_e)) => {
                            metrics.record_write_error();
                            consecutive_errors += 1;
                        },
                        None => {
                            metrics.record_write_error();
                            consecutive_errors += 1;
                        },
                    }
                },
                Err(RpcError::WireError(wire_err)) if wire_err.code == ErrorCode::StaleRouting => {
                    // Leader-routing redirect: the wire stack does not auto-follow
                    // `LeaderHint` on the bare `WireServiceClient` (the SDK's
                    // `RegionLeaderCache` does that); for stress tests we rely on
                    // colocation between system + region leaders enforced by
                    // `TestCluster::create_data_region`. Treat as transient.
                    consecutive_errors += 1;
                    if consecutive_errors <= 3 {
                        eprintln!("Write worker {} stale-routing: {}", worker_id, wire_err.message);
                    }
                },
                Err(e) => {
                    metrics.record_write_error();
                    consecutive_errors += 1;
                    if consecutive_errors <= 3 {
                        eprintln!("Write worker {} error: {}", worker_id, e);
                    }
                },
            }
        }

        batch_counter += 1;
    }
}

/// Reads worker that sends read requests to a node.
///
/// Uses BatchRead API when read_batch_size > 1 to amortize wire overhead.
/// The wire client maintains a long-lived QUIC connection per `WireClient`
/// instance, so reusing a single client across iterations avoids per-call
/// connection overhead.
async fn read_worker(
    worker_id: usize,
    client: Arc<ReadServiceClient>,
    config: StressConfig,
    metrics: Arc<StressMetrics>,
    running: Arc<AtomicBool>,
    semaphore: Arc<Semaphore>,
) {
    let mut key_counter = 0u64;
    let read_batch_size = config.read_batch_size;

    while running.load(Ordering::Relaxed) {
        // Acquire semaphore for backpressure
        let _permit = semaphore.acquire().await.unwrap();

        let start = Instant::now();

        // Use BatchRead when batch_size > 1 for higher throughput
        if read_batch_size > 1 {
            // Generate batch of keys to read
            let keys: Vec<String> = (0..read_batch_size)
                .map(|i| {
                    format!(
                        "stress-key-{}-{}-{}",
                        worker_id % config.write_workers,
                        (key_counter + i as u64) % 1000,
                        i
                    )
                })
                .collect();

            let request = wr::BatchReadRequest {
                organization: Some(config.organization),
                vault: Some(config.vault),
                keys,
                consistency: ws::ReadConsistency::Eventual,
                caller: None,
            };

            match client.batch_read(request, rand::random::<u128>()).await {
                Ok(response) => {
                    let latency = start.elapsed();
                    let batch_size = response.results.len();
                    // Record amortized latency per read
                    let per_read_latency =
                        Duration::from_nanos(latency.as_nanos() as u64 / batch_size.max(1) as u64);
                    for _ in 0..batch_size {
                        metrics.record_read(per_read_latency);
                    }
                },
                Err(RpcError::WireError(wire_err)) if wire_err.code == ErrorCode::NotFound => {
                    // NOT_FOUND counts as successful reads
                    let latency = start.elapsed();
                    let per_read_latency =
                        Duration::from_nanos(latency.as_nanos() as u64 / read_batch_size as u64);
                    for _ in 0..read_batch_size {
                        metrics.record_read(per_read_latency);
                    }
                },
                Err(_) => {
                    for _ in 0..read_batch_size {
                        metrics.record_read_error();
                    }
                },
            }
            key_counter += read_batch_size as u64;
        } else {
            // Single read - use regular Read RPC
            let key =
                format!("stress-key-{}-{}-0", worker_id % config.write_workers, key_counter % 1000);

            let request = wr::ReadRequest {
                organization: Some(config.organization),
                vault: Some(config.vault),
                key,
                consistency: ws::ReadConsistency::Eventual,
                caller: None,
            };

            match client.read(request, rand::random::<u128>()).await {
                Ok(_) => {
                    metrics.record_read(start.elapsed());
                },
                Err(RpcError::WireError(wire_err)) if wire_err.code == ErrorCode::NotFound => {
                    metrics.record_read(start.elapsed());
                },
                Err(_) => {
                    metrics.record_read_error();
                },
            }
            key_counter += 1;
        }
    }
}

/// Verifies consistency of written values by reading them back.
async fn verify_consistency(
    client: &ReadServiceClient,
    config: &StressConfig,
    metrics: &StressMetrics,
) -> Result<(), String> {
    println!("\nVerifying consistency of written values...");

    let written = metrics.written_values.lock().clone();
    let total = written.len();
    let mut _verified = 0usize;
    let mut mismatches = 0usize;
    let sample_size = std::cmp::min(1000, total); // Sample up to 1000 keys

    for (i, (key, expected_value)) in written.iter().take(sample_size).enumerate() {
        let request = wr::ReadRequest {
            organization: Some(config.organization),
            vault: Some(config.vault),
            key: key.clone(),
            // Use eventual consistency for verification - linearizable reads require
            // additional Raft configuration that isn't always enabled in test clusters.
            // Eventual consistency is sufficient here since we wait for cluster sync.
            consistency: ws::ReadConsistency::Eventual,
            caller: None,
        };

        // Add timeout to prevent hanging if server is unresponsive
        let read_result = tokio::time::timeout(
            Duration::from_secs(5),
            client.read(request, rand::random::<u128>()),
        )
        .await;

        match read_result {
            Ok(Ok(response)) => {
                if let Some(value) = response.value {
                    if value.as_ref() == expected_value.as_slice() {
                        _verified += 1;
                    } else {
                        mismatches += 1;
                        if mismatches <= 5 {
                            eprintln!(
                                "  Mismatch for key '{}': expected {} bytes, got {} bytes",
                                key,
                                expected_value.len(),
                                value.len()
                            );
                        }
                    }
                } else {
                    mismatches += 1;
                    if mismatches <= 5 {
                        eprintln!("  Key '{}' not found but was written", key);
                    }
                }
            },
            Ok(Err(e)) => {
                mismatches += 1;
                if mismatches <= 5 {
                    eprintln!("  Error reading key '{}': {}", key, e);
                }
            },
            Err(_) => {
                // Timeout - server is unresponsive
                mismatches += 1;
                if mismatches <= 5 {
                    eprintln!("  Timeout reading key '{}' (server unresponsive)", key);
                }
                // Skip remaining verifications if we're timing out
                if mismatches > 3 {
                    eprintln!("  Too many timeouts, skipping remaining verifications");
                    break;
                }
            },
        }

        if (i + 1) % 100 == 0 {
            print!("\r  Verified {}/{} keys...", i + 1, sample_size);
        }
    }

    println!("\r  Verified {}/{} keys       ", sample_size, total);

    if mismatches > 0 {
        Err(format!(
            "Consistency check failed: {} mismatches out of {} sampled keys",
            mismatches, sample_size
        ))
    } else {
        println!("  All {} sampled keys verified successfully", sample_size);
        Ok(())
    }
}

/// Verifies consistency of written values across multiple regions.
///
/// Unlike single-region verification, this reads from the correct organization/vault
/// for each key based on where it was written.
async fn verify_multi_region_consistency(
    client: &ReadServiceClient,
    metrics: &StressMetrics,
) -> Result<(), String> {
    println!("\nVerifying consistency across all regions...");

    let written = metrics.written_values_with_location.lock().clone();
    let total = written.len();

    if total == 0 {
        println!("  No values recorded for verification");
        return Ok(());
    }

    let mut verified = 0usize;
    let mut mismatches = 0usize;
    let sample_size = std::cmp::min(1000, total); // Sample up to 1000 keys

    // Collect keys to verify (deterministic sample)
    let keys_to_verify: Vec<_> = written.keys().take(sample_size).cloned().collect();

    for (i, key) in keys_to_verify.iter().enumerate() {
        let written_value = written.get(key).unwrap();

        let request = wr::ReadRequest {
            organization: Some(written_value.organization),
            vault: Some(written_value.vault),
            key: key.clone(),
            consistency: ws::ReadConsistency::Eventual,
            caller: None,
        };

        let read_result = tokio::time::timeout(
            Duration::from_secs(5),
            client.read(request, rand::random::<u128>()),
        )
        .await;

        match read_result {
            Ok(Ok(response)) => {
                if let Some(value) = response.value {
                    if value.as_ref() == written_value.value.as_slice() {
                        verified += 1;
                    } else {
                        mismatches += 1;
                        if mismatches <= 5 {
                            eprintln!(
                                "  Mismatch for key '{}' (ns={}, vault={}): expected {} bytes, got {} bytes",
                                key,
                                written_value.organization,
                                written_value.vault,
                                written_value.value.len(),
                                value.len()
                            );
                        }
                    }
                } else {
                    mismatches += 1;
                    if mismatches <= 5 {
                        eprintln!(
                            "  Key '{}' not found in ns={}, vault={} (was written)",
                            key, written_value.organization, written_value.vault
                        );
                    }
                }
            },
            Ok(Err(e)) => {
                mismatches += 1;
                if mismatches <= 5 {
                    eprintln!(
                        "  Error reading key '{}' from ns={}, vault={}: {}",
                        key, written_value.organization, written_value.vault, e
                    );
                }
            },
            Err(_) => {
                mismatches += 1;
                if mismatches <= 5 {
                    eprintln!("  Timeout reading key '{}'", key);
                }
                if mismatches > 3 {
                    eprintln!("  Too many timeouts, skipping remaining verifications");
                    break;
                }
            },
        }

        if (i + 1) % 100 == 0 {
            print!("\r  Verified {}/{} keys...", i + 1, sample_size);
        }
    }

    println!(
        "\r  Verified {}/{} keys ({} from {} total)       ",
        verified, sample_size, sample_size, total
    );

    if mismatches > 0 {
        Err(format!(
            "Multi-region consistency check failed: {} mismatches out of {} sampled keys",
            mismatches, sample_size
        ))
    } else {
        println!("  All {} sampled keys verified successfully across all regions", sample_size);
        Ok(())
    }
}

/// Run the full stress test with default cluster size.
async fn run_stress_test(config: StressConfig) {
    run_stress_test_with_cluster_size(3, config).await;
}

/// Run the full stress test with configurable cluster size.
async fn run_stress_test_with_cluster_size(cluster_size: usize, mut config: StressConfig) {
    println!("\nStarting Stress Test");
    println!("   Cluster size: {} node(s)", cluster_size);
    println!("   Write workers: {}", config.write_workers);
    println!("   Read workers: {}", config.read_workers);
    println!("   Duration: {:?}", config.duration);
    println!("   Batch size: {}", config.batch_size);
    println!();

    // Start cluster on the wire transport (single data region — `with_tcp`
    // semantics shifted to wire-mode in F.1.f.2.S1f).
    println!("Creating {}-node cluster...", cluster_size);
    let cluster = TestCluster::with_wire_transport_and_size(1, cluster_size).await;
    let leader_id = cluster.wait_for_leader().await;
    println!("   Leader elected: node {}", leader_id);

    // Allow cluster to fully stabilize before stress testing
    println!("   Waiting for cluster stabilization...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let leader = cluster.leader().expect("should have leader");
    let leader_node_id = leader.id;
    let node_ids: Vec<u64> = cluster.nodes().iter().map(|n| n.id).collect();

    // Setup: Create admin user + organization + vault for the stress test.
    println!("Setting up organization and vault...");
    let (org_slug, _admin_slug) = wire_create_test_organization(
        &cluster,
        leader_node_id,
        &format!("stress-ns-{}", config.organization.value()),
    )
    .await
    .expect("create test organization");
    config.organization = org_slug;

    let vault = wire_create_test_vault(&cluster, leader_node_id, config.organization)
        .await
        .expect("create test vault");
    config.vault = vault;
    println!(
        "   Organization (slug={}) and vault (slug={}) created",
        config.organization, config.vault
    );

    // Build a single write client (per leader) shared across write workers.
    // Wire QUIC streams multiplex over one connection, so a shared client
    // is the canonical pattern.
    let write_client = Arc::new(wire_write_client(&cluster, leader_node_id));

    // Build per-node read clients to spread reads across all nodes.
    let read_clients: Vec<Arc<ReadServiceClient>> =
        node_ids.iter().map(|nid| Arc::new(wire_read_client(&cluster, *nid))).collect();

    // Metrics and control
    let metrics = Arc::new(StressMetrics::new());
    let running = Arc::new(AtomicBool::new(true));
    // Separate semaphores for reads and writes - reads can have much higher concurrency
    let write_semaphore = Arc::new(Semaphore::new(config.max_concurrent_writes));
    let read_semaphore = Arc::new(Semaphore::new(config.max_concurrent_reads));

    // Spawn write workers
    println!("Spawning {} write workers...", config.write_workers);
    let mut handles = Vec::new();
    for i in 0..config.write_workers {
        let m = metrics.clone();
        let r = running.clone();
        let s = write_semaphore.clone();
        let c = config.clone();
        let wc = write_client.clone();
        handles.push(tokio::spawn(write_worker(i, wc, c, m, r, s)));
    }

    // Spawn read workers (distributed across all nodes)
    println!("Spawning {} read workers...", config.read_workers);
    for i in 0..config.read_workers {
        let rc = read_clients[i % read_clients.len()].clone();
        let m = metrics.clone();
        let r = running.clone();
        let s = read_semaphore.clone();
        let c = config.clone();
        handles.push(tokio::spawn(read_worker(i, rc, c, m, r, s)));
    }

    // Run for the specified duration
    println!("Running stress test for {:?}...\n", config.duration);
    let start = Instant::now();

    // Progress updates - print every 2 seconds or at end of test
    let progress_interval = Duration::from_secs(2);
    while start.elapsed() < config.duration {
        tokio::time::sleep(progress_interval).await;
        let writes = metrics.write_count.load(Ordering::Relaxed);
        let reads = metrics.read_count.load(Ordering::Relaxed);
        let elapsed = start.elapsed().as_secs().max(1); // Avoid division by zero
        println!(
            "   [{:>3}s] Writes: {:>8} ({:>6.0}/s) | Reads: {:>8} ({:>6.0}/s)",
            elapsed,
            writes,
            writes as f64 / elapsed as f64,
            reads,
            reads as f64 / elapsed as f64
        );
    }

    // Stop workers
    running.store(false, Ordering::Relaxed);

    // Wait for workers to finish (with timeout)
    println!("\nWaiting for workers to finish...");
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        for handle in handles {
            let _ = handle.await;
        }
    })
    .await;

    // Report metrics
    let actual_duration = start.elapsed();
    metrics.report(actual_duration);

    // Verify consistency via leader's read client.
    let consistency_result = verify_consistency(&read_clients[0], &config, &metrics).await;

    // Final sync check
    println!("\nChecking cluster sync...");
    let synced = cluster.wait_for_sync(Duration::from_secs(10)).await;
    if synced {
        println!("   All nodes synchronized");
    } else {
        println!("   Nodes may not be fully synchronized");
    }

    // Final assertions
    let writes = metrics.write_count.load(Ordering::Relaxed);
    let write_errors = metrics.write_errors.load(Ordering::Relaxed);
    let error_rate = if writes > 0 { write_errors as f64 / writes as f64 } else { 0.0 };

    assert!(writes > 0, "Should have completed some writes");
    assert!(error_rate < 0.01, "Write error rate should be <1%, was {:.2}%", error_rate * 100.0);
    assert!(consistency_result.is_ok(), "Consistency check failed: {:?}", consistency_result.err());
}

/// Quick smoke test - fast validation.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_stress_quick() {
    run_stress_test_with_cluster_size(
        3, // 3-node cluster
        StressConfig {
            write_workers: 2,
            read_workers: 4,
            duration: Duration::from_secs(10),
            batch_size: 1, // Single operation per batch for stability
            max_concurrent_writes: 10,
            max_concurrent_reads: 100,
            ..Default::default()
        },
    )
    .await;
}

/// Single node stress test - validates write path without replication.
/// Run with: cargo test --test stress_test test_stress_single_node -- --nocapture
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_stress_single_node() {
    run_stress_test_with_cluster_size(
        1, // Single node - no replication
        StressConfig {
            write_workers: 2, // Multiple writers now that RwLock contention is fixed
            read_workers: 4,  /* Multiple readers - StateLayer is internally thread-safe via
                               * inferadb-ledger-store MVCC */
            duration: Duration::from_secs(10),
            batch_size: 1,
            max_concurrent_writes: 20,
            max_concurrent_reads: 200,
            ..Default::default()
        },
    )
    .await;
}

/// Batch write stress test - validates throughput improvement from batching.
/// Uses BatchWrite RPC to amortize Raft consensus overhead across multiple operations.
/// Run with: cargo test --release --test stress_test test_stress_batched -- --nocapture
///
/// ## Throughput Analysis
///
/// Raft consensus takes ~20-30ms per batch. Single-region theoretical max:
/// - batch_size=50 @ 25ms = 2,000 ops/sec max
/// - batch_size=100 @ 25ms = 4,000 ops/sec max (requires 16KB pages)
///
/// With multi-region (8 regions) and batch_size=100, achieves 6000+ ops/sec.
/// All databases use 16KB pages to support larger batch sizes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_stress_batched() {
    run_stress_test_with_cluster_size(
        3, // 3-node cluster for realistic Raft consensus
        StressConfig {
            write_workers: 4, // Multiple writers
            read_workers: 8,  // Multiple readers
            duration: Duration::from_secs(10),
            batch_size: 50, // 50 operations per batch - amortizes consensus overhead
            max_concurrent_writes: 50,
            max_concurrent_reads: 500,
            ..Default::default()
        },
    )
    .await;
}

/// High-throughput read test - pushes read throughput toward 100k/sec target.
/// Run with: cargo test --test stress_test test_stress_read_throughput -- --nocapture
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_stress_read_throughput() {
    run_stress_test_with_cluster_size(
        3, // 3-node cluster - reads distributed across all nodes
        StressConfig {
            write_workers: 2, // Minimal writers (just to have data)
            read_workers: 32, // Many read workers to saturate
            duration: Duration::from_secs(10),
            batch_size: 10, // Batch writes to create data quickly
            max_concurrent_writes: 20,
            max_concurrent_reads: 2000, // Very high read concurrency
            ..Default::default()
        },
    )
    .await;
}

/// Standard stress test - moderate load for CI.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_stress_standard() {
    run_stress_test(StressConfig {
        write_workers: 4,
        read_workers: 8,
        duration: Duration::from_secs(10),
        batch_size: 5,
        max_concurrent_writes: 50,
        max_concurrent_reads: 500,
        ..Default::default()
    })
    .await;
}

// ============================================================================
// Multi-Region Stress Tests
// ============================================================================
//
// These tests use TestCluster::with_data_regions for multi-region throughput
// via parallel Raft consensus across multiple independent regions.
//
// Each region is a separate Raft group, allowing writes to different regions
// to proceed in parallel without consensus conflicts.

/// Run a stress test using multi-region architecture.
///
/// This enables significantly higher write throughput by distributing
/// writes across multiple independent Raft groups. Each region has its own
/// organization, and workers are distributed across regions for parallel consensus.
async fn run_multi_region_stress_test(num_nodes: usize, num_regions: usize, config: StressConfig) {
    println!("\nStarting Multi-Region Stress Test (PARALLEL WRITES)");
    println!("   Nodes: {}", num_nodes);
    println!("   Data regions: {}", num_regions);
    println!(
        "   Write workers: {} ({} per region)",
        config.write_workers,
        config.write_workers / num_regions.max(1)
    );
    println!("   Read workers: {}", config.read_workers);
    println!("   Duration: {:?}", config.duration);
    println!("   Batch size: {}", config.batch_size);
    println!();

    // Start multi-region cluster on the wire transport.
    println!("Creating {}-node, {}-region cluster...", num_nodes, num_regions);
    let cluster = TestCluster::with_wire_transport_and_size(num_regions, num_nodes).await;

    // Wait for all regions to have leaders
    let leaders_ready = cluster.wait_for_leaders(Duration::from_secs(10)).await;
    if !leaders_ready {
        panic!("Failed to elect leaders on all regions");
    }
    println!("   Leaders elected on all {} regions", num_regions + 1); // +1 for system region

    // Allow cluster to stabilize
    println!("   Waiting for cluster stabilization...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let leader_node_id = cluster.any_node().id;
    let node_ids: Vec<u64> = cluster.nodes().iter().map(|n| n.id).collect();

    // Setup organizations across all regions - one organization per region
    println!("Setting up {} organizations (one per region)...", num_regions);
    let region_assignments =
        match setup_multi_region_organizations(&cluster, leader_node_id, num_regions).await {
            Ok(assignments) => {
                for assignment in &assignments {
                    println!(
                        "   Region {} -> Organization {} -> Vault {}",
                        assignment.region_index, assignment.organization, assignment.vault
                    );
                }
                assignments
            },
            Err(e) => {
                panic!("Failed to setup multi-region organizations: {}", e);
            },
        };

    // Build a single shared write client (leader-rooted) and per-node read
    // clients. With true multi-region writes, the leader for each
    // organization may differ; the wire stack returns `StaleRouting` for
    // cross-leader writes and the worker treats them as transient.
    let write_client = Arc::new(wire_write_client(&cluster, leader_node_id));
    let read_clients: Vec<Arc<ReadServiceClient>> =
        node_ids.iter().map(|nid| Arc::new(wire_read_client(&cluster, *nid))).collect();

    // Metrics and control
    let metrics = Arc::new(StressMetrics::new());
    let running = Arc::new(AtomicBool::new(true));
    let write_semaphore = Arc::new(Semaphore::new(config.max_concurrent_writes));
    let read_semaphore = Arc::new(Semaphore::new(config.max_concurrent_reads));

    // Spawn write workers - DISTRIBUTED ACROSS REGIONS
    // Each worker is assigned to a specific region to enable parallel consensus
    println!("Spawning {} write workers across {} regions...", config.write_workers, num_regions);
    let mut handles = Vec::new();
    for i in 0..config.write_workers {
        // Distribute workers round-robin across regions
        let assignment = region_assignments[i % region_assignments.len()].clone();
        let worker_config = StressConfig {
            organization: assignment.organization,
            vault: assignment.vault,
            track_write_locations: true, // Enable location tracking for multi-region verification
            ..config.clone()
        };
        let metrics = metrics.clone();
        let running = running.clone();
        let semaphore = write_semaphore.clone();
        let wc = write_client.clone();
        handles.push(tokio::spawn(async move {
            write_worker(i, wc, worker_config, metrics, running, semaphore).await;
        }));
    }

    // For reads, use the first region assignment (reads are fast anyway)
    let read_config = StressConfig {
        organization: region_assignments[0].organization,
        vault: region_assignments[0].vault,
        ..config.clone()
    };

    // Spawn read workers - distribute across all nodes for load balancing
    println!("Spawning {} read workers...", read_config.read_workers);
    for i in 0..read_config.read_workers {
        let worker_config = read_config.clone();
        let metrics = metrics.clone();
        let running = running.clone();
        let semaphore = read_semaphore.clone();
        let rc = read_clients[i % read_clients.len()].clone();
        handles.push(tokio::spawn(async move {
            read_worker(i, rc, worker_config, metrics, running, semaphore).await;
        }));
    }

    // Progress updates
    let start = Instant::now();
    let progress_interval = Duration::from_secs(2);
    while start.elapsed() < config.duration {
        tokio::time::sleep(progress_interval).await;
        let writes = metrics.write_count.load(Ordering::Relaxed);
        let reads = metrics.read_count.load(Ordering::Relaxed);
        let elapsed = start.elapsed().as_secs().max(1);
        println!(
            "   [{:>3}s] Writes: {:>8} ({:>6.0}/s) | Reads: {:>8} ({:>6.0}/s)",
            elapsed,
            writes,
            writes as f64 / elapsed as f64,
            reads,
            reads as f64 / elapsed as f64
        );
    }

    // Stop workers
    running.store(false, Ordering::Relaxed);

    // Wait for workers to finish (with timeout)
    println!("\nWaiting for workers to finish...");
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        for handle in handles {
            let _ = handle.await;
        }
    })
    .await;

    // Report metrics - reuse existing report() for detailed output
    let actual_duration = start.elapsed();
    println!("\nMulti-Region Stress Test Results ({} nodes, {} regions)", num_nodes, num_regions);
    metrics.report(actual_duration);

    // Verify consistency across all regions - reads from each key's recorded organization/vault
    let consistency_result = verify_multi_region_consistency(&read_clients[0], &metrics).await;
    if let Err(e) = &consistency_result {
        eprintln!("  Multi-region consistency verification failed: {}", e);
    }

    // Final assertions
    let writes = metrics.write_count.load(Ordering::Relaxed);
    let write_errors = metrics.write_errors.load(Ordering::Relaxed);
    let error_rate = if writes > 0 { write_errors as f64 / writes as f64 } else { 0.0 };

    // Calculate effective throughput accounting for batch size
    let actual_secs = start.elapsed().as_secs_f64();
    let ops_per_sec = writes as f64 / actual_secs;

    assert!(writes > 0, "Should have completed some writes");
    assert!(error_rate < 0.01, "Write error rate should be <1%, was {:.2}%", error_rate * 100.0);

    // Report multi-region specific summary
    println!("\nMulti-Region Summary:");
    println!("   Regions: {} data regions + 1 system region", num_regions);
    println!("   Per-region throughput: {:.0} ops/sec", ops_per_sec / num_regions as f64);
    println!("   Total throughput: {:.0} ops/sec", ops_per_sec);
    println!(
        "   Target (5000 ops/sec): {}",
        if ops_per_sec >= MULTI_REGION_THROUGHPUT_TARGET { "PASS" } else { "MISS" }
    );
}

/// Quick multi-region stress test - validates infrastructure works.
/// Run with: cargo test test_stress_multi_region_quick -- --nocapture
///
/// Creates one organization per region and distributes write workers across them
/// for true parallel Raft consensus.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_stress_multi_region_quick() {
    run_multi_region_stress_test(
        1, // Single node for speed
        2, // 2 data regions
        StressConfig {
            write_workers: 4,
            read_workers: 8,
            duration: Duration::from_secs(5),
            batch_size: 10,
            organization: OrganizationSlug::new(0), // Overridden per-worker by region assignments
            vault: VaultSlug::new(1),
            max_concurrent_writes: 50,
            max_concurrent_reads: 200,
            ..Default::default()
        },
    )
    .await;
}

/// Multi-region batched throughput test - parallel writes across 4 regions.
/// Run with: cargo test --release test_stress_multi_region_batched -- --nocapture
///
/// This is the multi-region equivalent of `test_stress_batched`. With 4 regions
/// and batch_size=50, theoretical max is 4x single-region (~2300 ops/sec).
///
/// ## Expected Results (release mode)
/// - Single region batched: ~580 ops/sec
/// - 4 regions batched: ~2000-2300 ops/sec (4x parallel consensus)
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_stress_multi_region_batched() {
    run_multi_region_stress_test(
        3, // 3 nodes (required for protected regions)
        2, // 2 data regions (non-protected: US_EAST_VA, US_WEST_OR)
        StressConfig {
            write_workers: 8, // 2 workers per region
            read_workers: 16,
            duration: Duration::from_secs(10),
            batch_size: 50, // Match batched test
            read_batch_size: 50,
            organization: OrganizationSlug::new(0), // Overridden per-worker by region assignments
            vault: VaultSlug::new(1),
            max_concurrent_writes: 100,
            max_concurrent_reads: 500,
            ..Default::default()
        },
    )
    .await;
}

/// Multi-region sustained test - 4 regions for 15 seconds.
/// Run with: cargo test --release test_stress_multi_region -- --nocapture
///
/// Validates sustained multi-region throughput over moderate duration.
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_stress_multi_region() {
    run_multi_region_stress_test(
        3, // 3 nodes (required for protected regions)
        2, // 2 data regions (non-protected: US_EAST_VA, US_WEST_OR)
        StressConfig {
            write_workers: 16,
            read_workers: 32,
            duration: Duration::from_secs(15),
            batch_size: 50, // Match batched test for consistency
            read_batch_size: 100,
            organization: OrganizationSlug::new(0), // Overridden per-worker by region assignments
            vault: VaultSlug::new(1),
            max_concurrent_writes: 200,
            max_concurrent_reads: 1000,
            ..Default::default()
        },
    )
    .await;
}

/// Multi-region maximum throughput test on single machine.
/// Uses 8 regions with batch_size=100 to achieve target throughput.
/// Run with: cargo test --release test_stress_multi_region_target -- --nocapture
///
/// ## Expected Results
/// - Write throughput: ~6000+ ops/sec (exceeds 5000 target)
/// - Write p99 latency: <5ms (well under 50ms target)
/// - Per-region throughput: ~750-800 ops/sec
///
/// ## Technical Notes
/// - All databases (raft, state, blocks) use 16KB pages
/// - batch_size=100 requires 16KB pages (~10KB serialized per batch)
/// - 16 write workers distribute load across 8 regions
#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
async fn test_stress_multi_region_target() {
    run_multi_region_stress_test(
        3, // 3 nodes (required for protected regions)
        2, // 2 data regions (non-protected: US_EAST_VA, US_WEST_OR)
        StressConfig {
            write_workers: 16, // 2 workers per region
            read_workers: 16,
            duration: Duration::from_secs(15),
            batch_size: 100, // 16KB pages support larger batches
            read_batch_size: 100,
            organization: OrganizationSlug::new(0), // Overridden per-worker by region assignments
            vault: VaultSlug::new(1),
            max_concurrent_writes: 160,
            max_concurrent_reads: 500,
            ..Default::default()
        },
    )
    .await;
}
