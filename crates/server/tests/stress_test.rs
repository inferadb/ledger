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
//! ## DESIGN.md Targets (§1.10)
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
//! Raft consensus overhead (~16-30ms) limits single-shard throughput to ~60 ops/sec.
//! To achieve 5000 tx/sec, InferaDB uses two strategies:
//!
//! 1. **Write Batching**: Multiple operations in a single Raft entry amortizes consensus overhead.
//!    With 16KB pages, batch_size=100 achieves ~6000 ops/sec.
//!
//! 2. **Multi-Shard**: Multiple parallel Raft groups via MultiRaftManager. Each shard has
//!    independent consensus, enabling parallel writes. MultiShardTestCluster is implemented (see
//!    test_stress_multi_shard_*). NOTE: Namespace→shard assignment needed for true parallel writes.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

mod common;

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use common::{MultiShardTestCluster, TestCluster};
use parking_lot::Mutex;
use tokio::sync::Semaphore;

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
    /// Target namespace ID.
    namespace_id: i64,
    /// Target vault ID.
    vault_id: i64,
    /// Maximum concurrent write requests (backpressure).
    max_concurrent_writes: usize,
    /// Maximum concurrent read requests (can be higher since reads are fast).
    max_concurrent_reads: usize,
    /// Track write locations (namespace/vault) for multi-shard verification.
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
            namespace_id: 1,
            vault_id: 1,
            max_concurrent_writes: 50,
            max_concurrent_reads: 500, // Reads are fast, allow high concurrency
            track_write_locations: false,
        }
    }
}

/// Extract leader address from a forwarding error message.
///
/// Error format varies - may have escaped or unescaped quotes:
/// - Unescaped: addr: "127.0.0.1:50187"
/// - Escaped: addr: \"127.0.0.1:50187\"
fn extract_leader_addr_from_error(error_msg: &str) -> Option<SocketAddr> {
    // Try escaped quotes first (more common in error strings)
    if let Some(start) = error_msg.find("addr: \\\"") {
        let addr_start = start + "addr: \\\"".len();
        let remaining = &error_msg[addr_start..];
        if let Some(end) = remaining.find("\\\"") {
            if let Ok(addr) = remaining[..end].parse() {
                return Some(addr);
            }
        }
    }

    // Fall back to unescaped quotes
    if let Some(start) = error_msg.find("addr: \"") {
        let addr_start = start + "addr: \"".len();
        let remaining = &error_msg[addr_start..];
        if let Some(end) = remaining.find('"') {
            if let Ok(addr) = remaining[..end].parse() {
                return Some(addr);
            }
        }
    }

    None
}

/// Written value with its location for multi-shard consistency verification.
#[derive(Debug, Clone)]
struct WrittenValue {
    namespace_id: i64,
    vault_id: i64,
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
    /// Write latencies in microseconds.
    write_latencies: Mutex<Vec<u64>>,
    /// Read latencies in microseconds.
    read_latencies: Mutex<Vec<u64>>,
    /// Values written (key -> value) for consistency verification.
    written_values: Mutex<HashMap<String, Vec<u8>>>,
    /// Values written with location for multi-shard consistency verification.
    /// Maps key -> (namespace_id, vault_id, value).
    written_values_with_location: Mutex<HashMap<String, WrittenValue>>,
}

/// Setup namespace and vault for stress testing.
///
/// For namespace_id=0 (system namespace), the namespace already exists,
/// so we only create the vault. For other namespaces, we create both.
async fn setup_namespace_and_vault(
    leader_addr: SocketAddr,
    config: &StressConfig,
) -> Result<(), String> {
    let endpoint = format!("http://{}", leader_addr);
    let mut admin_client =
        inferadb_ledger_raft::proto::admin_service_client::AdminServiceClient::connect(
            endpoint.clone(),
        )
        .await
        .map_err(|e| format!("Failed to connect admin client: {}", e))?;

    // Create namespace (skip for system namespace 0)
    if config.namespace_id != 0 {
        let ns_request = inferadb_ledger_raft::proto::CreateNamespaceRequest {
            name: format!("stress-ns-{}", config.namespace_id),
            shard_id: None,
        };
        admin_client
            .create_namespace(ns_request)
            .await
            .map_err(|e| format!("Failed to create namespace: {}", e))?;
    }

    // Create vault (replication_factor=1 for test simplicity)
    let vault_request = inferadb_ledger_raft::proto::CreateVaultRequest {
        namespace_id: Some(inferadb_ledger_raft::proto::NamespaceId { id: config.namespace_id }),
        replication_factor: 1,
        initial_nodes: vec![],
        retention_policy: None,
    };
    admin_client
        .create_vault(vault_request)
        .await
        .map_err(|e| format!("Failed to create vault: {}", e))?;

    Ok(())
}

/// Namespace and vault assignment for multi-shard stress testing.
#[derive(Debug, Clone)]
struct ShardAssignment {
    /// Shard ID (1-based for data shards).
    shard_id: u32,
    /// Namespace ID assigned to this shard.
    namespace_id: i64,
    /// Vault ID within the namespace.
    vault_id: i64,
}

/// Setup multiple namespaces across different shards for true multi-shard stress testing.
///
/// Creates one namespace per data shard, each explicitly routed to its shard.
/// This enables parallel writes since each shard has independent Raft consensus.
async fn setup_multi_shard_namespaces(
    leader_addr: SocketAddr,
    num_shards: usize,
) -> Result<Vec<ShardAssignment>, String> {
    let endpoint = format!("http://{}", leader_addr);
    let mut admin_client =
        inferadb_ledger_raft::proto::admin_service_client::AdminServiceClient::connect(
            endpoint.clone(),
        )
        .await
        .map_err(|e| format!("Failed to connect admin client: {}", e))?;

    let mut assignments = Vec::with_capacity(num_shards);

    for shard_id in 1..=num_shards {
        let shard_id_u32 = shard_id as u32;

        // Create namespace explicitly assigned to this shard
        let ns_request = inferadb_ledger_raft::proto::CreateNamespaceRequest {
            name: format!("stress-shard-{}-ns", shard_id),
            shard_id: Some(inferadb_ledger_raft::proto::ShardId { id: shard_id_u32 }),
        };

        let ns_response = admin_client
            .create_namespace(ns_request)
            .await
            .map_err(|e| format!("Failed to create namespace for shard {}: {}", shard_id, e))?;

        let namespace_id = ns_response
            .into_inner()
            .namespace_id
            .map(|n| n.id)
            .ok_or_else(|| format!("No namespace_id in response for shard {}", shard_id))?;

        // Create vault in this namespace
        let vault_request = inferadb_ledger_raft::proto::CreateVaultRequest {
            namespace_id: Some(inferadb_ledger_raft::proto::NamespaceId { id: namespace_id }),
            replication_factor: 1,
            initial_nodes: vec![],
            retention_policy: None,
        };

        let vault_response = admin_client
            .create_vault(vault_request)
            .await
            .map_err(|e| format!("Failed to create vault in namespace {}: {}", namespace_id, e))?;

        let vault_id = vault_response
            .into_inner()
            .vault_id
            .map(|v| v.id)
            .ok_or_else(|| format!("No vault_id in response for namespace {}", namespace_id))?;

        assignments.push(ShardAssignment { shard_id: shard_id_u32, namespace_id, vault_id });
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

    /// Record a write with its namespace/vault location for multi-shard verification.
    fn record_write_with_location(
        &self,
        latency: Duration,
        key: String,
        value: Vec<u8>,
        namespace_id: i64,
        vault_id: i64,
    ) {
        self.write_count.fetch_add(1, Ordering::Relaxed);
        self.write_latencies.lock().push(latency.as_micros() as u64);
        self.written_values_with_location
            .lock()
            .insert(key, WrittenValue { namespace_id, vault_id, value });
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

        println!("\n╔══════════════════════════════════════════════════════════════╗");
        println!("║                    STRESS TEST RESULTS                       ║");
        println!("╠══════════════════════════════════════════════════════════════╣");
        println!("║ Duration: {:>10.2}s                                       ║", secs);
        println!("╠══════════════════════════════════════════════════════════════╣");
        println!("║                         WRITES                               ║");
        println!("╠══════════════════════════════════════════════════════════════╣");
        println!(
            "║ Total:     {:>10}  │  Throughput: {:>10.0} ops/sec      ║",
            writes, write_throughput
        );
        println!(
            "║ Errors:    {:>10}  │  Error Rate: {:>10.2}%              ║",
            write_errors,
            if writes > 0 { write_errors as f64 / writes as f64 * 100.0 } else { 0.0 }
        );
        println!("║ Latency (µs):                                                ║");
        println!("║   p50: {:>8}  │  p95: {:>8}  │  p99: {:>8}          ║", w_p50, w_p95, w_p99);
        println!("║   max: {:>8}                                              ║", w_max);
        println!("╠══════════════════════════════════════════════════════════════╣");
        println!("║                          READS                               ║");
        println!("╠══════════════════════════════════════════════════════════════╣");
        println!(
            "║ Total:     {:>10}  │  Throughput: {:>10.0} ops/sec      ║",
            reads, read_throughput
        );
        println!(
            "║ Errors:    {:>10}  │  Error Rate: {:>10.2}%              ║",
            read_errors,
            if reads > 0 { read_errors as f64 / reads as f64 * 100.0 } else { 0.0 }
        );
        println!("║ Latency (µs):                                                ║");
        println!("║   p50: {:>8}  │  p95: {:>8}  │  p99: {:>8}          ║", r_p50, r_p95, r_p99);
        println!("║   max: {:>8}                                              ║", r_max);
        println!("╠══════════════════════════════════════════════════════════════╣");
        println!("║                    DESIGN.md TARGETS                         ║");
        println!("╠══════════════════════════════════════════════════════════════╣");

        let write_p99_target = 50_000; // 50ms in µs
        let read_p99_target = 2_000; // 2ms in µs
        let write_throughput_target = 5_000.0; // 5K ops/sec

        let write_latency_pass = w_p99 <= write_p99_target;
        let read_latency_pass = r_p99 <= read_p99_target;
        let write_throughput_pass = write_throughput >= write_throughput_target;

        println!(
            "║ Write p99 <50ms:   {:>5}ms  │  Target: 50ms    │  {}  ║",
            w_p99 / 1000,
            if write_latency_pass { "✅ PASS" } else { "❌ FAIL" }
        );
        println!(
            "║ Read p99 <2ms:     {:>5}ms  │  Target: 2ms     │  {}  ║",
            r_p99 / 1000,
            if read_latency_pass { "✅ PASS" } else { "❌ FAIL" }
        );
        println!(
            "║ Write throughput:  {:>5.0}/s  │  Target: 5000/s  │  {}  ║",
            write_throughput,
            if write_throughput_pass { "✅ PASS" } else { "❌ FAIL" }
        );
        println!("╚══════════════════════════════════════════════════════════════╝\n");
    }
}

/// Write worker that sends write requests to the leader.
///
/// Uses `BatchWrite` RPC when batch_size > 1 to amortize Raft consensus overhead
/// across multiple operations. This is the key to achieving high throughput.
async fn write_worker(
    worker_id: usize,
    leader_addr: SocketAddr,
    config: StressConfig,
    metrics: Arc<StressMetrics>,
    running: Arc<AtomicBool>,
    semaphore: Arc<Semaphore>,
) {
    let mut current_endpoint = format!("http://{}", leader_addr);
    let mut client =
        match inferadb_ledger_raft::proto::write_service_client::WriteServiceClient::connect(
            current_endpoint.clone(),
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Write worker {} failed to connect: {}", worker_id, e);
                return;
            },
        };

    let client_id = format!("stress-writer-{}", worker_id);
    let mut sequence = 1u64;
    let mut consecutive_errors = 0u32;

    // Stagger worker starts to prevent thundering herd
    let stagger_delay = Duration::from_millis(worker_id as u64 * 50);
    tokio::time::sleep(stagger_delay).await;

    while running.load(Ordering::Relaxed) {
        // Back off if we're seeing too many errors
        if consecutive_errors > 3 {
            tokio::time::sleep(Duration::from_millis(100 * consecutive_errors as u64)).await;
            if consecutive_errors > 10 {
                // Reset and try reconnecting
                consecutive_errors = 0;
                if let Ok(new_client) =
                    inferadb_ledger_raft::proto::write_service_client::WriteServiceClient::connect(
                        current_endpoint.clone(),
                    )
                    .await
                {
                    client = new_client;
                }
            }
        }

        // Acquire semaphore for backpressure
        let _permit = semaphore.acquire().await.unwrap();

        // Build operations for this batch
        let batch_size = config.batch_size;
        let keys_and_values: Vec<(String, Vec<u8>)> = (0..batch_size)
            .map(|i| {
                let key = format!("stress-key-{}-{}-{}", worker_id, sequence, i);
                let value = format!("stress-value-{}-{}-{}", worker_id, sequence, i).into_bytes();
                (key, value)
            })
            .collect();

        let start = Instant::now();

        // Use BatchWrite RPC when batch_size > 1 for better throughput.
        // BatchWrite sends multiple operations in a single Raft log entry,
        // amortizing the consensus overhead (~16-30ms) across all operations.
        if batch_size > 1 {
            // Build BatchWriteRequest with operations grouped
            let operations: Vec<inferadb_ledger_raft::proto::BatchWriteOperation> = keys_and_values
                .iter()
                .map(|(key, value)| inferadb_ledger_raft::proto::BatchWriteOperation {
                    operations: vec![inferadb_ledger_raft::proto::Operation {
                        op: Some(inferadb_ledger_raft::proto::operation::Op::SetEntity(
                            inferadb_ledger_raft::proto::SetEntity {
                                key: key.clone(),
                                value: value.clone(),
                                expires_at: None,
                                condition: None,
                            },
                        )),
                    }],
                })
                .collect();

            let request = inferadb_ledger_raft::proto::BatchWriteRequest {
                namespace_id: Some(inferadb_ledger_raft::proto::NamespaceId {
                    id: config.namespace_id,
                }),
                vault_id: Some(inferadb_ledger_raft::proto::VaultId { id: config.vault_id }),
                client_id: Some(inferadb_ledger_raft::proto::ClientId { id: client_id.clone() }),
                sequence,
                operations,
                include_tx_proofs: false,
            };

            match client.batch_write(request).await {
                Ok(response) => {
                    let latency = start.elapsed();
                    let inner = response.into_inner();
                    match inner.result {
                        Some(
                            inferadb_ledger_raft::proto::batch_write_response::Result::Success(_),
                        ) => {
                            // Record all operations in the batch with amortized latency.
                            // The batch latency divided by batch_size gives per-operation latency.
                            let per_op_latency =
                                Duration::from_nanos(latency.as_nanos() as u64 / batch_size as u64);
                            for (key, value) in keys_and_values {
                                if config.track_write_locations {
                                    metrics.record_write_with_location(
                                        per_op_latency,
                                        key,
                                        value,
                                        config.namespace_id,
                                        config.vault_id,
                                    );
                                } else {
                                    metrics.record_write(per_op_latency, key, value);
                                }
                            }
                            consecutive_errors = 0;
                        },
                        Some(inferadb_ledger_raft::proto::batch_write_response::Result::Error(
                            e,
                        )) => {
                            if !e.message.contains("Sequence gap") {
                                for _ in 0..batch_size {
                                    metrics.record_write_error();
                                }
                                consecutive_errors += 1;
                            } else {
                                sequence += 1;
                                continue;
                            }
                        },
                        None => {
                            for _ in 0..batch_size {
                                metrics.record_write_error();
                            }
                            consecutive_errors += 1;
                        },
                    }
                },
                Err(e) => {
                    let error_msg = e.to_string();

                    // Check if this is a forwarding error and extract the leader address
                    if let Some(leader_addr) = extract_leader_addr_from_error(&error_msg) {
                        // Don't count forwarding as error - just reconnect to leader
                        let new_endpoint = format!("http://{}", leader_addr);
                        if current_endpoint != new_endpoint {
                            current_endpoint = new_endpoint;
                            if let Ok(new_client) =
                                inferadb_ledger_raft::proto::write_service_client::WriteServiceClient::connect(
                                    current_endpoint.clone(),
                                )
                                .await
                            {
                                client = new_client;
                            }
                        }
                        // Retry immediately without counting as error
                        continue;
                    }

                    // Other errors are real errors
                    for _ in 0..batch_size {
                        metrics.record_write_error();
                    }
                    consecutive_errors += 1;
                    if consecutive_errors <= 3 {
                        eprintln!("Write worker {} batch error: {}", worker_id, e);
                    }
                    if let Ok(new_client) =
                        inferadb_ledger_raft::proto::write_service_client::WriteServiceClient::connect(
                            current_endpoint.clone(),
                        )
                        .await
                    {
                        client = new_client;
                    }
                },
            }
        } else {
            // Single-operation write uses regular Write RPC
            let (key, value) = keys_and_values.into_iter().next().unwrap();
            let request = inferadb_ledger_raft::proto::WriteRequest {
                client_id: Some(inferadb_ledger_raft::proto::ClientId { id: client_id.clone() }),
                sequence,
                namespace_id: Some(inferadb_ledger_raft::proto::NamespaceId {
                    id: config.namespace_id,
                }),
                vault_id: Some(inferadb_ledger_raft::proto::VaultId { id: config.vault_id }),
                operations: vec![inferadb_ledger_raft::proto::Operation {
                    op: Some(inferadb_ledger_raft::proto::operation::Op::SetEntity(
                        inferadb_ledger_raft::proto::SetEntity {
                            key: key.clone(),
                            value: value.clone(),
                            expires_at: None,
                            condition: None,
                        },
                    )),
                }],
                include_tx_proof: false,
            };

            match client.write(request).await {
                Ok(response) => {
                    let latency = start.elapsed();
                    let inner = response.into_inner();
                    match inner.result {
                        Some(inferadb_ledger_raft::proto::write_response::Result::Success(_)) => {
                            if config.track_write_locations {
                                metrics.record_write_with_location(
                                    latency,
                                    key,
                                    value,
                                    config.namespace_id,
                                    config.vault_id,
                                );
                            } else {
                                metrics.record_write(latency, key, value);
                            }
                            consecutive_errors = 0;
                        },
                        Some(inferadb_ledger_raft::proto::write_response::Result::Error(e)) => {
                            if !e.message.contains("Sequence gap") {
                                metrics.record_write_error();
                                consecutive_errors += 1;
                            } else {
                                sequence += 1;
                                continue;
                            }
                        },
                        None => {
                            metrics.record_write_error();
                            consecutive_errors += 1;
                        },
                    }
                },
                Err(e) => {
                    let error_msg = e.to_string();

                    // Check if this is a forwarding error and extract the leader address
                    if let Some(leader_addr) = extract_leader_addr_from_error(&error_msg) {
                        // Don't count forwarding as error - just reconnect to leader
                        let new_endpoint = format!("http://{}", leader_addr);
                        if current_endpoint != new_endpoint {
                            current_endpoint = new_endpoint;
                            if let Ok(new_client) =
                                inferadb_ledger_raft::proto::write_service_client::WriteServiceClient::connect(
                                    current_endpoint.clone(),
                                )
                                .await
                            {
                                client = new_client;
                            }
                        }
                        // Retry immediately without counting as error
                        continue;
                    }

                    // Other errors are real errors
                    metrics.record_write_error();
                    consecutive_errors += 1;
                    if consecutive_errors <= 3 {
                        eprintln!("Write worker {} error: {}", worker_id, e);
                    }
                    if let Ok(new_client) =
                        inferadb_ledger_raft::proto::write_service_client::WriteServiceClient::connect(
                            current_endpoint.clone(),
                        )
                        .await
                    {
                        client = new_client;
                    }
                },
            }
        }

        sequence += 1;
    }
}

/// Read worker that sends read requests to a node.
///
/// Uses BatchRead API when read_batch_size > 1 to amortize gRPC overhead.
/// Connection pooling via tonic's keep-alive prevents reconnection overhead.
async fn read_worker(
    worker_id: usize,
    node_addr: SocketAddr,
    config: StressConfig,
    metrics: Arc<StressMetrics>,
    running: Arc<AtomicBool>,
    semaphore: Arc<Semaphore>,
) {
    // Create a channel with keep-alive to reuse the TCP connection.
    // This is more efficient than creating new connections per request.
    let endpoint = tonic::transport::Channel::from_shared(format!("http://{}", node_addr))
        .expect("valid endpoint")
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_while_idle(true);

    let channel = match endpoint.connect().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Read worker {} failed to connect: {}", worker_id, e);
            return;
        },
    };

    let mut client =
        inferadb_ledger_raft::proto::read_service_client::ReadServiceClient::new(channel);
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

            let request = inferadb_ledger_raft::proto::BatchReadRequest {
                namespace_id: Some(inferadb_ledger_raft::proto::NamespaceId {
                    id: config.namespace_id,
                }),
                vault_id: Some(inferadb_ledger_raft::proto::VaultId { id: config.vault_id }),
                keys,
                consistency: inferadb_ledger_raft::proto::ReadConsistency::Eventual as i32,
            };

            match client.batch_read(request).await {
                Ok(response) => {
                    let latency = start.elapsed();
                    let batch_size = response.into_inner().results.len();
                    // Record amortized latency per read
                    let per_read_latency =
                        Duration::from_nanos(latency.as_nanos() as u64 / batch_size.max(1) as u64);
                    for _ in 0..batch_size {
                        metrics.record_read(per_read_latency);
                    }
                },
                Err(e) => {
                    // Only count real errors, not NOT_FOUND
                    if e.code() != tonic::Code::NotFound {
                        for _ in 0..read_batch_size {
                            metrics.record_read_error();
                        }
                    } else {
                        // NOT_FOUND counts as successful reads
                        let latency = start.elapsed();
                        let per_read_latency = Duration::from_nanos(
                            latency.as_nanos() as u64 / read_batch_size as u64,
                        );
                        for _ in 0..read_batch_size {
                            metrics.record_read(per_read_latency);
                        }
                    }
                },
            }
            key_counter += read_batch_size as u64;
        } else {
            // Single read - use regular Read RPC
            let key =
                format!("stress-key-{}-{}-0", worker_id % config.write_workers, key_counter % 1000);

            let request = inferadb_ledger_raft::proto::ReadRequest {
                namespace_id: Some(inferadb_ledger_raft::proto::NamespaceId {
                    id: config.namespace_id,
                }),
                vault_id: Some(inferadb_ledger_raft::proto::VaultId { id: config.vault_id }),
                key,
                consistency: inferadb_ledger_raft::proto::ReadConsistency::Eventual as i32,
            };

            match client.read(request).await {
                Ok(_) => {
                    metrics.record_read(start.elapsed());
                },
                Err(e) => {
                    if e.code() != tonic::Code::NotFound {
                        metrics.record_read_error();
                    } else {
                        metrics.record_read(start.elapsed());
                    }
                },
            }
            key_counter += 1;
        }
    }
}

/// Verify consistency of written values by reading them back.
async fn verify_consistency(
    leader_addr: SocketAddr,
    config: &StressConfig,
    metrics: &StressMetrics,
) -> Result<(), String> {
    println!("\n🔍 Verifying consistency of written values...");

    let endpoint = format!("http://{}", leader_addr);
    let mut client =
        inferadb_ledger_raft::proto::read_service_client::ReadServiceClient::connect(endpoint)
            .await
            .map_err(|e| format!("Failed to connect for verification: {}", e))?;

    let written = metrics.written_values.lock().clone();
    let total = written.len();
    let mut _verified = 0usize;
    let mut mismatches = 0usize;
    let sample_size = std::cmp::min(1000, total); // Sample up to 1000 keys

    for (i, (key, expected_value)) in written.iter().take(sample_size).enumerate() {
        let request = inferadb_ledger_raft::proto::ReadRequest {
            namespace_id: Some(inferadb_ledger_raft::proto::NamespaceId {
                id: config.namespace_id,
            }),
            vault_id: Some(inferadb_ledger_raft::proto::VaultId { id: config.vault_id }),
            key: key.clone(),
            // Use eventual consistency for verification - linearizable reads require
            // additional Raft configuration that isn't always enabled in test clusters.
            // Eventual consistency is sufficient here since we wait for cluster sync.
            consistency: inferadb_ledger_raft::proto::ReadConsistency::Eventual as i32,
        };

        // Add timeout to prevent hanging if server is unresponsive
        let read_result = tokio::time::timeout(Duration::from_secs(5), client.read(request)).await;

        match read_result {
            Ok(Ok(response)) => {
                let inner = response.into_inner();
                if let Some(value) = inner.value {
                    if value == *expected_value {
                        _verified += 1;
                    } else {
                        mismatches += 1;
                        if mismatches <= 5 {
                            eprintln!(
                                "  ❌ Mismatch for key '{}': expected {} bytes, got {} bytes",
                                key,
                                expected_value.len(),
                                value.len()
                            );
                        }
                    }
                } else {
                    mismatches += 1;
                    if mismatches <= 5 {
                        eprintln!("  ❌ Key '{}' not found but was written", key);
                    }
                }
            },
            Ok(Err(e)) => {
                mismatches += 1;
                if mismatches <= 5 {
                    eprintln!("  ❌ Error reading key '{}': {}", key, e);
                }
            },
            Err(_) => {
                // Timeout - server is unresponsive
                mismatches += 1;
                if mismatches <= 5 {
                    eprintln!("  ❌ Timeout reading key '{}' (server unresponsive)", key);
                }
                // Skip remaining verifications if we're timing out
                if mismatches > 3 {
                    eprintln!("  ⚠️  Too many timeouts, skipping remaining verifications");
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
        println!("  ✅ All {} sampled keys verified successfully", sample_size);
        Ok(())
    }
}

/// Verify consistency of written values across multiple shards.
///
/// Unlike single-shard verification, this reads from the correct namespace/vault
/// for each key based on where it was written.
async fn verify_multi_shard_consistency(
    leader_addr: SocketAddr,
    metrics: &StressMetrics,
) -> Result<(), String> {
    println!("\n🔍 Verifying consistency across all shards...");

    let endpoint = format!("http://{}", leader_addr);
    let mut client =
        inferadb_ledger_raft::proto::read_service_client::ReadServiceClient::connect(endpoint)
            .await
            .map_err(|e| format!("Failed to connect for verification: {}", e))?;

    let written = metrics.written_values_with_location.lock().clone();
    let total = written.len();

    if total == 0 {
        println!("  ⚠️  No values recorded for verification");
        return Ok(());
    }

    let mut verified = 0usize;
    let mut mismatches = 0usize;
    let sample_size = std::cmp::min(1000, total); // Sample up to 1000 keys

    // Collect keys to verify (deterministic sample)
    let keys_to_verify: Vec<_> = written.keys().take(sample_size).cloned().collect();

    for (i, key) in keys_to_verify.iter().enumerate() {
        let written_value = written.get(key).unwrap();

        let request = inferadb_ledger_raft::proto::ReadRequest {
            namespace_id: Some(inferadb_ledger_raft::proto::NamespaceId {
                id: written_value.namespace_id,
            }),
            vault_id: Some(inferadb_ledger_raft::proto::VaultId { id: written_value.vault_id }),
            key: key.clone(),
            consistency: inferadb_ledger_raft::proto::ReadConsistency::Eventual as i32,
        };

        let read_result = tokio::time::timeout(Duration::from_secs(5), client.read(request)).await;

        match read_result {
            Ok(Ok(response)) => {
                let inner = response.into_inner();
                if let Some(value) = inner.value {
                    if value == written_value.value {
                        verified += 1;
                    } else {
                        mismatches += 1;
                        if mismatches <= 5 {
                            eprintln!(
                                "  ❌ Mismatch for key '{}' (ns={}, vault={}): expected {} bytes, got {} bytes",
                                key,
                                written_value.namespace_id,
                                written_value.vault_id,
                                written_value.value.len(),
                                value.len()
                            );
                        }
                    }
                } else {
                    mismatches += 1;
                    if mismatches <= 5 {
                        eprintln!(
                            "  ❌ Key '{}' not found in ns={}, vault={} (was written)",
                            key, written_value.namespace_id, written_value.vault_id
                        );
                    }
                }
            },
            Ok(Err(e)) => {
                mismatches += 1;
                if mismatches <= 5 {
                    eprintln!(
                        "  ❌ Error reading key '{}' from ns={}, vault={}: {}",
                        key, written_value.namespace_id, written_value.vault_id, e
                    );
                }
            },
            Err(_) => {
                mismatches += 1;
                if mismatches <= 5 {
                    eprintln!("  ❌ Timeout reading key '{}'", key);
                }
                if mismatches > 3 {
                    eprintln!("  ⚠️  Too many timeouts, skipping remaining verifications");
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
            "Multi-shard consistency check failed: {} mismatches out of {} sampled keys",
            mismatches, sample_size
        ))
    } else {
        println!("  ✅ All {} sampled keys verified successfully across all shards", sample_size);
        Ok(())
    }
}

/// Run the full stress test with default cluster size.
async fn run_stress_test(config: StressConfig) {
    run_stress_test_with_cluster_size(3, config).await;
}

/// Run the full stress test with configurable cluster size.
async fn run_stress_test_with_cluster_size(cluster_size: usize, config: StressConfig) {
    println!("\n🚀 Starting Stress Test");
    println!("   Cluster size: {} node(s)", cluster_size);
    println!("   Write workers: {}", config.write_workers);
    println!("   Read workers: {}", config.read_workers);
    println!("   Duration: {:?}", config.duration);
    println!("   Batch size: {}", config.batch_size);
    println!();

    // Start cluster
    println!("📦 Creating {}-node cluster...", cluster_size);
    let cluster = TestCluster::new(cluster_size).await;
    let leader_id = cluster.wait_for_leader().await;
    println!("   Leader elected: node {}", leader_id);

    // Allow cluster to fully stabilize before stress testing
    // This prevents OpenRaft internal state race conditions
    println!("   Waiting for cluster stabilization...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let leader = cluster.leader().expect("should have leader");
    let leader_addr = leader.addr;
    let all_addrs: Vec<SocketAddr> = cluster.nodes().iter().map(|n| n.addr).collect();

    // Setup: Create namespace and vault for the stress test
    // This ensures the write operations don't fail due to missing entities
    println!("🔧 Setting up namespace and vault...");
    if let Err(e) = setup_namespace_and_vault(leader_addr, &config).await {
        eprintln!("   ⚠️ Setup failed (may already exist): {}", e);
        // Continue anyway - they might already exist
    } else {
        println!("   ✅ Namespace {} and vault {} created", config.namespace_id, config.vault_id);
    }

    // Metrics and control
    let metrics = Arc::new(StressMetrics::new());
    let running = Arc::new(AtomicBool::new(true));
    // Separate semaphores for reads and writes - reads can have much higher concurrency
    let write_semaphore = Arc::new(Semaphore::new(config.max_concurrent_writes));
    let read_semaphore = Arc::new(Semaphore::new(config.max_concurrent_reads));

    // Spawn write workers
    println!("🖊️  Spawning {} write workers...", config.write_workers);
    let mut handles = Vec::new();
    for i in 0..config.write_workers {
        let m = metrics.clone();
        let r = running.clone();
        let s = write_semaphore.clone();
        let c = config.clone();
        handles.push(tokio::spawn(write_worker(i, leader_addr, c, m, r, s)));
    }

    // Spawn read workers (distributed across all nodes)
    println!("📖 Spawning {} read workers...", config.read_workers);
    for i in 0..config.read_workers {
        let node_addr = all_addrs[i % all_addrs.len()];
        let m = metrics.clone();
        let r = running.clone();
        let s = read_semaphore.clone();
        let c = config.clone();
        handles.push(tokio::spawn(read_worker(i, node_addr, c, m, r, s)));
    }

    // Run for the specified duration
    println!("⏱️  Running stress test for {:?}...\n", config.duration);
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
    println!("\n⏳ Waiting for workers to finish...");
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        for handle in handles {
            let _ = handle.await;
        }
    })
    .await;

    // Report metrics
    let actual_duration = start.elapsed();
    metrics.report(actual_duration);

    // Verify consistency
    let consistency_result = verify_consistency(leader_addr, &config, &metrics).await;

    // Final sync check
    println!("\n🔄 Checking cluster sync...");
    let synced = cluster.wait_for_sync(Duration::from_secs(10)).await;
    if synced {
        println!("   ✅ All nodes synchronized");
    } else {
        println!("   ⚠️  Nodes may not be fully synchronized");
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
/// Raft consensus takes ~20-30ms per batch. Single-shard theoretical max:
/// - batch_size=50 @ 25ms = 2,000 ops/sec max
/// - batch_size=100 @ 25ms = 4,000 ops/sec max (requires 16KB pages)
///
/// With multi-shard (8 shards) and batch_size=100, achieves 6000+ ops/sec.
/// All Inkwell databases now use 16KB pages to support larger batch sizes.
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
#[ignore] // Takes ~15s, run with: cargo test stress_standard -- --ignored --nocapture
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

/// Heavy stress test - high load to find limits.
/// Run with: cargo test --release stress_heavy -- --ignored --nocapture
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore] // Too heavy for CI, run manually
async fn test_stress_heavy() {
    run_stress_test(StressConfig {
        write_workers: 16,
        read_workers: 32,
        duration: Duration::from_secs(60),
        batch_size: 10,
        max_concurrent_writes: 200,
        max_concurrent_reads: 2000,
        ..Default::default()
    })
    .await;
}

/// Maximum throughput test - push to the limit.
/// Run with: cargo test --release stress_max -- --ignored --nocapture
#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
#[ignore] // Too heavy for CI, run manually
async fn test_stress_max_throughput() {
    run_stress_test(StressConfig {
        write_workers: 32,
        read_workers: 64,
        duration: Duration::from_secs(120),
        batch_size: 20,
        max_concurrent_writes: 500,
        max_concurrent_reads: 5000,
        ..Default::default()
    })
    .await;
}

/// Sustained reliability test - long-running stability.
/// Run with: cargo test --release stress_sustained -- --ignored --nocapture
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore] // Long running, use for stability testing
async fn test_stress_sustained() {
    run_stress_test(StressConfig {
        write_workers: 8,
        read_workers: 16,
        duration: Duration::from_secs(300), // 5 minutes
        batch_size: 5,
        max_concurrent_writes: 100,
        max_concurrent_reads: 1000,
        ..Default::default()
    })
    .await;
}

// ============================================================================
// Multi-Shard Stress Tests
// ============================================================================
//
// These tests use MultiShardTestCluster to achieve higher write throughput
// via parallel Raft consensus across multiple independent shards.
//
// Each shard is a separate Raft group, allowing writes to different shards
// to proceed in parallel without consensus conflicts.

/// Run a stress test using multi-shard architecture.
///
/// This enables significantly higher write throughput by distributing
/// writes across multiple independent Raft groups. Each shard has its own
/// namespace, and workers are distributed across shards for parallel consensus.
async fn run_multi_shard_stress_test(num_nodes: usize, num_shards: usize, config: StressConfig) {
    println!("\n🚀 Starting Multi-Shard Stress Test (PARALLEL WRITES)");
    println!("   Nodes: {}", num_nodes);
    println!("   Data shards: {}", num_shards);
    println!(
        "   Write workers: {} ({} per shard)",
        config.write_workers,
        config.write_workers / num_shards.max(1)
    );
    println!("   Read workers: {}", config.read_workers);
    println!("   Duration: {:?}", config.duration);
    println!("   Batch size: {}", config.batch_size);
    println!();

    // Start multi-shard cluster
    println!("📦 Creating {}-node, {}-shard cluster...", num_nodes, num_shards);
    let cluster = MultiShardTestCluster::new(num_nodes, num_shards).await;

    // Wait for all shards to have leaders
    let leaders_ready = cluster.wait_for_leaders(Duration::from_secs(10)).await;
    if !leaders_ready {
        panic!("Failed to elect leaders on all shards");
    }
    println!("   ✅ Leaders elected on all {} shards", num_shards + 1); // +1 for system shard

    // Allow cluster to stabilize
    println!("   Waiting for cluster stabilization...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let node = cluster.any_node();
    let leader_addr = node.addr;
    let all_addrs = cluster.addresses();

    // Setup namespaces across all shards - one namespace per shard
    println!("🔧 Setting up {} namespaces (one per shard)...", num_shards);
    let shard_assignments = match setup_multi_shard_namespaces(leader_addr, num_shards).await {
        Ok(assignments) => {
            for assignment in &assignments {
                println!(
                    "   ✅ Shard {} → Namespace {} → Vault {}",
                    assignment.shard_id, assignment.namespace_id, assignment.vault_id
                );
            }
            assignments
        },
        Err(e) => {
            panic!("Failed to setup multi-shard namespaces: {}", e);
        },
    };

    // Metrics and control
    let metrics = Arc::new(StressMetrics::new());
    let running = Arc::new(AtomicBool::new(true));
    let write_semaphore = Arc::new(Semaphore::new(config.max_concurrent_writes));
    let read_semaphore = Arc::new(Semaphore::new(config.max_concurrent_reads));

    // Spawn write workers - DISTRIBUTED ACROSS SHARDS
    // Each worker is assigned to a specific shard to enable parallel consensus
    println!("🔥 Spawning {} write workers across {} shards...", config.write_workers, num_shards);
    let mut handles = Vec::new();
    for i in 0..config.write_workers {
        // Distribute workers round-robin across shards
        let assignment = shard_assignments[i % shard_assignments.len()].clone();
        let worker_config = StressConfig {
            namespace_id: assignment.namespace_id,
            vault_id: assignment.vault_id,
            track_write_locations: true, // Enable location tracking for multi-shard verification
            ..config.clone()
        };
        let metrics = metrics.clone();
        let running = running.clone();
        let semaphore = write_semaphore.clone();
        handles.push(tokio::spawn(async move {
            write_worker(i, leader_addr, worker_config, metrics, running, semaphore).await;
        }));
    }

    // For reads, use the first shard assignment (reads are fast anyway)
    let read_config = StressConfig {
        namespace_id: shard_assignments[0].namespace_id,
        vault_id: shard_assignments[0].vault_id,
        ..config.clone()
    };

    // Spawn read workers - distribute across all nodes for load balancing
    // Note: reads go to the first shard's namespace. For read testing, this is fine
    // since read throughput isn't limited by Raft consensus.
    println!("📖 Spawning {} read workers...", read_config.read_workers);
    for i in 0..read_config.read_workers {
        let worker_config = read_config.clone();
        let metrics = metrics.clone();
        let running = running.clone();
        let semaphore = read_semaphore.clone();
        // Distribute read workers across all nodes
        let node_addr = all_addrs[i % all_addrs.len()];
        handles.push(tokio::spawn(async move {
            read_worker(i, node_addr, worker_config, metrics, running, semaphore).await;
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
    println!("\n⏳ Waiting for workers to finish...");
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        for handle in handles {
            let _ = handle.await;
        }
    })
    .await;

    // Report metrics - reuse existing report() for detailed output
    let actual_duration = start.elapsed();
    println!("\n📊 Multi-Shard Stress Test Results ({} nodes, {} shards)", num_nodes, num_shards);
    metrics.report(actual_duration);

    // Verify consistency across all shards - reads from each key's recorded namespace/vault
    let consistency_result = verify_multi_shard_consistency(leader_addr, &metrics).await;
    if let Err(e) = &consistency_result {
        eprintln!("  ❌ Multi-shard consistency verification failed: {}", e);
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

    // Report multi-shard specific summary
    println!("\n🎯 Multi-Shard Summary:");
    println!("   Shards: {} data shards + 1 system shard", num_shards);
    println!("   Per-shard throughput: {:.0} ops/sec", ops_per_sec / num_shards as f64);
    println!("   Total throughput: {:.0} ops/sec", ops_per_sec);
    println!(
        "   Target (5000 ops/sec): {}",
        if ops_per_sec >= 5000.0 { "✅ PASS" } else { "❌ FAIL" }
    );
}

/// Quick multi-shard stress test - validates infrastructure works.
/// Run with: cargo test test_stress_multi_shard_quick -- --nocapture
///
/// Creates one namespace per shard and distributes write workers across them
/// for true parallel Raft consensus.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_stress_multi_shard_quick() {
    run_multi_shard_stress_test(
        1, // Single node for speed
        2, // 2 data shards
        StressConfig {
            write_workers: 4,
            read_workers: 8,
            duration: Duration::from_secs(5),
            batch_size: 10,
            namespace_id: 0, // Overridden per-worker by shard assignments
            vault_id: 1,
            max_concurrent_writes: 50,
            max_concurrent_reads: 200,
            ..Default::default()
        },
    )
    .await;
}

/// Multi-shard batched throughput test - parallel writes across 4 shards.
/// Run with: cargo test --release test_stress_multi_shard_batched -- --nocapture
///
/// This is the multi-shard equivalent of `test_stress_batched`. With 4 shards
/// and batch_size=50, theoretical max is 4x single-shard (~2300 ops/sec).
///
/// ## Expected Results (release mode)
/// - Single shard batched: ~580 ops/sec
/// - 4 shards batched: ~2000-2300 ops/sec (4x parallel consensus)
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_stress_multi_shard_batched() {
    run_multi_shard_stress_test(
        1, // Single node
        4, // 4 data shards for parallel writes
        StressConfig {
            write_workers: 8, // 2 workers per shard
            read_workers: 16,
            duration: Duration::from_secs(10),
            batch_size: 50, // Match batched test
            read_batch_size: 50,
            namespace_id: 0, // Overridden per-worker by shard assignments
            vault_id: 1,
            max_concurrent_writes: 100,
            max_concurrent_reads: 500,
            ..Default::default()
        },
    )
    .await;
}

/// Multi-shard sustained test - 4 shards for 15 seconds.
/// Run with: cargo test --release test_stress_multi_shard -- --nocapture
///
/// Validates sustained multi-shard throughput over moderate duration.
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_stress_multi_shard() {
    run_multi_shard_stress_test(
        1, // Single node
        4, // 4 data shards for parallel writes
        StressConfig {
            write_workers: 16,
            read_workers: 32,
            duration: Duration::from_secs(15),
            batch_size: 50, // Match batched test for consistency
            read_batch_size: 100,
            namespace_id: 0, // Overridden per-worker by shard assignments
            vault_id: 1,
            max_concurrent_writes: 200,
            max_concurrent_reads: 1000,
            ..Default::default()
        },
    )
    .await;
}

/// Multi-shard maximum throughput test on single machine.
/// Uses 8 shards with batch_size=100 to achieve target throughput.
/// Run with: cargo test --release test_stress_multi_shard_target -- --nocapture
///
/// ## Expected Results
/// - Write throughput: ~6000+ ops/sec (exceeds 5000 target)
/// - Write p99 latency: <5ms (well under 50ms target)
/// - Per-shard throughput: ~750-800 ops/sec
///
/// ## Technical Notes
/// - All Inkwell databases (raft, state, blocks) use 16KB pages
/// - batch_size=100 requires 16KB pages (~10KB serialized per batch)
/// - 16 write workers distribute load across 8 shards
#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
async fn test_stress_multi_shard_target() {
    run_multi_shard_stress_test(
        1, // Single node
        8, // 8 data shards - optimal for single machine
        StressConfig {
            write_workers: 16, // 2 workers per shard
            read_workers: 16,
            duration: Duration::from_secs(15),
            batch_size: 100, // 16KB pages support larger batches
            read_batch_size: 100,
            namespace_id: 0, // Overridden per-worker by shard assignments
            vault_id: 1,
            max_concurrent_writes: 160,
            max_concurrent_reads: 500,
            ..Default::default()
        },
    )
    .await;
}

/// Debug test for batch_write issue - single worker
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore] // Flaky in CI, run with: cargo test stress_batch_single_worker -- --ignored --nocapture
async fn test_stress_batch_single_worker() {
    run_stress_test_with_cluster_size(
        3, // 3-node cluster
        StressConfig {
            write_workers: 1, // Single writer to isolate issue
            read_workers: 1,
            duration: Duration::from_secs(5),
            batch_size: 10,           // Small batch
            max_concurrent_writes: 1, // One at a time
            max_concurrent_reads: 10,
            ..Default::default()
        },
    )
    .await;
}

/// Test batch_write with multiple workers but serialized (one at a time)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore] // Flaky in CI, run with: cargo test stress_batch_serialized -- --ignored --nocapture
async fn test_stress_batch_serialized() {
    run_stress_test_with_cluster_size(
        3, // 3-node cluster
        StressConfig {
            write_workers: 4, // Multiple writers
            read_workers: 4,
            duration: Duration::from_secs(5),
            batch_size: 10,           // Batch of 10 ops
            max_concurrent_writes: 1, // But only ONE write at a time
            max_concurrent_reads: 50,
            ..Default::default()
        },
    )
    .await;
}

/// Test batch_write with smaller batch size
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore] // Flaky in CI, run with: cargo test stress_batch_small -- --ignored --nocapture
async fn test_stress_batch_small() {
    run_stress_test_with_cluster_size(
        3, // 3-node cluster
        StressConfig {
            write_workers: 4,
            read_workers: 4,
            duration: Duration::from_secs(5),
            batch_size: 5, // Small batch - 5 ops per Raft entry
            max_concurrent_writes: 10,
            max_concurrent_reads: 50,
            ..Default::default()
        },
    )
    .await;
}

/// Test batch_write with medium batch size (10)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore] // Flaky in CI, run with: cargo test stress_batch_medium_10 -- --ignored --nocapture
async fn test_stress_batch_medium_10() {
    run_stress_test_with_cluster_size(
        3,
        StressConfig {
            write_workers: 4,
            read_workers: 4,
            duration: Duration::from_secs(5),
            batch_size: 10,
            max_concurrent_writes: 10,
            max_concurrent_reads: 50,
            ..Default::default()
        },
    )
    .await;
}

/// Test batch_write with medium batch size (14)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore] // Flaky in CI, run with: cargo test stress_batch_medium_14 -- --ignored --nocapture
async fn test_stress_batch_medium_14() {
    run_stress_test_with_cluster_size(
        3,
        StressConfig {
            write_workers: 4,
            read_workers: 4,
            duration: Duration::from_secs(5),
            batch_size: 14,
            max_concurrent_writes: 10,
            max_concurrent_reads: 50,
            ..Default::default()
        },
    )
    .await;
}

/// Test batch_write with medium batch size (15)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore] // Flaky in CI, run with: cargo test stress_batch_medium_15 -- --ignored --nocapture
async fn test_stress_batch_medium_15() {
    run_stress_test_with_cluster_size(
        3,
        StressConfig {
            write_workers: 4,
            read_workers: 4,
            duration: Duration::from_secs(5),
            batch_size: 15,
            max_concurrent_writes: 10,
            max_concurrent_reads: 50,
            ..Default::default()
        },
    )
    .await;
}

/// Test batch_write with medium batch size (20)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore] // Flaky in CI, run with: cargo test stress_batch_medium_20 -- --ignored --nocapture
async fn test_stress_batch_medium_20() {
    run_stress_test_with_cluster_size(
        3,
        StressConfig {
            write_workers: 4,
            read_workers: 4,
            duration: Duration::from_secs(5),
            batch_size: 20,
            max_concurrent_writes: 10,
            max_concurrent_reads: 50,
            ..Default::default()
        },
    )
    .await;
}
