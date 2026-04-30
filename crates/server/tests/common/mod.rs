//! Test harness for cluster integration tests.
//!
//! Provides utilities for spawning and managing multi-node test clusters.
//!
//! ## Cluster Types
//!
//! - `TestCluster`: Full-stack cluster using `bootstrap_node()`. Supports single-region (`new()`)
//!   and multi-region (`with_data_regions()`) configurations. Includes gRPC server, saga
//!   orchestrator, background jobs, and all production services.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods
)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicU16, Ordering},
    },
    time::Duration,
};

/// Monotonically increasing cluster counter — guarantees each test gets unique
/// socket paths even when tests run in parallel.
static CLUSTER_COUNTER: AtomicU16 = AtomicU16::new(0);

fn next_cluster_id() -> u16 {
    CLUSTER_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Monotonically increasing port counter for TCP-mode test clusters.
static NEXT_PORT: AtomicU16 = AtomicU16::new(30_000);

/// Allocates `count` contiguous ports from the global counter.
pub fn allocate_ports(count: u16) -> u16 {
    NEXT_PORT.fetch_add(count, Ordering::Relaxed)
}

/// Transport mode for test clusters.
#[derive(Clone, Copy, Default)]
pub enum TestTransport {
    /// Unix domain sockets (default). Eliminates port contention.
    #[default]
    Uds,
    /// TCP with auto-allocated ports. Required for tests that need `SocketAddr`.
    Tcp,
}

/// Creates a gRPC channel — UDS for paths starting with `/`, TCP otherwise.
pub fn connect_channel(addr: &str) -> tonic::transport::Channel {
    if addr.starts_with('/') {
        let path = std::path::PathBuf::from(addr);
        tonic::transport::Endpoint::try_from("http://[::]:50051")
            .unwrap()
            .connect_with_connector_lazy(tower::service_fn(move |_: tonic::transport::Uri| {
                let path = path.clone();
                async move {
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                        tokio::net::UnixStream::connect(&path).await?,
                    ))
                }
            }))
    } else {
        let endpoint = format!("http://{}", addr);
        tonic::transport::Channel::from_shared(endpoint).unwrap().connect_lazy()
    }
}

use inferadb_ledger_proto::proto::{JoinClusterRequest, admin_service_client::AdminServiceClient};
use inferadb_ledger_raft::{
    ConsensusHandle, OrganizationGroup, RaftManager, RegionConfig, RegionGroup, SystemGroup,
};
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::{FileBackend, crypto::InMemoryKeyManager};
use inferadb_ledger_test_utils::TestDir;
use tokio::time::timeout;

/// A test node in a cluster.
///
/// Every node gets the full production bootstrap (saga orchestrator, blinding
/// key, background jobs, etc.) plus at least one data region for org creation.
pub struct TestNode {
    /// The node ID.
    pub id: u64,
    /// The gRPC address (UDS socket path or TCP "ip:port").
    pub addr: String,
    /// The GLOBAL consensus handle.
    pub handle: Arc<ConsensusHandle>,
    /// The GLOBAL state layer (internally thread-safe via inferadb-ledger-store MVCC).
    pub state: Arc<StateLayer<FileBackend>>,
    /// Multi-Raft manager for region routing and data region access.
    pub manager: Arc<RaftManager>,
    /// Temporary directory for node data.
    ///
    /// Captured as an `Option` so `TestCluster::graceful_restart` can take
    /// ownership and preserve the data_dir across server lifetimes — a
    /// restart re-runs `bootstrap_node` against the same on-disk state.
    _temp_dir: Option<TestDir>,
    /// Server task handle for cleanup.
    ///
    /// `Option` so `graceful_restart` can take ownership and await the task
    /// for clean termination before re-bootstrapping.
    _server_handle: Option<tokio::task::JoinHandle<()>>,
    /// Shutdown sender — kept alive so the server doesn't immediately exit.
    /// When dropped, the watch receiver in `serve_with_shutdown` resolves,
    /// triggering graceful shutdown.
    ///
    /// `Option` so `graceful_restart` can take ownership and explicitly
    /// trigger a clean shutdown (rather than relying on the implicit drop
    /// path) before re-bootstrapping.
    _shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
    /// Shutdown coordinator — held so `graceful_restart` can drain
    /// background tasks (compaction, GC, checkpointer, etc.) before
    /// re-bootstrapping against the same data_dir. Without an explicit
    /// coordinator drain, those tasks keep file descriptors open and
    /// the new `bootstrap_node` call can race them on B+ tree open.
    coordinator: Option<Arc<inferadb_ledger_server::shutdown::ShutdownCoordinator>>,
    /// UDS socket path (if the transport is UDS) — preserved so restart can
    /// rebind to the same path.
    socket_path: Option<std::path::PathBuf>,
    /// TCP listen addr (if the transport is TCP) — preserved so restart can
    /// rebind to the same port.
    listen: Option<std::net::SocketAddr>,
}

impl TestNode {
    /// Checks if this node is the current leader for the GLOBAL region.
    pub fn is_leader(&self) -> bool {
        self.handle.current_leader() == Some(self.id)
    }

    /// Returns the current leader ID for the GLOBAL region if known.
    pub fn current_leader(&self) -> Option<u64> {
        self.handle.current_leader()
    }

    /// Returns the current term for the GLOBAL region.
    pub fn current_term(&self) -> u64 {
        self.handle.current_term()
    }

    /// Returns the last applied log index for the GLOBAL region.
    pub fn last_applied(&self) -> u64 {
        self.handle.commit_index()
    }

    /// Returns the system (GLOBAL) region group.
    pub fn system_region(&self) -> Arc<SystemGroup> {
        self.manager.system_region().expect("system region exists")
    }

    /// Returns a region group by region.
    pub fn region_group(&self, region: inferadb_ledger_types::Region) -> Option<Arc<RegionGroup>> {
        self.manager.get_region_group(region).ok()
    }

    /// Returns every per-organization shard group registered for `region`
    /// on this node.
    ///
    /// Under Task 3's tier split the data-region group at
    /// `OrganizationId::new(0)` is the regional control plane
    /// ([`RegionGroup`]), not a data-plane shard, so it is excluded from
    /// this collection. Callers iterating "every shard" mean the
    /// per-organization groups; the data-region group is reachable via
    /// [`region_group`].
    pub fn shard_groups(
        &self,
        region: inferadb_ledger_types::Region,
    ) -> Vec<Arc<OrganizationGroup>> {
        self.manager
            .list_organization_groups()
            .into_iter()
            .filter(|(r, s)| *r == region && *s != inferadb_ledger_types::OrganizationId::new(0))
            .filter_map(|(r, s)| self.manager.get_organization_group(r, s).ok())
            .collect()
    }

    /// Returns all region IDs registered on this node.
    pub fn regions(&self) -> Vec<inferadb_ledger_types::Region> {
        self.manager.list_regions()
    }

    /// Checks if this node is leader for the system (GLOBAL) region.
    pub fn is_system_leader(&self) -> bool {
        self.handle.current_leader() == Some(self.id)
    }

    /// Returns the on-disk data directory of this node, if the test
    /// fixture still owns the temp dir (it is taken away by
    /// `graceful_restart`).
    ///
    /// Useful for tests that need to inspect on-disk state (e.g. the
    /// per-vault `state.db` files an archive includes) outside the gRPC
    /// surface.
    #[allow(dead_code)]
    pub fn data_dir(&self) -> Option<&std::path::Path> {
        self._temp_dir.as_ref().map(|t| t.path())
    }
}

/// Builds a Raft config with aggressive timeouts for fast test execution.
///
/// Production defaults (100ms heartbeat, 300-500ms election) are tuned for
/// real networks. Tests run on localhost but under heavy parallelism (8 threads,
/// each spawning 1-3 tokio runtimes), so we use 50ms heartbeat and 150-300ms
/// election timeouts — 2x faster than production while remaining stable under
/// CPU contention from parallel test execution.
fn test_rate_limit_config() -> inferadb_ledger_types::config::RateLimitConfig {
    // Enable the limiter for tests that exercise it. Tests that rely on the
    // disabled fast path build their own config (or omit `--rate-limit`).
    inferadb_ledger_types::config::RateLimitConfig::builder()
        .enabled(true)
        .client_burst(10_000_u64)
        .client_rate(10_000.0)
        .organization_burst(10_000_u64)
        .organization_rate(10_000.0)
        .backpressure_threshold(10_000_u64)
        .build()
        .expect("valid rate limit config")
}

fn test_raft_config() -> inferadb_ledger_types::config::RaftConfig {
    inferadb_ledger_types::config::RaftConfig::builder()
        .heartbeat_interval(Duration::from_millis(100))
        .election_timeout_min(Duration::from_millis(300))
        .election_timeout_max(Duration::from_millis(600))
        .build()
        .expect("valid test raft config")
}

/// A test cluster of Raft nodes.
///
/// Every cluster gets the full production bootstrap (saga orchestrator, blinding
/// key, background jobs) plus at least one data region. This mirrors production
/// where all Ledger instances are inherently multi-region.
pub struct TestCluster {
    /// The nodes in the cluster.
    nodes: Vec<TestNode>,
    /// Number of data regions (at least 1).
    num_data_regions: usize,
    /// Keeps socket files alive for cluster lifetime.
    _socket_dir: TestDir,
    /// Transport mode — preserved so `graceful_restart` can reconstruct
    /// server configs with the same UDS/TCP choice.
    transport: TestTransport,
    /// Whether the email blinding key was configured on the bootstrap node
    /// — preserved so `graceful_restart` restores the same surface.
    include_blinding_key: bool,
    /// Rate-limit override passed to `build_full`, if any — preserved so
    /// `graceful_restart` restores the same throttling surface.
    rate_limit_override: Option<inferadb_ledger_types::config::RateLimitConfig>,
    /// Shared key manager — SAME instance across the whole cluster, SAME
    /// instance across a restart. Per-region keys are derived from this
    /// manager; a fresh manager after restart would not unseal any existing
    /// vault.
    key_manager: Arc<dyn inferadb_ledger_store::crypto::RegionKeyManager>,
}

impl TestCluster {
    /// Creates a new test cluster with the given number of nodes — lean, system
    /// group only.
    ///
    /// The first node bootstraps the cluster, and other nodes join via the
    /// AdminService's `join_cluster` RPC. No data regions are started. Tests
    /// that need a data region must call [`TestCluster::create_data_region`]
    /// explicitly — this matches production, where data regions are created
    /// through cluster consensus rather than being pinned at startup.
    pub async fn new(size: usize) -> Self {
        Self::build(size, 0, true).await
    }

    /// Creates a new test cluster with the given number of nodes and data regions.
    ///
    /// Each node gets a full production bootstrap (saga, blinding key, background
    /// jobs) plus `num_data_regions` data regions in addition to the GLOBAL region.
    pub async fn with_data_regions(size: usize, num_data_regions: usize) -> Self {
        Self::build(size, num_data_regions, true).await
    }

    /// Creates a cluster without the email blinding key configured.
    ///
    /// Used to test that onboarding RPCs return `FAILED_PRECONDITION` when
    /// the server is started without a blinding key.
    pub async fn without_blinding_key(size: usize, num_data_regions: usize) -> Self {
        Self::build(size, num_data_regions, false).await
    }

    /// Creates a cluster using TCP transport instead of UDS.
    ///
    /// Required for tests that need `SocketAddr` (e.g., `discover_node_info`)
    /// or TCP channel caching validation (port consumption tests).
    pub async fn with_tcp(size: usize) -> Self {
        Self::build_full(size, 1, true, None, TestTransport::Tcp).await
    }

    /// Creates a multi-region cluster using TCP transport.
    pub async fn with_tcp_data_regions(size: usize, num_data_regions: usize) -> Self {
        Self::build_full(size, num_data_regions, true, None, TestTransport::Tcp).await
    }

    /// Creates a cluster with a custom rate limit config.
    ///
    /// Allows tests to override the default high-burst test config (burst=10,000)
    /// with a lower config to exercise rate limiting behavior.
    pub async fn with_rate_limit(
        size: usize,
        rate_limit: inferadb_ledger_types::config::RateLimitConfig,
    ) -> Self {
        Self::build_full(size, 1, true, Some(rate_limit), TestTransport::Uds).await
    }

    async fn build(size: usize, num_data_regions: usize, include_blinding_key: bool) -> Self {
        Self::build_full(size, num_data_regions, include_blinding_key, None, TestTransport::Uds)
            .await
    }

    async fn build_full(
        size: usize,
        num_data_regions: usize,
        include_blinding_key: bool,
        rate_limit_override: Option<inferadb_ledger_types::config::RateLimitConfig>,
        transport: TestTransport,
    ) -> Self {
        assert!(size >= 1, "cluster must have at least 1 node");

        let socket_dir = TestDir::new();
        let cluster_id = next_cluster_id();
        let base_port = match transport {
            TestTransport::Tcp => allocate_ports(size as u16),
            TestTransport::Uds => 0, // unused
        };
        let mut nodes = Vec::with_capacity(size);

        // Shared key manager for all nodes (enables TokenService with JWT support).
        // Uses ALL_REGIONS to cover both GLOBAL (user sessions) and org-scoped keys.
        let key_manager: Arc<dyn inferadb_ledger_store::crypto::RegionKeyManager> =
            Arc::new(InMemoryKeyManager::generate_for_regions(&inferadb_ledger_types::ALL_REGIONS));

        // Step 1: Start the bootstrap node with cluster_id pre-written so it
        // takes the restart path (immediate startup). It becomes leader, then
        // we dynamically add nodes.
        let (node_addr, socket_path, listen) = match transport {
            TestTransport::Uds => {
                let sp = socket_dir.path().join(format!("c{cluster_id}-n0.sock"));
                let addr = sp.to_string_lossy().to_string();
                (addr, Some(sp), None)
            },
            TestTransport::Tcp => {
                let tcp_addr: std::net::SocketAddr =
                    format!("127.0.0.1:{base_port}").parse().unwrap();
                (tcp_addr.to_string(), None, Some(tcp_addr))
            },
        };
        let temp_dir = TestDir::new();
        let data_dir = temp_dir.path().to_path_buf();

        // Bootstrap node: write cluster_id so bootstrap_node takes the restart
        // path (starts background jobs, marks ready) instead of the fresh path
        // (which blocks waiting for InitCluster RPC).
        inferadb_ledger_server::cluster_id::write_cluster_id(&data_dir, 1)
            .expect("write cluster_id for bootstrap node");

        // Uses auto-generated Snowflake ID for realistic testing.
        let backup_config = inferadb_ledger_types::config::BackupConfig::builder()
            .destination(data_dir.join("backups").to_string_lossy().to_string())
            .build()
            .expect("valid backup config");

        // Tests historically expect rate limiting to be wired up (a few of
        // them assert it rejects with `ResourceExhausted`). Mirror the master
        // switch from the rate-limit struct so the existing high-burst
        // defaults take effect; per-test overrides then narrow the bucket
        // via `with_rate_limit`.
        let rate_limit_cfg = rate_limit_override.clone().unwrap_or_else(test_rate_limit_config);
        // Per the DSoT migration: the CLI override is left as `None` and
        // the inner `RateLimitConfig::enabled` field on `rate_limit_cfg`
        // drives the master switch. Tests that need to flip it on
        // construct `RateLimitConfig::builder().enabled(true)…build()`
        // directly via `with_rate_limit`.
        let config = inferadb_ledger_server::config::Config {
            listen,
            socket: socket_path.clone(),
            metrics_addr: None,
            data_dir: Some(data_dir.clone()),
            backup: Some(backup_config),
            raft: Some(test_raft_config()),
            saga: inferadb_ledger_types::config::SagaConfig { poll_interval_secs: 2 },
            token_maintenance_interval_secs: 3,
            ratelimit: None,
            rate_limit: Some(rate_limit_cfg),
            email_blinding_key: if include_blinding_key {
                Some("deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string())
            } else {
                None
            },
            ..inferadb_ledger_server::config::Config::default()
        };

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let health_state = inferadb_ledger_raft::HealthState::new();
        let bootstrapped = inferadb_ledger_server::bootstrap::bootstrap_node(
            &config,
            &data_dir,
            health_state.clone(),
            shutdown_rx,
            Some(key_manager.clone()),
        )
        .await
        .expect("bootstrap node");

        // Mark node as ready (mirrors main.rs post-bootstrap behavior)
        health_state.mark_ready();

        // Get the auto-generated Snowflake ID from the consensus handle
        let node_id = bootstrapped.handle.node_id();

        // Server is already running and accepting TCP connections from bootstrap_node().
        // Extract fields before moving server_handle into the spawned task.
        let handle_clone = bootstrapped.handle.clone();
        let state_clone = bootstrapped.state.clone();
        let manager_clone = bootstrapped.manager.clone();
        let coordinator = bootstrapped.coordinator.clone();
        let bg_server_handle = bootstrapped.server_handle;
        let server_handle = tokio::spawn(async move {
            let _ = bg_server_handle.await;
        });

        let leader_handle = handle_clone.clone();
        let leader_addr = node_addr.clone();
        nodes.push(TestNode {
            id: node_id,
            addr: node_addr.clone(),
            handle: handle_clone,
            state: state_clone,
            manager: manager_clone.clone(),
            _temp_dir: Some(temp_dir),
            _server_handle: Some(server_handle),
            _shutdown_tx: Some(shutdown_tx),
            coordinator: Some(coordinator),
            socket_path: socket_path.clone(),
            listen,
        });

        // Wait for the bootstrap node to become GLOBAL leader first.
        // With test config (300-600ms election timeout on localhost), 10 seconds
        // is generous budget for parallel test runs under CPU contention.
        let start = tokio::time::Instant::now();
        let timeout_duration = Duration::from_secs(10);
        while start.elapsed() < timeout_duration {
            if leader_handle.current_leader() == Some(node_id) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Verify GLOBAL leader election succeeded
        if leader_handle.current_leader() != Some(node_id) {
            panic!("Bootstrap node failed to become GLOBAL leader within timeout");
        }

        // Start data regions AFTER GLOBAL leader election so the two don't interfere.
        let data_regions = &inferadb_ledger_types::ALL_REGIONS[1..];
        for &data_region in data_regions.iter().take(num_data_regions) {
            let data_region_config = RegionConfig {
                region: data_region,
                initial_members: vec![(node_id, node_addr.clone())],
                bootstrap: true,
                enable_background_jobs: true,
                batch_writer_config: None,
                event_writer: None,
                events_config: None,
                delegated_leadership: false,
                // Test fixture: bypasses the `CreateDataRegion` apply path,
                // so no directory entry exists. Default to non-protected so
                // single-node test setups don't trip the protected-region
                // quorum check.
                requires_residency_hint: false,
            };
            manager_clone
                .start_data_region(data_region_config)
                .await
                .unwrap_or_else(|e| panic!("start data region {:?}: {e}", data_region));
        }

        // Wait for all data region leader elections.
        //
        // Phase A multi-shard: each region runs N independent Raft groups,
        // each with its own election. Writes route to a specific shard via
        // `ShardRouter`, so a test write that lands on shard 5 will fail
        // with "no leader" if we only waited for shard 0. Iterate every
        // `(region, shard)` pair the manager has registered.
        let data_region_wait_start = tokio::time::Instant::now();
        'data_wait: while data_region_wait_start.elapsed() < Duration::from_secs(30) {
            let mut all_ready = true;
            for &dr in data_regions.iter().take(num_data_regions) {
                let shards =
                    manager_clone.list_organization_groups().into_iter().filter(|(r, _)| *r == dr);
                let mut any_shard = false;
                for (r, shard) in shards {
                    any_shard = true;
                    match manager_clone.get_organization_group(r, shard) {
                        Ok(rg) if rg.handle().current_leader().is_some() => continue,
                        _ => {
                            all_ready = false;
                            break;
                        },
                    }
                }
                if !any_shard || !all_ready {
                    all_ready = false;
                    break;
                }
            }
            if all_ready {
                break 'data_wait;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Step 2: Start remaining nodes and have them join the cluster dynamically
        for i in 1..size {
            let (join_addr, join_socket, join_listen) = match transport {
                TestTransport::Uds => {
                    let sp = socket_dir.path().join(format!("c{cluster_id}-n{i}.sock"));
                    let addr = sp.to_string_lossy().to_string();
                    (addr, Some(sp), None)
                },
                TestTransport::Tcp => {
                    let tcp_addr: std::net::SocketAddr =
                        format!("127.0.0.1:{}", base_port + i as u16).parse().unwrap();
                    (tcp_addr.to_string(), None, Some(tcp_addr))
                },
            };
            let temp_dir = TestDir::new();
            let data_dir = temp_dir.path().to_path_buf();

            inferadb_ledger_server::cluster_id::write_cluster_id(&data_dir, 1)
                .expect("write cluster_id for joining node");

            let backup_config = inferadb_ledger_types::config::BackupConfig::builder()
                .destination(data_dir.join("backups").to_string_lossy().to_string())
                .build()
                .expect("valid backup config");

            let config = inferadb_ledger_server::config::Config {
                listen: join_listen,
                socket: join_socket.clone(),
                metrics_addr: None,
                data_dir: Some(data_dir.clone()),
                backup: Some(backup_config),
                raft: Some(test_raft_config()),
                saga: inferadb_ledger_types::config::SagaConfig { poll_interval_secs: 2 },
                token_maintenance_interval_secs: 3,
                rate_limit: Some(
                    rate_limit_override.clone().unwrap_or_else(test_rate_limit_config),
                ),
                email_blinding_key: Some(
                    "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string(),
                ),
                ..inferadb_ledger_server::config::Config::default()
            };
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

            let health_state = inferadb_ledger_raft::HealthState::new();
            let bootstrapped = inferadb_ledger_server::bootstrap::bootstrap_node(
                &config,
                &data_dir,
                health_state.clone(),
                shutdown_rx,
                Some(key_manager.clone()),
            )
            .await
            .expect("bootstrap node");

            // Mark node as ready (mirrors main.rs post-bootstrap behavior)
            health_state.mark_ready();

            // Get the auto-generated Snowflake ID from the consensus handle
            let node_id = bootstrapped.handle.node_id();

            // Server is already running and accepting TCP connections from bootstrap_node().
            // Extract fields before moving server_handle into the spawned task.
            let handle_clone = bootstrapped.handle.clone();
            let state_clone = bootstrapped.state.clone();
            let manager_clone = bootstrapped.manager.clone();
            let coordinator = bootstrapped.coordinator.clone();
            let bg_server_handle = bootstrapped.server_handle;
            let server_handle = tokio::spawn(async move {
                let _ = bg_server_handle.await;
            });

            // Join the cluster via the current leader's AdminService.
            // Under parallel test execution, leader re-elections can occur, so we
            // discover the actual leader from Raft metrics on each attempt rather
            // than assuming the bootstrap node is still leader.
            let mut join_success = false;
            let mut last_error = String::new();
            let max_attempts = 40;

            for attempt in 0..max_attempts {
                // Discover the current leader by checking all existing nodes' metrics.
                // This handles the case where the bootstrap node lost leadership
                // during cluster formation under heavy parallel test load.
                let current_leader_addr = nodes
                    .iter()
                    .find_map(|n| {
                        let leader_id = n.handle.current_leader()?;
                        nodes.iter().find(|n2| n2.id == leader_id).map(|n2| n2.addr.clone())
                    })
                    .unwrap_or_else(|| leader_addr.clone());

                let channel = connect_channel(&current_leader_addr);
                let mut client = AdminServiceClient::new(channel);

                let join_request = JoinClusterRequest { node_id, address: join_addr.clone() };

                match client.join_cluster(join_request).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        if resp.success {
                            join_success = true;
                            break;
                        } else {
                            last_error = resp.message.clone();
                            // Membership conflicts and timeouts need backoff to let
                            // the cluster stabilize before retrying
                            let backoff = if resp.message.contains("membership")
                                || resp.message.contains("Timeout")
                            {
                                Duration::from_millis(100 * (attempt + 1) as u64)
                            } else {
                                Duration::from_millis(25 * (attempt + 1) as u64)
                            };
                            tokio::time::sleep(backoff).await;
                        }
                    },
                    Err(e) => {
                        last_error = format!("join RPC failed: {}", e);
                        tokio::time::sleep(Duration::from_millis(50 * (attempt + 1) as u64)).await;
                    },
                }
            }

            if !join_success {
                panic!(
                    "Node {} failed to join cluster after {} attempts: {}",
                    node_id, max_attempts, last_error
                );
            }

            let new_handle = handle_clone.clone();
            nodes.push(TestNode {
                id: node_id,
                addr: join_addr.clone(),
                handle: handle_clone,
                state: state_clone,
                manager: manager_clone,
                _temp_dir: Some(temp_dir),
                _server_handle: Some(server_handle),
                _shutdown_tx: Some(shutdown_tx),
                coordinator: Some(coordinator),
                socket_path: join_socket.clone(),
                listen: join_listen,
            });

            // Wait for the new node to see a leader after joining.
            let sync_start = tokio::time::Instant::now();
            let sync_timeout = Duration::from_secs(10);
            while sync_start.elapsed() < sync_timeout {
                if new_handle.current_leader().is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            // Wait for the membership change to fully commit before adding
            // the next node. Raft allows only one membership change at a time.
            // Poll until the new node appears as a voter in the leader's state.
            let target_node = inferadb_ledger_consensus::types::NodeId(node_id);
            let stabilize_start = tokio::time::Instant::now();
            let stabilize_timeout = Duration::from_secs(10);
            while stabilize_start.elapsed() < stabilize_timeout {
                let state = leader_handle.shard_state();
                if state.voters.contains(&target_node) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            // Ensure full-mesh GLOBAL transport between the new node and ALL
            // existing nodes (not just the bootstrap node). Without this, after
            // leader removal the remaining nodes can't communicate for elections.
            let joining_manager_ref = nodes.last().unwrap().manager.clone();
            if let Ok(joining_global) = joining_manager_ref.system_region()
                && let Some(jt) = joining_global.consensus_transport()
            {
                for existing in &nodes[..nodes.len() - 1] {
                    // Skip bootstrap node — already connected via JoinCluster RPC.
                    if existing.id == nodes[0].id {
                        continue;
                    }
                    let addr_str = existing.addr.to_string();
                    let _ = jt.set_peer_via_registry(existing.id, &addr_str).await;
                    // Reverse direction: existing node → joining node.
                    if let Ok(existing_global) = existing.manager.system_region()
                        && let Some(et) = existing_global.consensus_transport()
                    {
                        let join_addr_str = join_addr.clone();
                        let _ = et.set_peer_via_registry(node_id, &join_addr_str).await;
                    }
                }
            }

            // Start data regions on the joining node WITHOUT bootstrap (bootstrap: false).
            // The joining node creates the Raft group but doesn't initialize it — it will
            // receive the cluster membership from the leader via Raft log replication.
            let joining_manager = nodes.last().unwrap().manager.clone();
            let joining_node_id = node_id;
            let _joining_addr = join_addr.clone();
            for &data_region in data_regions.iter().take(num_data_regions) {
                let data_region_config = RegionConfig {
                    region: data_region,
                    initial_members: vec![], // empty — will be added via change_membership
                    bootstrap: false,        // don't bootstrap — join existing cluster
                    enable_background_jobs: true,
                    batch_writer_config: None,
                    event_writer: None,
                    events_config: None,
                    delegated_leadership: false,
                    // Test fixture: bypasses the `CreateDataRegion` apply
                    // path. Default to non-protected so the protected-region
                    // quorum check doesn't fire on the joining node's empty
                    // initial-members vec.
                    requires_residency_hint: false,
                };
                joining_manager.start_data_region(data_region_config).await.unwrap_or_else(|e| {
                    panic!(
                        "start data region {:?} on joining node {}: {e}",
                        data_region, joining_node_id
                    )
                });

                // Add this joining node to EVERY shard's Raft cluster via
                // membership change. Phase A multi-shard: each shard runs an
                // independent Raft group, so a single `add_learner` against
                // shard 0 only joins shard 0. Writes routed to other shards
                // (via `ShardRouter`) would never replicate to the joining
                // node, breaking read-after-failover assertions. Iterate
                // every `(region, shard)` pair the bootstrap node has
                // registered.
                let bootstrap_manager = &nodes[0].manager;
                let shards: Vec<inferadb_ledger_types::OrganizationId> = bootstrap_manager
                    .list_organization_groups()
                    .into_iter()
                    .filter(|(r, _)| *r == data_region)
                    .map(|(_, s)| s)
                    .collect();

                // Register transport once per node pair — channels are
                // multiplexed across shards by the consensus engine.
                let joining_addr_str = join_addr.clone();
                let bootstrap_addr_str = nodes[0].addr.clone();
                if let Ok(bootstrap_rg_zero) =
                    bootstrap_manager.get_organization_group(data_region, shards[0])
                    && let Some(bt) = bootstrap_rg_zero.consensus_transport()
                {
                    let _ = bt.set_peer_via_registry(joining_node_id, &joining_addr_str).await;
                }
                if let Ok(joining_rg_zero) =
                    joining_manager.get_organization_group(data_region, shards[0])
                    && let Some(jt) = joining_rg_zero.consensus_transport()
                {
                    let _ = jt.set_peer_via_registry(nodes[0].id, &bootstrap_addr_str).await;
                }

                for shard in shards {
                    if let Ok(bootstrap_rg) =
                        bootstrap_manager.get_organization_group(data_region, shard)
                    {
                        let bootstrap_handle = bootstrap_rg.handle();

                        // Per-shard transport registration — each shard's
                        // consensus engine owns its own peer routing table.
                        if let Some(bt) = bootstrap_rg.consensus_transport() {
                            let _ =
                                bt.set_peer_via_registry(joining_node_id, &joining_addr_str).await;
                        }
                        if let Ok(joining_rg) =
                            joining_manager.get_organization_group(data_region, shard)
                            && let Some(jt) = joining_rg.consensus_transport()
                        {
                            let _ =
                                jt.set_peer_via_registry(nodes[0].id, &bootstrap_addr_str).await;
                        }

                        let _ = bootstrap_handle.add_learner(joining_node_id, true).await;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        let _ = bootstrap_handle.promote_voter(joining_node_id).await;
                    }
                }
                // Wait once for all per-shard memberships to stabilize.
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            // Wait for data region leader elections on this joining node (it should
            // see a leader from the bootstrap node's cluster after membership change)
            let dr_wait_start = tokio::time::Instant::now();
            while dr_wait_start.elapsed() < Duration::from_secs(10) {
                let mut all_ready = true;
                for &dr in data_regions.iter().take(num_data_regions) {
                    if let Ok(rg) = joining_manager.get_region_group(dr) {
                        if rg.handle().current_leader().is_none() {
                            all_ready = false;
                            break;
                        }
                    } else {
                        all_ready = false;
                        break;
                    }
                }
                if all_ready {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }

        Self {
            nodes,
            num_data_regions,
            _socket_dir: socket_dir,
            transport,
            include_blinding_key,
            rate_limit_override,
            key_manager,
        }
    }

    /// Waits for a leader to be elected AND all nodes to agree.
    ///
    /// Returns the leader's node ID. Uses a generous timeout to accommodate
    /// standard Raft election semantics under CI contention.
    pub async fn wait_for_leader(&self) -> u64 {
        self.wait_for_leader_agreement(Duration::from_secs(30))
            .await
            .expect("leader election timed out")
    }

    /// Waits for a leader with timeout (any node reporting a leader).
    pub async fn wait_for_leader_timeout(&self, duration: Duration) -> Option<u64> {
        let start = tokio::time::Instant::now();

        while start.elapsed() < duration {
            for node in &self.nodes {
                if let Some(leader_id) = node.current_leader() {
                    return Some(leader_id);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        None
    }

    /// Waits for ALL nodes to agree on the same leader.
    ///
    /// This is more robust than `wait_for_leader_timeout` as it ensures
    /// leader information has propagated to all nodes.
    pub async fn wait_for_leader_agreement(&self, duration: Duration) -> Option<u64> {
        let start = tokio::time::Instant::now();

        while start.elapsed() < duration {
            let leaders: Vec<Option<u64>> = self.nodes.iter().map(|n| n.current_leader()).collect();

            // Check if all nodes report the same leader (and it's not None)
            if let Some(first) = leaders.first().copied().flatten()
                && leaders.iter().all(|&l| l == Some(first))
            {
                return Some(first);
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        None
    }

    /// Returns the current leader node.
    ///
    /// Uses consensus from node metrics rather than relying on a single node.
    pub fn leader(&self) -> Option<&TestNode> {
        // Find consensus leader ID from node metrics
        let leader_id = self.nodes.iter().filter_map(|n| n.current_leader()).next()?;

        // Return the node with that ID
        self.nodes.iter().find(|n| n.id == leader_id)
    }

    /// Returns all follower nodes.
    pub fn followers(&self) -> Vec<&TestNode> {
        self.nodes.iter().filter(|n| !n.is_leader()).collect()
    }

    /// Returns a node by ID.
    pub fn node(&self, id: u64) -> Option<&TestNode> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// Returns all nodes.
    pub fn nodes(&self) -> &[TestNode] {
        &self.nodes
    }

    /// Waits for all nodes to have the same applied index on GLOBAL **and**
    /// every active data region.
    ///
    /// Entity writes commit through regional Raft, not GLOBAL, so a sync
    /// helper that only checks GLOBAL can return `true` before regional
    /// replication has finished — producing false-negative reads from
    /// followers in tests. This helper waits for apply-level convergence
    /// across every region the cluster owns.
    ///
    /// For callers that genuinely only need GLOBAL convergence (e.g.
    /// membership-only assertions) use [`wait_for_global_sync`]. For
    /// data-region-only convergence use [`wait_for_data_region_sync`].
    #[allow(dead_code)]
    pub async fn wait_for_sync(&self, timeout_duration: Duration) -> bool {
        timeout(timeout_duration, async {
            loop {
                if self.global_synced() && self.data_regions_synced() {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or(false)
    }

    /// Waits for the GLOBAL Raft group to converge across all nodes.
    ///
    /// Narrow counterpart to [`wait_for_sync`]. Use when the assertion under
    /// test only depends on GLOBAL state (e.g. cluster membership,
    /// orchestrator state) and waiting for data-region convergence would
    /// add unnecessary latency.
    #[allow(dead_code)]
    pub async fn wait_for_global_sync(&self, timeout_duration: Duration) -> bool {
        timeout(timeout_duration, async {
            loop {
                if self.global_synced() {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or(false)
    }

    /// Waits for data region Raft groups to sync across all nodes.
    ///
    /// Narrow counterpart to [`wait_for_sync`]. Prefer [`wait_for_sync`]
    /// unless the assertion under test only depends on data-region state.
    #[allow(dead_code)]
    pub async fn wait_for_data_region_sync(&self, timeout_duration: Duration) -> bool {
        timeout(timeout_duration, async {
            loop {
                if self.data_regions_synced() {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or(false)
    }

    /// Returns `true` when every node's GLOBAL commit index matches and is
    /// positive.
    fn global_synced(&self) -> bool {
        let indices: Vec<u64> = self.nodes.iter().map(|n| n.last_applied()).collect();
        !indices.is_empty() && indices.iter().all(|&i| i == indices[0] && i > 0)
    }

    /// Returns `true` when every data region's applied index matches across
    /// all nodes and is positive. Compares applied (state-machine visible)
    /// rather than commit index: EVENTUAL reads serve from applied state,
    /// so callers need apply-level convergence.
    ///
    /// Phase A multi-shard: each region runs N independent Raft groups.
    /// Convergence is a per-`(region, shard)` property — waiting for shard
    /// 0 alone leaves writes routed to other shards racing the assertion.
    /// `applied_index` for a shard with no traffic is `0`; we skip that
    /// case (an idle shard is "synced" by definition since there is
    /// nothing to apply yet).
    ///
    /// Per-vault consensus: vault entity writes route through per-vault
    /// Raft shards, NOT the parent organization shard. Convergence on the
    /// org shard alone returns `true` while vault apply pipelines are still
    /// catching up — producing stale follower reads in tests. Iterates
    /// every `(region, organization_id, vault_id)` triple registered on the
    /// reference node and compares `applied_index_watch` across all nodes
    /// for each.
    fn data_regions_synced(&self) -> bool {
        // Derive the set of actually-started data regions from the reference
        // node's manager rather than a fixed count. Data regions may be
        // brought up eagerly (via `with_data_regions`) or lazily (via
        // `create_data_region`) after `new`, so the manager is the source of
        // truth.
        let reference_node = &self.nodes[0];
        let started_regions: Vec<inferadb_ledger_types::Region> = reference_node
            .manager
            .list_regions()
            .into_iter()
            .filter(|r| *r != inferadb_ledger_types::Region::GLOBAL)
            .collect();
        for dr in started_regions {
            for shard_group in reference_node.shard_groups(dr) {
                let organization_id = shard_group.organization_id();
                let mut indices = Vec::new();
                for node in &self.nodes {
                    if let Ok(rg) = node.manager.get_organization_group(dr, organization_id) {
                        indices.push(*rg.applied_index_watch().borrow());
                    }
                }
                if indices.len() < self.nodes.len() {
                    // A node is missing this shard — not yet converged.
                    return false;
                }
                let first = indices[0];
                if first == 0 {
                    // Idle shard: no writes yet. Treat as synced.
                    continue;
                }
                if !indices.iter().all(|&i| i == first) {
                    return false;
                }
            }

            // Vault shards register on the parent org's `ConsensusEngine`
            // and run their own apply pipelines. Wait for every locally-
            // registered vault group to converge across nodes — entity
            // writes commit through vault shards, so org-shard convergence
            // alone is insufficient.
            let vault_triples: Vec<(
                inferadb_ledger_types::OrganizationId,
                inferadb_ledger_types::VaultId,
            )> = reference_node
                .manager
                .list_vault_groups()
                .into_iter()
                .filter(|(r, ..)| *r == dr)
                .map(|(_, o, v)| (o, v))
                .collect();
            for (organization_id, vault_id) in vault_triples {
                let mut indices = Vec::new();
                for node in &self.nodes {
                    if let Ok(vg) = node.manager.get_vault_group(dr, organization_id, vault_id) {
                        indices.push(*vg.applied_index_watch().borrow());
                    }
                }
                if indices.len() < self.nodes.len() {
                    // A node is missing this vault group — not yet
                    // converged on registration.
                    return false;
                }
                let first = indices[0];
                if first == 0 {
                    // Idle vault: no writes yet. Treat as synced.
                    continue;
                }
                if !indices.iter().all(|&i| i == first) {
                    return false;
                }
            }
        }
        true
    }

    /// Returns any node (convenience for single-node clusters or when
    /// any node will do).
    pub fn any_node(&self) -> &TestNode {
        &self.nodes[0]
    }

    /// Returns the leader node for the system (GLOBAL) region.
    pub fn system_leader(&self) -> Option<&TestNode> {
        self.nodes.iter().find(|n| n.is_system_leader())
    }

    /// Returns the number of data regions.
    pub fn num_data_regions(&self) -> usize {
        self.num_data_regions
    }

    /// Returns all node addresses.
    pub fn addrs(&self) -> Vec<String> {
        self.nodes.iter().map(|n| n.addr.clone()).collect()
    }

    // ========================================================================
    // B.1 helpers — on-demand data region + organization creation.
    //
    // Phase B.1 replaces the eager `with_data_regions` bootstrap with a lean
    // `new(size)` bootstrap plus explicit `create_data_region` /
    // `create_organization` steps. Tests exercising organization isolation
    // and multi-tier orchestration should use these helpers. The legacy
    // `with_data_regions` surface is retained until every existing test is
    // migrated (B.1 Task 14).
    // ========================================================================

    /// Proposes `SystemRequest::CreateDataRegion` to the system (GLOBAL)
    /// group and waits for every node's region group for `region` to elect
    /// a leader.
    ///
    /// The bootstrap-installed region-creation handler on every node picks
    /// up the applied entry and calls `start_data_region` locally, so a
    /// single GLOBAL proposal fans out across the cluster.
    pub async fn create_data_region(
        &self,
        region: inferadb_ledger_types::Region,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.create_data_region_with_protection(region, false).await
    }

    /// Like [`TestCluster::create_data_region`] but allows the caller to
    /// mark the region as protected. Protected regions require nodes to
    /// opt in via `--regions <name>` to host them.
    ///
    /// When `protected = true`, the helper does NOT wait for every node to
    /// register the region — only nodes whose `--regions` opt-in matches
    /// will start the group. Callers asserting which nodes hosted the
    /// region must inspect `manager.has_region(region)` themselves.
    pub async fn create_data_region_with_protection(
        &self,
        region: inferadb_ledger_types::Region,
        protected: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Default helper: residency tracks the protection flag (historical
        // semantics) and retention defaults to 90 days. Tests exercising the
        // R6 residency contract directly should call
        // [`create_data_region_with_residency`] with explicit values.
        self.create_data_region_with_residency(region, protected, protected, 90).await
    }

    /// Like [`TestCluster::create_data_region_with_protection`] but exposes
    /// the full residency contract (`requires_residency`, `retention_days`).
    /// Use this for tests that depend on the registry-driven behaviour
    /// added in R6 — e.g. asserting that an EU region with
    /// `retention_days=30` is honoured by the soft-delete reaper.
    pub async fn create_data_region_with_residency(
        &self,
        region: inferadb_ledger_types::Region,
        protected: bool,
        requires_residency: bool,
        retention_days: u32,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use inferadb_ledger_raft::types::{RaftPayload, SystemRequest};

        // Pre-flight: wait until every node's `peer_addresses` map
        // contains all other nodes' addresses. CreateDataRegion's apply
        // path registers peer transports from `initial_members`, but the
        // saga orchestrator's `propose_to_region` path — which runs for
        // subsequent CreateOrganization calls — resolves peers via
        // `manager.peer_addresses()` (populated through the GLOBAL
        // `RegisterPeerAddress` apply). Without this wait, a multi-node
        // cluster can race: CreateDataRegion applies + elects a leader
        // before every node has applied every peer's
        // `RegisterPeerAddress` entry, so follow-on saga RPCs from a
        // non-leader node can't route to the region leader. This
        // matches the `build_full` eager path's explicit `JoinCluster`
        // propagation without the eager region bootstrap.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let all_synced = self.nodes.iter().all(|n| {
                // `peer_addresses` is the org-wide map; it includes self
                // on some code paths and not others — the robust check is
                // that every OTHER node's id is present.
                let peers = n.manager.peer_addresses();
                self.nodes
                    .iter()
                    .filter(|other| other.id != n.id)
                    .all(|other| peers.get(other.id).is_some())
            });
            if all_synced {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "peer_addresses did not propagate to all {} nodes within 10s",
                    self.nodes.len()
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Find the current system leader — `create_data_region` must be
        // proposed on the leader's handle (followers return NotLeader).
        let leader = self
            .nodes
            .iter()
            .find(|n| n.is_system_leader())
            .ok_or("create_data_region: no system leader")?;

        let initial_members: Vec<(u64, String)> =
            self.nodes.iter().map(|n| (n.id, n.addr.clone())).collect();

        let req = SystemRequest::CreateDataRegion {
            region,
            protected,
            requires_residency,
            retention_days,
            initial_members,
        };
        let resp = leader
            .handle
            .propose_and_wait(RaftPayload::system(req), Duration::from_secs(10))
            .await
            .map_err(|e| format!("CreateDataRegion propose failed: {e}"))?;

        match resp {
            inferadb_ledger_raft::types::LedgerResponse::DataRegionCreated { region: r } => {
                assert_eq!(r, region, "DataRegionCreated region mismatch");
            },
            other => return Err(format!("expected DataRegionCreated, got {other}").into()),
        }

        // For protected regions, the cluster's nodes do NOT have an
        // opt-in (`--regions <name>`), so the region-creation handler
        // skips the local start. The directory entry is persisted; the
        // helper returns immediately and lets the caller assert the
        // skip behavior or restart specific nodes with opt-in.
        if protected {
            // Brief settle so the apply path persists the directory entry
            // and every node observes it on a subsequent scan.
            tokio::time::sleep(Duration::from_millis(200)).await;
            return Ok(());
        }

        // Wait for every node to register the region group and elect a
        // leader. The region-creation handler is fire-and-forget from the
        // apply path's perspective, so we poll for registration + leader
        // election on every node.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let all_ready = self.nodes.iter().all(|n| {
                n.manager.has_region(region)
                    && n.manager
                        .get_region_group(region)
                        .map(|g| g.handle().current_leader().is_some())
                        .unwrap_or(false)
            });
            if all_ready {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!("region {region:?} did not converge within 30s").into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Colocate the region leader with the system (GLOBAL) leader.
        //
        // Tests use `.leader().addr` (the system leader) as the entry point
        // for writes that ultimately route through the per-organization
        // groups. Per-organization groups default to `Delegated` leadership
        // and follow their parent `RegionGroup`'s leader via
        // `adopt_leader`, so if the region leader is a different node than
        // the system leader, per-org writes on the system leader return
        // `NotLeader` — the test helpers that use direct tonic clients
        // (e.g. `create_test_vault`, `write_entity`) don't follow
        // `LeaderHint` redirects, so the test hangs/fails.
        //
        // The production SDK (`RegionLeaderCache`) DOES follow redirects;
        // this colocation step is only needed for the direct-tonic-client
        // test harness. Transfer is a no-op when the leaders are already
        // colocated.
        let system_leader_id = self
            .nodes
            .iter()
            .find(|n| n.is_system_leader())
            .map(|n| n.id)
            .ok_or("create_data_region: no system leader for colocation step")?;
        let region_leader_id = self
            .nodes
            .iter()
            .find_map(|n| {
                n.manager.get_region_group(region).ok().and_then(|g| g.handle().current_leader())
            })
            .ok_or("create_data_region: no region leader for colocation step")?;
        if region_leader_id != system_leader_id {
            // Only the current region leader can initiate a transfer.
            let current_region_leader = self
                .nodes
                .iter()
                .find(|n| n.id == region_leader_id)
                .ok_or("create_data_region: region leader not in cluster")?;
            if let Ok(rg) = current_region_leader.manager.get_region_group(region) {
                let _ = rg.handle().transfer_leader(system_leader_id).await;
            }
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                let colocated = self.nodes.iter().all(|n| {
                    n.manager
                        .get_region_group(region)
                        .ok()
                        .and_then(|g| g.handle().current_leader())
                        == Some(system_leader_id)
                });
                if colocated {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(format!(
                        "region {region:?} leader did not colocate with system leader within 10s"
                    )
                    .into());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }

        Ok(())
    }

    /// Proposes `SystemRequest::CreateOrganization` to the system group.
    ///
    /// Waits for the multi-tier orchestration (system apply → region
    /// placement signal → per-voter `start_organization_group`) to complete
    /// on every node, then returns the allocated `OrganizationId`.
    ///
    /// `slug` is a human-readable identifier used to derive a deterministic
    /// external Snowflake via a fresh `generate_organization_slug()` call.
    /// The input string is not currently embedded in the organization's
    /// display name — that's the saga orchestrator's job, which this bypass
    /// doesn't exercise.
    pub async fn create_organization(
        &self,
        region: inferadb_ledger_types::Region,
        _slug: &str,
    ) -> Result<inferadb_ledger_types::OrganizationId, Box<dyn std::error::Error + Send + Sync>>
    {
        use inferadb_ledger_raft::types::{RaftPayload, SystemRequest};

        let leader = self
            .nodes
            .iter()
            .find(|n| n.is_system_leader())
            .ok_or("create_organization: no system leader")?;

        let org_slug = inferadb_ledger_types::snowflake::generate_organization_slug()
            .map_err(|e| format!("generate_organization_slug: {e}"))?;

        let req = SystemRequest::CreateOrganization {
            slug: org_slug,
            region,
            tier: inferadb_ledger_state::system::OrganizationTier::Free,
            admin: inferadb_ledger_types::UserId::new(0),
        };

        let resp = leader
            .handle
            .propose_and_wait(RaftPayload::system(req), Duration::from_secs(10))
            .await
            .map_err(|e| format!("CreateOrganization propose failed: {e}"))?;

        let organization_id = match resp {
            inferadb_ledger_raft::types::LedgerResponse::OrganizationCreated {
                organization_id,
                ..
            } => organization_id,
            other => return Err(format!("expected OrganizationCreated, got {other}").into()),
        };

        // Wait for the per-org group to start on every node. The
        // organization-creation handler on each node is fire-and-forget,
        // so we poll `has_organization_group` across the cluster.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let all_started = self
                .nodes
                .iter()
                .all(|n| n.manager.has_organization_group(region, organization_id));
            if all_started {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "organization {organization_id} group did not start on all nodes within 30s"
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        Ok(organization_id)
    }

    /// Returns the `OrganizationGroup` on `node_idx` for
    /// `(region, organization_id)`. Panics if the node is not a region voter
    /// or the group is not started.
    pub fn organization_group(
        &self,
        node_idx: usize,
        region: inferadb_ledger_types::Region,
        organization_id: inferadb_ledger_types::OrganizationId,
    ) -> Arc<OrganizationGroup> {
        self.nodes[node_idx].manager.get_organization_group(region, organization_id).unwrap_or_else(
            |e| panic!("organization_group({node_idx}, {region:?}, {organization_id}): {e}"),
        )
    }

    /// Returns the region control-plane group on `node_idx` for `region`.
    ///
    /// Returns `Arc<RegionGroup>` (B.1 Task 3 — three-tier type split).
    /// `region_group_at(node, GLOBAL)` now refers to the same regional
    /// control-plane storage as the system tier's organization-0 record,
    /// but the returned wrapper exposes only regional-tier accessors; if
    /// a test needs system-tier accessors it should call
    /// [`TestNode::system_region`] instead.
    pub fn region_group_at(
        &self,
        node_idx: usize,
        region: inferadb_ledger_types::Region,
    ) -> Arc<RegionGroup> {
        self.nodes[node_idx]
            .manager
            .get_region_group(region)
            .unwrap_or_else(|e| panic!("region_group_at({node_idx}, {region:?}): {e}"))
    }

    /// Returns `true` when every organization group in `region` has
    /// converged across all nodes (applied indices match and are > 0).
    ///
    /// Idle groups (applied index 0 on every node) are treated as synced —
    /// there is nothing to converge yet. Groups with traffic must match on
    /// every node to count as converged.
    pub fn organizations_synced(&self, region: inferadb_ledger_types::Region) -> bool {
        let reference = &self.nodes[0];
        let org_ids: Vec<inferadb_ledger_types::OrganizationId> = reference
            .manager
            .list_organization_groups()
            .into_iter()
            .filter(|(r, _)| *r == region)
            .map(|(_, org)| org)
            .collect();

        if org_ids.is_empty() {
            // No organization groups registered for this region — treat as
            // synced (nothing to wait for).
            return true;
        }

        for org_id in org_ids {
            let mut indices = Vec::with_capacity(self.nodes.len());
            for node in &self.nodes {
                match node.manager.get_organization_group(region, org_id) {
                    Ok(group) => indices.push(*group.applied_index_watch().borrow()),
                    Err(_) => return false, // missing group on a node
                }
            }
            let first = indices[0];
            if first == 0 {
                // Idle group on the reference node — only synced if every
                // other node is also idle.
                if !indices.iter().all(|&i| i == 0) {
                    return false;
                }
                continue;
            }
            if !indices.iter().all(|&i| i == first) {
                return false;
            }
        }
        true
    }

    /// Polls until every node has an elected leader for the region group
    /// at `region`, up to 30 seconds. Returns the leader's node id. Panics
    /// on timeout.
    pub async fn wait_for_region_leader(&self, region: inferadb_ledger_types::Region) -> u64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            // All nodes must agree on a non-None leader for this region.
            let leaders: Vec<Option<u64>> = self
                .nodes
                .iter()
                .map(|n| {
                    n.manager
                        .get_region_group(region)
                        .ok()
                        .and_then(|g| g.handle().current_leader())
                })
                .collect();

            if let Some(first) = leaders.first().copied().flatten()
                && leaders.iter().all(|&l| l == Some(first))
            {
                return first;
            }

            if tokio::time::Instant::now() >= deadline {
                panic!("wait_for_region_leader({region:?}): no agreed leader after 30s");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Polls `organizations_synced(region)` until true or `timeout` elapses.
    pub async fn wait_for_organizations_synced(
        &self,
        region: inferadb_ledger_types::Region,
        timeout_duration: Duration,
    ) {
        let deadline = tokio::time::Instant::now() + timeout_duration;
        while tokio::time::Instant::now() < deadline {
            if self.organizations_synced(region) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // Don't panic — match the existing `wait_for_*_sync` helpers, which
        // return silently on timeout so callers can assert explicitly.
    }

    /// Waits until all regions (GLOBAL + data) on the first node have elected
    /// leaders. Used by multi-region tests to ensure the cluster is fully ready.
    pub async fn wait_for_leaders(&self, timeout_duration: Duration) -> bool {
        let start = tokio::time::Instant::now();

        while start.elapsed() < timeout_duration {
            let mut all_ready = true;

            if let Some(node) = self.nodes.first() {
                for region in node.regions() {
                    if let Some(region_group) = node.region_group(region) {
                        if region_group.handle().current_leader().is_none() {
                            all_ready = false;
                            break;
                        }
                    } else {
                        all_ready = false;
                        break;
                    }
                }
            }

            if all_ready {
                return true;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        false
    }

    /// Gracefully shuts down every node in the cluster, then re-bootstraps
    /// each node against the same on-disk state (data_dir, cluster_id, node
    /// id, socket / listen address, shared key manager). Returns a fresh
    /// `TestCluster` handle over the restarted nodes.
    ///
    /// This is the test-only equivalent of a whole-cluster restart:
    ///
    ///   1. Every node's WAL is flushed via `ConsensusHandle::flush_for_shutdown` so
    ///      `applied_durable == last_committed` at the moment of shutdown. Combined with (2), this
    ///      matches the production clean-shutdown contract.
    ///   2. Every node's state DBs (state.db + raft.db, per region, per vault) are synced via
    ///      `RaftManager::sync_all_state_dbs` — the same call `main.rs`'s `pre_shutdown` closure
    ///      makes. Without this, `applied_durable` resets to 0 on restart and the full WAL replays,
    ///      which is correct but defeats the point of a "graceful" restart.
    ///   3. The server task for each node is signalled (`shutdown_tx.send(true)`) and awaited with
    ///      a per-node timeout. Nodes that do not terminate cleanly within the timeout are
    ///      abandoned; the subsequent restart will still observe their on-disk state because the
    ///      WAL + DB syncs above already happened.
    ///   4. Each node is re-bootstrapped via `bootstrap_node` against the same data_dir. Because
    ///      the cluster_id file already exists, `bootstrap_node` takes the restart path, which
    ///      rediscovers and restarts data regions via `discover_existing_regions`, rediscovers and
    ///      rehydrates per-organization Raft groups via `discover_existing_organizations` +
    ///      `rehydrate_organization_group` (Task #151), and rehydrates per-vault Raft groups inside
    ///      `start_organization_group` (Task #146).
    ///
    /// The restart intentionally does NOT pre-populate
    /// `RaftManager::peer_addresses` — that map starts empty on each
    /// re-bootstrapped node and populates incrementally as the GLOBAL log
    /// replays (for `RegisterPeerAddress` entries). Rehydration of
    /// per-organization groups with an empty voter set is warn-and-skip by
    /// design in `rehydrate_organization_group`; the test helpers wait long
    /// enough for `peer_addresses` to populate before relying on the groups
    /// being live.
    /// Graceful-restart the cluster, opting one node into a list of
    /// protected regions. The opt-in node is selected by index (0-based).
    /// All other nodes restart with no `--regions` flag, mirroring the
    /// production "single operator opted-in node" topology.
    ///
    /// This is the test analogue of `inferadb-ledger --regions
    /// <name1,name2>` on a single node.
    pub async fn graceful_restart_with_regions(
        self,
        opt_in_node_index: usize,
        regions: Vec<String>,
    ) -> Self {
        self.graceful_restart_inner(Some((opt_in_node_index, regions))).await
    }

    pub async fn graceful_restart(self) -> Self {
        self.graceful_restart_inner(None).await
    }

    async fn graceful_restart_inner(self, opt_in: Option<(usize, Vec<String>)>) -> Self {
        // Destructure the cluster so we can move per-node blueprints out of
        // the nodes Vec without hitting partial-move errors through a &mut.
        let Self {
            nodes,
            num_data_regions,
            _socket_dir,
            transport,
            include_blinding_key,
            rate_limit_override,
            key_manager,
        } = self;

        // Phase 1: drain background tasks, flush WAL, sync state DBs, then
        // stop the gRPC server per node. Approximates the production
        // `main.rs` shutdown ordering:
        //   a. Cancel the coordinator's root token so background tasks
        //      exit their loops. `root_token.cancel()` is a fast signal;
        //      we do NOT await each handle (the coordinator's default
        //      30s-per-handle deadline balloons test time unacceptably
        //      — cancelled tasks drop their file handles on drop of
        //      their enclosing Arcs below, which is sufficient for the
        //      restart to re-open the same paths).
        //   b. `flush_for_shutdown` on the consensus handle pushes any
        //      pending WAL writes out.
        //   c. `sync_all_state_dbs` force-syncs state.db + raft.db per
        //      region + vault, narrowing post-restart WAL replay to zero.
        //   d. `shutdown_tx.send(true)` signals the gRPC listener to
        //      stop; await the task with a timeout.
        let mut blueprints: Vec<NodeRestartBlueprint> = Vec::with_capacity(nodes.len());
        for mut node in nodes.into_iter() {
            // Flush + state DB sync FIRST so clean-shutdown durability is
            // preserved before we tear down the consensus engines.
            let _ = node.handle.flush_for_shutdown(Duration::from_secs(5)).await;
            node.manager.sync_all_state_dbs(Duration::from_secs(5)).await;

            // Drain the coordinator — cancels child tokens and awaits the
            // registered background handles. Wrapped in an outer timeout
            // because the coordinator's default per-handle deadline is 30s
            // and we have many handles; without the outer cap the test
            // deadline balloons.
            if let Some(coordinator) = node.coordinator.take() {
                let _ = tokio::time::timeout(Duration::from_secs(15), async move {
                    coordinator.shutdown().await;
                })
                .await;
            }

            // Tear down every region's consensus engine. `stop_region`
            // drops the engine (which closes its control_inbox and
            // propagates to the reactor task), flushes pending WAL entries,
            // and releases the log-store handle. Without this, the OLD
            // reactor task keeps running on the OLD WAL files even after
            // the gRPC server stops — the NEW `bootstrap_node` call then
            // opens the same paths while the OLD engine still holds them,
            // and the NEW engine's election timers never fire (racy shared
            // state on the backing files).
            let _ = tokio::time::timeout(Duration::from_secs(15), node.manager.shutdown()).await;

            if let Some(tx) = node._shutdown_tx.take() {
                let _ = tx.send(true);
            }
            if let Some(handle) = node._server_handle.take() {
                let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
            }

            // Dropping `manager` / `handle` / `state` is essential — their
            // background tasks hold file descriptors and channel
            // references on the data_dir. Moving them into `_` forces the
            // drop BEFORE the new bootstrap re-opens the same paths.
            let TestNode {
                id,
                addr,
                handle: _,
                state: _,
                manager: _,
                _temp_dir,
                socket_path,
                listen,
                ..
            } = node;

            let temp_dir = _temp_dir.expect(
                "graceful_restart: temp_dir must be live for the node's entire pre-restart \
                 lifetime",
            );
            blueprints.push(NodeRestartBlueprint { id, addr, temp_dir, socket_path, listen });
        }

        // Give dropped tasks a tick to actually release file handles.
        // Some background tokio tasks spawned off the coordinator's child
        // tokens observe cancellation on their next loop iteration; yielding
        // plus a short sleep lets them run to completion before the new
        // bootstrap reopens the same paths.
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Phase 2: re-bootstrap each node against the same data_dir. The
        // `cluster_id` file inside `temp_dir` is already populated from the
        // initial `build_full` pass, so `bootstrap_node` takes the restart
        // path.
        //
        // Collect the TCP join-seed list once — if `transport == Tcp` the
        // restart path spawns a background seed-discovery task that
        // populates `peer_addresses` as soon as every re-bootstrapped node
        // is listening. For UDS transport, `parse_seed_addresses` rejects
        // non-`SocketAddr` strings, so the join list is empty and peers
        // must auto-register via incoming consensus traffic instead — a
        // node that elects itself leader on restart will broadcast
        // RequestVote using the transport's auto-registration of the
        // `from_address` field carried on every consensus RPC.
        let join_seeds: Vec<String> = match transport {
            TestTransport::Tcp => blueprints.iter().map(|b| b.addr.clone()).collect(),
            TestTransport::Uds => Vec::new(),
        };

        let mut restarted: Vec<TestNode> = Vec::with_capacity(blueprints.len());
        for (idx, blueprint) in blueprints.into_iter().enumerate() {
            // Seed list excludes the node's own address so the node doesn't
            // discover itself. Leaving self in the list is harmless (the
            // restart seed loop filters by `info.node_id != my_node_id`)
            // but cleaner to scope here.
            let seeds_for_node: Option<Vec<String>> = if join_seeds.is_empty() {
                None
            } else {
                let own_addr = &blueprint.addr;
                let seeds: Vec<String> =
                    join_seeds.iter().filter(|a| *a != own_addr).cloned().collect();
                if seeds.is_empty() { None } else { Some(seeds) }
            };
            let regions_for_node: Vec<String> = match opt_in {
                Some((target_idx, ref regions)) if target_idx == idx => regions.clone(),
                _ => Vec::new(),
            };
            let node = bootstrap_one_node_with_regions(
                blueprint,
                key_manager.clone(),
                include_blinding_key,
                rate_limit_override.clone(),
                seeds_for_node,
                regions_for_node,
            )
            .await;
            restarted.push(node);
        }

        // Phase 3: fan-out peer registration across all restarted nodes.
        //
        // The production restart path relies on `RegisterPeerAddress` Raft
        // log entries re-applying during log replay to repopulate
        // `RaftManager::peer_addresses`. After clean shutdown
        // `applied_durable == last_committed`, so no entries re-apply —
        // meaning the map stays empty on restart until some external
        // mechanism (the `--join` seed discovery task, or a peer
        // initiating a consensus RPC and auto-registering via
        // `from_address`) re-injects entries.
        //
        // The seed discovery task is a one-shot best-effort spawn from
        // `bootstrap_node` and runs in parallel with `bootstrap_one_node`
        // returning — when the entire cluster restarts simultaneously,
        // every seed-discovery task's targets are either mid-bootstrap or
        // not-yet-listening, so none of them succeed cleanly. The test
        // therefore performs the peer-registration step explicitly here:
        // iterate every pair and register (node_id, addr) via both the
        // shared peer-address map AND the system-region consensus
        // transport (so `RequestVote` / `AppendEntries` have a channel to
        // use).
        //
        // This is a TEST-HARNESS workaround for a production gap —
        // production nodes use `--join` seeds to bootstrap peer addresses.
        // The assertion below tests the rehydration chain CONDITIONAL on
        // peer addresses being known, which is the invariant the
        // production code relies on.
        for node in &restarted {
            for other in &restarted {
                if other.id == node.id {
                    continue;
                }
                node.manager.peer_addresses().insert(other.id, other.addr.clone());
            }
            // `reconcile_transport_channels` walks every region's consensus
            // transport and registers every known peer that isn't already
            // in the transport's channel map — this is the same helper the
            // production `PlacementController` uses on its periodic
            // reconciliation cycle. It covers every region, not just the
            // system region, so data-region elections can proceed too.
            inferadb_ledger_server::placement::reconcile_transport_channels(&node.manager).await;
        }

        // The rehydration sweep inside `bootstrap_node` runs with empty
        // `peer_addresses` (the WAL replay of `RegisterPeerAddress` entries
        // has nothing to replay after clean shutdown, and seed discovery is
        // an async background task that races the sweep). Skipping nodes
        // mean the per-organization groups are NOT started on every voter
        // at this point. Now that `peer_addresses` is populated, walk the
        // on-disk organization directories on every restarted node and
        // invoke `start_organization_group` directly — the call is
        // idempotent (`Ok(_)` on "already running"), so nodes where the
        // first sweep succeeded are no-ops.
        //
        // This mirrors exactly what `bootstrap.rs`'s restart-path
        // rehydration block does, including the same bootstrap-selection
        // rule (smallest node id bootstraps). The test re-runs the sweep
        // here because the production sweep is synchronous inside
        // `bootstrap_node` and there's no re-trigger hook.
        let voter_set: Vec<(u64, String)> =
            restarted.iter().map(|n| (n.id, n.addr.clone())).collect();
        let bootstrap_node_id = voter_set.iter().map(|(id, _)| *id).min().unwrap_or(0);
        for node in &restarted {
            let mgr = node.manager.clone();
            let storage = inferadb_ledger_raft::RegionStorageManager::new(
                node._temp_dir
                    .as_ref()
                    .expect("temp_dir captured post-bootstrap")
                    .path()
                    .to_path_buf(),
            );
            for region in mgr.list_regions() {
                if region == inferadb_ledger_types::Region::GLOBAL {
                    continue;
                }
                let Ok(org_ids) = storage.discover_existing_organizations(region) else {
                    continue;
                };
                for organization_id in org_ids {
                    let bootstrap = node.id == bootstrap_node_id;
                    let _ = mgr
                        .start_organization_group(
                            region,
                            organization_id,
                            voter_set.clone(),
                            bootstrap,
                            None,
                            None,
                        )
                        .await;
                }
            }
        }

        Self {
            nodes: restarted,
            num_data_regions,
            _socket_dir,
            transport,
            include_blinding_key,
            rate_limit_override,
            key_manager,
        }
    }
}

/// Preserved per-node state required to re-bootstrap a node against the same
/// on-disk data directory, node id, and socket / TCP address.
///
/// The `temp_dir` ownership is critical: it outlives the original server
/// task's file descriptors, so the new `bootstrap_node` call opens the same
/// paths.
struct NodeRestartBlueprint {
    id: u64,
    addr: String,
    temp_dir: TestDir,
    socket_path: Option<std::path::PathBuf>,
    listen: Option<std::net::SocketAddr>,
}

/// Re-bootstraps a single node against its preserved data_dir + socket /
/// listen address. Used exclusively by `TestCluster::graceful_restart`.
///
/// The node's on-disk `cluster_id` file (written by `build_full` on first
/// boot) steers `bootstrap_node` into the restart path, so this helper does
/// NOT write the file again. The same shared `key_manager` from the cluster
/// is passed through so region keys unseal identically.
async fn bootstrap_one_node(
    blueprint: NodeRestartBlueprint,
    key_manager: Arc<dyn inferadb_ledger_store::crypto::RegionKeyManager>,
    include_blinding_key: bool,
    rate_limit_override: Option<inferadb_ledger_types::config::RateLimitConfig>,
    join_seeds: Option<Vec<String>>,
) -> TestNode {
    bootstrap_one_node_with_regions(
        blueprint,
        key_manager,
        include_blinding_key,
        rate_limit_override,
        join_seeds,
        Vec::new(),
    )
    .await
}

/// Like [`bootstrap_one_node`] but accepts an explicit `regions` opt-in
/// list. Used by tests that need to verify protected-region opt-in
/// behavior.
async fn bootstrap_one_node_with_regions(
    blueprint: NodeRestartBlueprint,
    key_manager: Arc<dyn inferadb_ledger_store::crypto::RegionKeyManager>,
    include_blinding_key: bool,
    rate_limit_override: Option<inferadb_ledger_types::config::RateLimitConfig>,
    join_seeds: Option<Vec<String>>,
    regions: Vec<String>,
) -> TestNode {
    let NodeRestartBlueprint { id: node_id_hint, addr, temp_dir, socket_path, listen } = blueprint;
    let data_dir = temp_dir.path().to_path_buf();

    let backup_config = inferadb_ledger_types::config::BackupConfig::builder()
        .destination(data_dir.join("backups").to_string_lossy().to_string())
        .build()
        .expect("valid backup config");

    let rate_limit_cfg = rate_limit_override.unwrap_or_else(test_rate_limit_config);
    // Per the DSoT migration: the CLI override is left as `None` and the
    // inner `RateLimitConfig::enabled` field drives the master switch.
    let config = inferadb_ledger_server::config::Config {
        listen,
        socket: socket_path.clone(),
        metrics_addr: None,
        data_dir: Some(data_dir.clone()),
        backup: Some(backup_config),
        raft: Some(test_raft_config()),
        saga: inferadb_ledger_types::config::SagaConfig { poll_interval_secs: 2 },
        token_maintenance_interval_secs: 3,
        ratelimit: None,
        rate_limit: Some(rate_limit_cfg),
        email_blinding_key: if include_blinding_key {
            Some(TEST_BLINDING_KEY_HEX.to_string())
        } else {
            None
        },
        join: join_seeds,
        regions,
        ..inferadb_ledger_server::config::Config::default()
    };

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let health_state = inferadb_ledger_raft::HealthState::new();
    let bootstrapped = inferadb_ledger_server::bootstrap::bootstrap_node(
        &config,
        &data_dir,
        health_state.clone(),
        shutdown_rx,
        Some(key_manager),
    )
    .await
    .expect("bootstrap node (restart path)");

    health_state.mark_ready();

    // `bootstrap_node` re-derives node_id from the cluster-id file + stored
    // data, so it must return the SAME id as before the restart. A mismatch
    // here means bootstrap lost the node's identity — assert eagerly so the
    // downstream assertion in the test reports a clean failure mode.
    let node_id = bootstrapped.handle.node_id();
    assert_eq!(
        node_id, node_id_hint,
        "graceful_restart: node id changed across restart (before={}, after={}) — data_dir / \
         cluster_id mismatch",
        node_id_hint, node_id,
    );

    let handle_clone = bootstrapped.handle.clone();
    let state_clone = bootstrapped.state.clone();
    let manager_clone = bootstrapped.manager.clone();
    let coordinator = bootstrapped.coordinator.clone();
    let bg_server_handle = bootstrapped.server_handle;
    let server_handle = tokio::spawn(async move {
        let _ = bg_server_handle.await;
    });

    TestNode {
        id: node_id,
        addr,
        handle: handle_clone,
        state: state_clone,
        manager: manager_clone,
        _temp_dir: Some(temp_dir),
        _server_handle: Some(server_handle),
        _shutdown_tx: Some(shutdown_tx),
        coordinator: Some(coordinator),
        socket_path,
        listen,
    }
}

/// Helper to create a gRPC write client for a node.
#[allow(dead_code)]
pub async fn create_write_client(
    addr: &str,
) -> Result<
    inferadb_ledger_proto::proto::write_service_client::WriteServiceClient<
        tonic::transport::Channel,
    >,
    tonic::transport::Error,
> {
    let channel = connect_channel(addr);
    Ok(inferadb_ledger_proto::proto::write_service_client::WriteServiceClient::new(channel))
}

/// Helper to create a read client for a node.
#[allow(dead_code)]
pub async fn create_read_client(
    addr: &str,
) -> Result<
    inferadb_ledger_proto::proto::read_service_client::ReadServiceClient<tonic::transport::Channel>,
    tonic::transport::Error,
> {
    let channel = connect_channel(addr);
    Ok(inferadb_ledger_proto::proto::read_service_client::ReadServiceClient::new(channel))
}

/// Helper to create a health client for a node.
#[allow(dead_code)]
pub async fn create_health_client(
    addr: &str,
) -> Result<
    inferadb_ledger_proto::proto::health_service_client::HealthServiceClient<
        tonic::transport::Channel,
    >,
    tonic::transport::Error,
> {
    let channel = connect_channel(addr);
    Ok(inferadb_ledger_proto::proto::health_service_client::HealthServiceClient::new(channel))
}

/// Helper to create an admin client for a node.
#[allow(dead_code)]
pub async fn create_admin_client(
    addr: &str,
) -> Result<
    inferadb_ledger_proto::proto::admin_service_client::AdminServiceClient<
        tonic::transport::Channel,
    >,
    tonic::transport::Error,
> {
    let channel = connect_channel(addr);
    Ok(inferadb_ledger_proto::proto::admin_service_client::AdminServiceClient::new(channel))
}

/// Helper to create an organization client for a node.
#[allow(dead_code)]
pub async fn create_organization_client(
    addr: &str,
) -> Result<
    inferadb_ledger_proto::proto::organization_service_client::OrganizationServiceClient<
        tonic::transport::Channel,
    >,
    tonic::transport::Error,
> {
    let channel = connect_channel(addr);
    Ok(inferadb_ledger_proto::proto::organization_service_client::OrganizationServiceClient::new(
        channel,
    ))
}

// ============================================================================
// External Cluster Infrastructure (for integration tests against running cluster)
// ============================================================================

/// An external cluster started by a shell script.
///
/// Reads endpoints from the `LEDGER_ENDPOINTS` environment variable.
/// Returns `None` from `from_env()` if the variable is unset, allowing
/// tests to gracefully skip when no cluster is available.
pub struct ExternalCluster {
    endpoints: Vec<String>,
}

impl ExternalCluster {
    /// Reads `LEDGER_ENDPOINTS` env var (comma-separated `http://host:port`).
    ///
    /// Returns `None` if the variable is unset, allowing tests to skip.
    pub fn from_env() -> Option<Self> {
        let raw = std::env::var("LEDGER_ENDPOINTS").ok()?;
        let endpoints: Vec<String> =
            raw.split(',').map(|e| e.trim().to_string()).filter(|e| !e.is_empty()).collect();

        if endpoints.is_empty() {
            return None;
        }

        Some(Self { endpoints })
    }

    /// Returns all cluster endpoints.
    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    /// Returns the first endpoint (convenience for single-endpoint operations).
    pub fn any_endpoint(&self) -> &str {
        &self.endpoints[0]
    }

    /// Poll `GetClusterInfo` until a leader exists, with timeout.
    ///
    /// Returns the leader's endpoint URL.
    pub async fn wait_for_leader(&self, timeout_duration: Duration) -> String {
        let start = tokio::time::Instant::now();

        while start.elapsed() < timeout_duration {
            for endpoint in &self.endpoints {
                if let Ok(info) = Self::get_cluster_info(endpoint).await
                    && info.leader_id > 0
                    && let Some(leader_ep) = Self::leader_endpoint_from_info(&info, &self.endpoints)
                {
                    return leader_ep;
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        panic!("No leader elected within {:?}", timeout_duration);
    }

    /// Call `GetClusterInfo` on the given endpoint.
    pub async fn get_cluster_info(
        endpoint: &str,
    ) -> Result<
        inferadb_ledger_proto::proto::GetClusterInfoResponse,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let mut client =
            inferadb_ledger_proto::proto::admin_service_client::AdminServiceClient::connect(
                endpoint.to_string(),
            )
            .await?;

        let response =
            client.get_cluster_info(inferadb_ledger_proto::proto::GetClusterInfoRequest {}).await?;

        Ok(response.into_inner())
    }

    /// Finds the leader's endpoint URL by matching leader address from
    /// `GetClusterInfoResponse` against known endpoints.
    pub fn leader_endpoint_from_info(
        info: &inferadb_ledger_proto::proto::GetClusterInfoResponse,
        endpoints: &[String],
    ) -> Option<String> {
        let leader_member = info.members.iter().find(|m| m.is_leader)?;
        let leader_addr = &leader_member.address;

        // Match against endpoints: endpoint is "http://host:port", address is "host:port"
        endpoints.iter().find(|ep| ep.ends_with(leader_addr) || ep.contains(leader_addr)).cloned()
    }

    /// Finds non-leader endpoint URLs.
    pub fn non_leader_endpoints(
        info: &inferadb_ledger_proto::proto::GetClusterInfoResponse,
        endpoints: &[String],
    ) -> Vec<String> {
        let leader_member = info.members.iter().find(|m| m.is_leader);
        let leader_addr = leader_member.map(|m| m.address.as_str()).unwrap_or("");

        endpoints
            .iter()
            .filter(|ep| !ep.ends_with(leader_addr) && !ep.contains(leader_addr))
            .cloned()
            .collect()
    }
}

/// Helper to create a vault client for a node.
#[allow(dead_code)]
pub async fn create_vault_client(
    addr: &str,
) -> Result<
    inferadb_ledger_proto::proto::vault_service_client::VaultServiceClient<
        tonic::transport::Channel,
    >,
    tonic::transport::Error,
> {
    let channel = connect_channel(addr);
    Ok(inferadb_ledger_proto::proto::vault_service_client::VaultServiceClient::new(channel))
}

/// Helper to create a vault client from a URL string.
#[allow(dead_code)]
pub async fn create_vault_client_from_url(
    endpoint: &str,
) -> Result<
    inferadb_ledger_proto::proto::vault_service_client::VaultServiceClient<
        tonic::transport::Channel,
    >,
    tonic::transport::Error,
> {
    inferadb_ledger_proto::proto::vault_service_client::VaultServiceClient::connect(
        endpoint.to_string(),
    )
    .await
}

/// Helper to create an organization client from a URL string.
#[allow(dead_code)]
pub async fn create_organization_client_from_url(
    endpoint: &str,
) -> Result<
    inferadb_ledger_proto::proto::organization_service_client::OrganizationServiceClient<
        tonic::transport::Channel,
    >,
    tonic::transport::Error,
> {
    inferadb_ledger_proto::proto::organization_service_client::OrganizationServiceClient::connect(
        endpoint.to_string(),
    )
    .await
}

/// Helper to create a health client from a URL string.
pub async fn create_health_client_from_url(
    endpoint: &str,
) -> Result<
    inferadb_ledger_proto::proto::health_service_client::HealthServiceClient<
        tonic::transport::Channel,
    >,
    tonic::transport::Error,
> {
    inferadb_ledger_proto::proto::health_service_client::HealthServiceClient::connect(
        endpoint.to_string(),
    )
    .await
}

/// Helper to create a user service client from a URL string.
#[allow(dead_code)]
pub async fn create_user_client_from_url(
    endpoint: &str,
) -> Result<
    inferadb_ledger_proto::proto::user_service_client::UserServiceClient<tonic::transport::Channel>,
    tonic::transport::Error,
> {
    inferadb_ledger_proto::proto::user_service_client::UserServiceClient::connect(
        endpoint.to_string(),
    )
    .await
}

/// Helper to create an app service client for a node.
#[allow(dead_code)]
pub async fn create_app_client(
    addr: &str,
) -> Result<
    inferadb_ledger_proto::proto::app_service_client::AppServiceClient<tonic::transport::Channel>,
    tonic::transport::Error,
> {
    let channel = connect_channel(addr);
    Ok(inferadb_ledger_proto::proto::app_service_client::AppServiceClient::new(channel))
}

pub async fn create_token_client(
    addr: &str,
) -> Result<
    inferadb_ledger_proto::proto::token_service_client::TokenServiceClient<
        tonic::transport::Channel,
    >,
    tonic::transport::Error,
> {
    let channel = connect_channel(addr);
    Ok(inferadb_ledger_proto::proto::token_service_client::TokenServiceClient::new(channel))
}

/// Helper to create a user service client for a node.
#[allow(dead_code)]
pub async fn create_user_client(
    addr: &str,
) -> Result<
    inferadb_ledger_proto::proto::user_service_client::UserServiceClient<tonic::transport::Channel>,
    tonic::transport::Error,
> {
    let channel = connect_channel(addr);
    Ok(inferadb_ledger_proto::proto::user_service_client::UserServiceClient::new(channel))
}

/// Helper to create an invitation service client for a node.
#[allow(dead_code)]
pub async fn create_invitation_client(
    addr: &str,
) -> Result<
    inferadb_ledger_proto::proto::invitation_service_client::InvitationServiceClient<
        tonic::transport::Channel,
    >,
    tonic::transport::Error,
> {
    let channel = connect_channel(addr);
    Ok(inferadb_ledger_proto::proto::invitation_service_client::InvitationServiceClient::new(
        channel,
    ))
}

// ============================================================================
// Shared Organization & Vault Helpers (saga-aware)
// ============================================================================

/// Creates an organization and waits for the saga to reach Active status.
///
/// Returns the organization's external slug. Retries if the saga orchestrator
/// isn't ready yet and polls `GetOrganization` until the status is Active.
#[allow(dead_code)]
pub async fn create_test_organization(
    addr: &str,
    name: &str,
    node: &TestNode,
) -> Result<(inferadb_ledger_types::OrganizationSlug, u64), Box<dyn std::error::Error>> {
    let start = tokio::time::Instant::now();
    let timeout_dur = Duration::from_secs(30);

    // Create an admin user for the organization (direct Raft write, bypasses saga)
    let admin_email = format!("admin-{}@test.example.com", name.to_lowercase().replace(' ', "-"));
    let admin_slug = setup_user(addr, "Admin", &admin_email, node).await;

    // Slug generated once — retries reuse it so CreateOrganization
    // idempotency-by-slug returns the same OrganizationId on network
    // retry, matching production SDK behaviour.
    let org_slug = inferadb_ledger_types::snowflake::generate_organization_slug()?;

    // Create org with admin (retry if saga orchestrator not ready)
    let slug = loop {
        let mut client = create_organization_client(addr).await?;
        let result = client
            .create_organization(inferadb_ledger_proto::proto::CreateOrganizationRequest {
                name: name.to_string(),
                region: "us-east-va".to_string(),
                tier: None,
                caller: Some(inferadb_ledger_proto::proto::UserSlug { slug: admin_slug }),
                slug: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                    slug: org_slug.value(),
                }),
            })
            .await;

        match result {
            Ok(resp) => {
                break resp
                    .into_inner()
                    .slug
                    .map(|n| inferadb_ledger_types::OrganizationSlug::new(n.slug))
                    .ok_or("No organization slug in response")?;
            },
            Err(status) if status.code() == tonic::Code::Unavailable => {
                if start.elapsed() > timeout_dur {
                    return Err(format!("org creation not ready: {}", status.message()).into());
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            },
            Err(status) => return Err(format!("create org failed: {status}").into()),
        }
    };

    // Poll until Active (using the admin as caller for GetOrganization auth)
    loop {
        let mut client = create_organization_client(addr).await?;
        let result = client
            .get_organization(inferadb_ledger_proto::proto::GetOrganizationRequest {
                slug: Some(inferadb_ledger_proto::proto::OrganizationSlug { slug: slug.value() }),
                caller: Some(inferadb_ledger_proto::proto::UserSlug { slug: admin_slug }),
            })
            .await;

        if let Ok(resp) = result {
            // OrganizationStatus::Active = 1
            if resp.into_inner().status == 1 {
                return Ok((slug, admin_slug));
            }
        }
        if start.elapsed() > timeout_dur {
            return Err(format!(
                "org {} did not become Active within {timeout_dur:?}",
                slug.value()
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Creates a vault in an organization and returns its slug.
///
/// Retries if the organization is not yet ready (saga still provisioning).
#[allow(dead_code)]
pub async fn create_test_vault(
    addr: &str,
    organization: inferadb_ledger_types::OrganizationSlug,
) -> Result<inferadb_ledger_types::VaultSlug, Box<dyn std::error::Error>> {
    let start = tokio::time::Instant::now();
    let timeout_dur = Duration::from_secs(15);

    // Slug generated once — retries reuse it so per-org CreateVault
    // idempotency returns the same VaultId on network retry, matching
    // production SDK behaviour.
    let vault_slug = inferadb_ledger_types::snowflake::generate_vault_slug()?;

    loop {
        let mut client = create_vault_client(addr).await?;
        let result = client
            .create_vault(inferadb_ledger_proto::proto::CreateVaultRequest {
                organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                    slug: organization.value(),
                }),
                replication_factor: 0,
                initial_nodes: vec![],
                retention_policy: None,
                caller: None,
                slug: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault_slug.value() }),
            })
            .await;

        match result {
            Ok(resp) => {
                return resp
                    .into_inner()
                    .vault
                    .map(|v| inferadb_ledger_types::VaultSlug::new(v.slug))
                    .ok_or_else(|| "No vault slug in response".into());
            },
            Err(status)
                if status.code() == tonic::Code::NotFound
                    || status.code() == tonic::Code::FailedPrecondition
                    || (status.code() == tonic::Code::Unavailable
                        && status.message().contains("Not the leader")) =>
            {
                // Org not yet ready — retry. The `Unavailable` + "Not the leader"
                // arm matches production SDK behavior: the per-org Raft group's
                // delegated-leader adoption watcher is `tokio::spawn`'d and may
                // not have observed the parent region group's leader yet at the
                // moment the saga finishes. Production clients retry through the
                // SDK's `with_retry_cancellable`; tests must do the same.
                if start.elapsed() > timeout_dur {
                    return Err(
                        format!("vault creation failed after retry: {}", status.message()).into()
                    );
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            },
            Err(status) => return Err(format!("create vault failed: {status}").into()),
        }
    }
}

// ============================================================================
// High-Level Test Helpers
// ============================================================================

/// The test blinding key used by `TestCluster` (32 bytes, hex-encoded).
const TEST_BLINDING_KEY_HEX: &str =
    "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

/// Parses the test blinding key into an `EmailBlindingKey`.
fn test_blinding_key() -> inferadb_ledger_types::EmailBlindingKey {
    TEST_BLINDING_KEY_HEX.parse().expect("valid test blinding key")
}

/// Creates a user by directly proposing through the GLOBAL Raft group.
///
/// Uses `SystemRequest::CreateUser` (the same request the saga uses in Step 1)
/// followed by `SystemRequest::RegisterEmailHash` (Step 0). This bypasses the
/// saga orchestrator, which is appropriate for test setup — tests need users
/// to exist immediately, not after async saga completion.
///
/// Returns the user's external slug.
#[allow(dead_code)]
pub async fn setup_user(_addr: &str, _name: &str, email: &str, node: &TestNode) -> u64 {
    use inferadb_ledger_raft::types::{RaftPayload, SystemRequest};

    let blinding_key = test_blinding_key();
    let email_hmac = inferadb_ledger_types::compute_email_hmac(&blinding_key, email);
    let user_slug =
        inferadb_ledger_types::snowflake::generate_user_slug().expect("generate user slug");

    // Step 0: Register email HMAC (reserves uniqueness in GLOBAL)
    let register_req = SystemRequest::RegisterEmailHash {
        hmac_hex: email_hmac,
        user_id: inferadb_ledger_types::UserId::new(0), /* placeholder — CreateUser allocates
                                                         * real ID */
    };
    let _ = node
        .handle
        .propose_and_wait(RaftPayload::system(register_req), Duration::from_secs(5))
        .await;

    // Step 1: Create user directory entry (allocates UserId, registers slug)
    let create_req = SystemRequest::CreateUser {
        user: inferadb_ledger_types::UserId::new(0), // 0 = auto-allocate from sequence
        admin: false,
        slug: user_slug,
        region: inferadb_ledger_types::Region::US_EAST_VA,
    };
    let result =
        node.handle.propose_and_wait(RaftPayload::system(create_req), Duration::from_secs(5)).await;

    match result {
        Ok(resp) => match resp {
            inferadb_ledger_raft::types::LedgerResponse::UserCreated { user_id, slug } => {
                // Step 2: Activate the user directory entry
                let activate_req = SystemRequest::UpdateUserDirectoryStatus {
                    user_id,
                    status: inferadb_ledger_state::system::UserDirectoryStatus::Active,
                    region: Some(inferadb_ledger_types::Region::US_EAST_VA),
                };
                let _ = node
                    .handle
                    .propose_and_wait(RaftPayload::system(activate_req), Duration::from_secs(5))
                    .await;

                // Step 3: Write slug index to entity store (so get_user_id_by_slug works).
                // The CreateUser SystemRequest only writes to the in-memory index;
                // the entity store entry is normally written by the REGIONAL saga step.
                let slug_key = inferadb_ledger_state::system::SystemKeys::user_slug_index_key(slug);
                let slug_op = inferadb_ledger_types::Operation::SetEntity {
                    key: slug_key,
                    value: user_id.value().to_string().into_bytes(),
                    condition: None,
                    expires_at: None,
                };
                let slug_txn = inferadb_ledger_types::Transaction {
                    id: *uuid::Uuid::new_v4().as_bytes(),
                    client_id: inferadb_ledger_types::ClientId::new("test:setup"),
                    sequence: 0,
                    operations: vec![slug_op],
                    timestamp: std::time::SystemTime::now().into(),
                };
                let slug_write = SystemRequest::Write {
                    vault: inferadb_ledger_types::VaultId::new(0),
                    transactions: vec![slug_txn],
                    idempotency_key: [0; 16],
                    request_hash: 0,
                };
                let _ = node
                    .handle
                    .propose_and_wait(RaftPayload::system(slug_write), Duration::from_secs(5))
                    .await;

                // Step 4: Write User entity to GLOBAL state layer.
                // Token services (CreateUserSession) read the User entity from
                // GLOBAL state. In production, this is written to REGIONAL by the
                // user creation saga, but tests need it in GLOBAL for service access.
                let user_entity = inferadb_ledger_state::system::User {
                    id: user_id,
                    slug,
                    region: inferadb_ledger_types::Region::US_EAST_VA,
                    name: String::new(),
                    email: inferadb_ledger_types::UserEmailId::new(0),
                    status: inferadb_ledger_state::system::UserStatus::Active,
                    role: inferadb_ledger_types::UserRole::User,
                    created_at: std::time::SystemTime::now().into(),
                    updated_at: std::time::SystemTime::now().into(),
                    deleted_at: None,
                    version: inferadb_ledger_types::TokenVersion::new(1),
                };
                let user_key = inferadb_ledger_state::system::SystemKeys::user_key(user_id);
                let user_value =
                    inferadb_ledger_types::encode(&user_entity).expect("encode user entity");
                let user_op = inferadb_ledger_types::Operation::SetEntity {
                    key: user_key,
                    value: user_value,
                    condition: None,
                    expires_at: None,
                };
                let user_txn = inferadb_ledger_types::Transaction {
                    id: *uuid::Uuid::new_v4().as_bytes(),
                    client_id: inferadb_ledger_types::ClientId::new("test:setup"),
                    sequence: 0,
                    operations: vec![user_op],
                    timestamp: std::time::SystemTime::now().into(),
                };
                let user_write = SystemRequest::Write {
                    vault: inferadb_ledger_types::VaultId::new(0),
                    transactions: vec![user_txn],
                    idempotency_key: [0; 16],
                    request_hash: 0,
                };
                let _ = node
                    .handle
                    .propose_and_wait(RaftPayload::system(user_write), Duration::from_secs(5))
                    .await;

                slug.value()
            },
            other => panic!("setup_user: expected UserCreated, got {other}"),
        },
        Err(e) => panic!("setup_user: consensus write failed: {e}"),
    }
}

/// Creates an organization with a real admin user and waits for it to become Active.
///
/// Returns `(org_slug, admin_user_slug)`. The admin user is created via
/// `setup_user` first, then the org is created with that user as admin.
#[allow(dead_code)]
pub async fn setup_org_with_admin(
    addr: &str,
    org_name: &str,
    admin_email: &str,
    node: &TestNode,
) -> (u64, u64) {
    // Create a user to serve as org admin (direct Raft write, bypasses saga)
    let admin_slug = setup_user(addr, "Admin", admin_email, node).await;

    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(30);

    // Slug generated once — retries reuse it so CreateOrganization
    // idempotency-by-slug returns the same OrganizationId on network
    // retry, matching production SDK behaviour.
    let requested_slug = inferadb_ledger_types::snowflake::generate_organization_slug()
        .expect("generate organization slug");

    // Create org with the admin user
    let org_slug = loop {
        let mut client = create_organization_client(addr).await.expect("connect org");
        let result = client
            .create_organization(inferadb_ledger_proto::proto::CreateOrganizationRequest {
                name: org_name.to_string(),
                region: "us-east-va".to_string(),
                tier: None,
                caller: Some(inferadb_ledger_proto::proto::UserSlug { slug: admin_slug }),
                slug: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                    slug: requested_slug.value(),
                }),
            })
            .await;

        match result {
            Ok(resp) => break resp.into_inner().slug.expect("org slug").slug,
            Err(status) if status.code() == tonic::Code::Unavailable => {
                if start.elapsed() > timeout {
                    panic!("org creation not ready after {timeout:?}: {}", status.message());
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            },
            Err(status) => panic!("create org '{org_name}' failed: {status}"),
        }
    };

    // Poll until org is Active
    loop {
        let mut client = create_organization_client(addr).await.expect("connect org");
        let result = client
            .get_organization(inferadb_ledger_proto::proto::GetOrganizationRequest {
                slug: Some(inferadb_ledger_proto::proto::OrganizationSlug { slug: org_slug }),
                caller: Some(inferadb_ledger_proto::proto::UserSlug { slug: admin_slug }),
            })
            .await;

        if let Ok(resp) = result {
            // OrganizationStatus::Active = 1
            if resp.into_inner().status == 1 {
                return (org_slug, admin_slug);
            }
        }

        if start.elapsed() > timeout {
            panic!("org {org_slug} did not become Active within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Creates a vault with retry — the organization may still be provisioning.
///
/// Retries on "not found", "provisioned", "provisioning", and "Not the
/// leader" errors. The pinned-client variant is sufficient when the
/// caller already knows it has the regional leader; tests that don't
/// should prefer [`create_vault_with_retry_endpoints`] which iterates
/// every cluster endpoint per attempt to absorb GLOBAL-vs-region leader
/// splits under multi-tier consensus.
pub async fn create_vault_with_retry(
    vault_client: &mut inferadb_ledger_proto::proto::vault_service_client::VaultServiceClient<
        tonic::transport::Channel,
    >,
    organization: inferadb_ledger_types::OrganizationSlug,
    caller_slug: u64,
) -> inferadb_ledger_types::VaultSlug {
    // Single client-generated slug reused across retries (matches SDK).
    let vault_slug =
        inferadb_ledger_types::snowflake::generate_vault_slug().expect("generate vault slug");
    for attempt in 0..60 {
        let request = inferadb_ledger_proto::proto::CreateVaultRequest {
            organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                slug: organization.value(),
            }),
            replication_factor: 0,
            initial_nodes: vec![],
            retention_policy: None,
            caller: Some(inferadb_ledger_proto::proto::UserSlug { slug: caller_slug }),
            slug: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault_slug.value() }),
        };
        match vault_client.create_vault(request).await {
            Ok(response) => {
                return response
                    .into_inner()
                    .vault
                    .map(|v| inferadb_ledger_types::VaultSlug::new(v.slug))
                    .expect("vault slug in response");
            },
            Err(e)
                if (e.message().contains("not found")
                    || e.message().contains("provisioned")
                    || e.message().contains("provisioning")
                    || e.message().contains("Not the leader"))
                    && attempt < 59 =>
            {
                tokio::time::sleep(Duration::from_millis(500)).await;
            },
            Err(e) => panic!("create vault: {e}"),
        }
    }
    panic!("create vault timed out after 60 attempts");
}

/// Multi-endpoint variant of [`create_vault_with_retry`]. Iterates every
/// endpoint in `endpoints` per attempt, so a GLOBAL-vs-regional leader
/// split (the common pattern after `setup_user_and_org` lands the
/// initial flow on the GLOBAL leader while the data-region leader is on
/// a different node) absorbs cleanly without panicking. Use this from
/// `external` integration tests where the caller has the cluster's
/// endpoint list available.
pub async fn create_vault_with_retry_endpoints(
    endpoints: &[String],
    organization: inferadb_ledger_types::OrganizationSlug,
    caller_slug: u64,
) -> inferadb_ledger_types::VaultSlug {
    let vault_slug =
        inferadb_ledger_types::snowflake::generate_vault_slug().expect("generate vault slug");
    for _attempt in 0..60 {
        for ep in endpoints {
            let mut vault_client = match inferadb_ledger_proto::proto::vault_service_client::VaultServiceClient::connect(ep.to_string()).await {
                Ok(c) => c,
                Err(_) => continue,
            };
            let request = inferadb_ledger_proto::proto::CreateVaultRequest {
                organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                    slug: organization.value(),
                }),
                replication_factor: 0,
                initial_nodes: vec![],
                retention_policy: None,
                caller: Some(inferadb_ledger_proto::proto::UserSlug { slug: caller_slug }),
                slug: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault_slug.value() }),
            };
            match vault_client.create_vault(request).await {
                Ok(response) => {
                    return response
                        .into_inner()
                        .vault
                        .map(|v| inferadb_ledger_types::VaultSlug::new(v.slug))
                        .expect("vault slug in response");
                },
                Err(e)
                    if e.message().contains("not found")
                        || e.message().contains("provisioned")
                        || e.message().contains("provisioning")
                        || e.message().contains("Not the leader")
                        || e.code() == tonic::Code::Unavailable =>
                {
                    continue;
                },
                Err(e) => panic!("create vault: {e}"),
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("create vault timed out after 60 attempts (all endpoints)");
}

// ============================================================================
// Vault group convergence helpers
// ============================================================================
//
// `VaultService::create_vault` waits for the local watcher to call
// `start_vault_group` before returning, so a write issued immediately
// after `create_test_vault` against the same leader observes the vault
// group registered. Tests that need to read or write through followers
// (or otherwise depend on the per-vault group being live on every voter)
// still need a cross-voter convergence wait — followers run their own
// watcher tasks after applying the entry, and the producer RPC only
// awaits the local node's registration.
//
// `wait_for_vault_group_live_on_all_voters` is the cross-voter wait;
// `wait_for_vault_group_removed_on_all_voters` is the symmetric teardown
// helper for delete tests, which still need to observe the
// `VaultDeletionRequest` → watcher → `stop_vault_group` chain on every
// voter.

/// Polls every node until the cluster agrees on a single registered
/// `(region, org_id, *)` triple, or `timeout` elapses. Returns the
/// `VaultId` every node observes.
///
/// This intentionally does NOT depend on the GLOBAL vault-slug index —
/// the index write (`SystemRequest::RegisterVaultDirectoryEntry`) is a
/// separate propose from the per-org `CreateVault` and their apply
/// order on followers is not coupled to the vault group start. The
/// "vault group is live on every voter" contract is observable directly
/// on `RaftManager::list_vault_groups`.
pub async fn wait_for_vault_group_live_on_all_voters(
    cluster: &TestCluster,
    region: inferadb_ledger_types::Region,
    org_id: inferadb_ledger_types::OrganizationId,
    timeout: Duration,
) -> inferadb_ledger_types::VaultId {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let per_node: Vec<Vec<inferadb_ledger_types::VaultId>> = cluster
            .nodes()
            .iter()
            .map(|n| {
                n.manager
                    .list_vault_groups()
                    .into_iter()
                    .filter(|(r, o, _)| *r == region && *o == org_id)
                    .map(|(_, _, v)| v)
                    .collect()
            })
            .collect();

        if let Some(first) = per_node.first()
            && first.len() == 1
            && per_node.iter().all(|ids| ids == first)
        {
            return first[0];
        }

        if tokio::time::Instant::now() >= deadline {
            let rendered: Vec<String> = cluster
                .nodes()
                .iter()
                .zip(per_node.iter())
                .map(|(n, ids)| format!("node {}: {:?}", n.id, ids))
                .collect();
            panic!(
                "vault group for (region={region:?}, org_id={org_id:?}) did not converge across \
                 voters within {timeout:?}. per-node state: [{}]",
                rendered.join(" | "),
            );
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Polls every node until NO voter reports `(region, org_id, vault_id)`
/// in its `list_vault_groups()`. Used by delete tests to assert that the
/// `VaultDeletionRequest` → watcher → `stop_vault_group` chain has fired
/// and torn down the vault group on every voter.
pub async fn wait_for_vault_group_removed_on_all_voters(
    cluster: &TestCluster,
    region: inferadb_ledger_types::Region,
    org_id: inferadb_ledger_types::OrganizationId,
    vault_id: inferadb_ledger_types::VaultId,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let still_present: Vec<u64> = cluster
            .nodes()
            .iter()
            .filter(|n| n.manager.has_vault_group(region, org_id, vault_id))
            .map(|n| n.id)
            .collect();

        if still_present.is_empty() {
            return;
        }

        if tokio::time::Instant::now() >= deadline {
            panic!(
                "vault group (region={region:?}, org_id={org_id:?}, vault_id={vault_id:?}) was \
                 not torn down on all voters within {timeout:?}. still present on: \
                 {still_present:?}",
            );
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Resolves an external `OrganizationSlug` to its internal
/// `OrganizationId` via the GLOBAL applied-state accessor. Panics if the
/// slug is not yet indexed — callers must have awaited organization
/// activation (e.g. via `create_test_organization` which polls until
/// status == Active).
pub fn resolve_org_id(
    node: &TestNode,
    slug: inferadb_ledger_types::OrganizationSlug,
) -> inferadb_ledger_types::OrganizationId {
    node.manager
        .system_region()
        .expect("system region running")
        .applied_state()
        .resolve_slug_to_id(slug)
        .expect("organization slug resolves after CreateOrganization commits")
}

/// Polls until every voter has at least one vault group registered for
/// `org_id`, in the same `(region, org_id, *)` triple, and every voter
/// agrees on the set of triples.
///
/// The region the organization landed in is auto-discovered from the
/// reference node's `RaftManager` via `get_organization_region` — this
/// avoids forcing every test to know up front which region a multi-region
/// `CreateOrganization` will place the org into. Returns the discovered
/// region alongside the converged vault id (when the test created exactly
/// one vault for `org_id`).
///
/// `VaultService::create_vault` already waits for the local node's
/// `start_vault_group` to land before returning, so this helper is
/// redundant for tests that operate against the same leader the
/// `create_test_vault` call landed on. Use it for tests that issue
/// reads or writes through a follower after creating a vault — the
/// follower's watcher fires asynchronously after its apply, and the
/// producer RPC does not await follower convergence.
pub async fn wait_for_org_vault_group_live(
    cluster: &TestCluster,
    org_slug: inferadb_ledger_types::OrganizationSlug,
    timeout: Duration,
) -> (inferadb_ledger_types::Region, inferadb_ledger_types::VaultId) {
    let reference = &cluster.nodes()[0];
    let org_id = resolve_org_id(reference, org_slug);
    let region = reference
        .manager
        .get_organization_region(org_id)
        .expect("organization is registered on at least one node after activation");
    let vault_id = wait_for_vault_group_live_on_all_voters(cluster, region, org_id, timeout).await;
    (region, vault_id)
}

/// Polls until every voter reports exactly `expected_count` vault groups
/// for the organization identified by `org_slug`, with the same set of
/// `VaultId`s on every node. Returns the converged set sorted ascending,
/// alongside the discovered region.
///
/// Use when a test creates multiple vaults under the same organization
/// and needs all of them live on every voter — for example, when reads
/// or writes need to land on followers. Tests that only use the leader
/// the `create_test_vault` call landed on do not need this wait:
/// `VaultService::create_vault` already awaits each vault group's local
/// registration before returning.
pub async fn wait_for_org_vault_groups_live(
    cluster: &TestCluster,
    org_slug: inferadb_ledger_types::OrganizationSlug,
    expected_count: usize,
    timeout: Duration,
) -> (inferadb_ledger_types::Region, Vec<inferadb_ledger_types::VaultId>) {
    let reference = &cluster.nodes()[0];
    let org_id = resolve_org_id(reference, org_slug);
    let region = reference
        .manager
        .get_organization_region(org_id)
        .expect("organization is registered on at least one node after activation");

    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let per_node: Vec<Vec<inferadb_ledger_types::VaultId>> = cluster
            .nodes()
            .iter()
            .map(|n| {
                let mut ids: Vec<inferadb_ledger_types::VaultId> = n
                    .manager
                    .list_vault_groups()
                    .into_iter()
                    .filter(|(r, o, _)| *r == region && *o == org_id)
                    .map(|(_, _, v)| v)
                    .collect();
                ids.sort();
                ids
            })
            .collect();

        if let Some(first) = per_node.first()
            && first.len() == expected_count
            && per_node.iter().all(|ids| ids == first)
        {
            return (region, first.clone());
        }

        if tokio::time::Instant::now() >= deadline {
            let rendered: Vec<String> = cluster
                .nodes()
                .iter()
                .zip(per_node.iter())
                .map(|(n, ids)| format!("node {}: {:?}", n.id, ids))
                .collect();
            panic!(
                "vault set for (region={region:?}, org_id={org_id:?}) did not converge to \
                 {expected_count} entries within {timeout:?}. per-node state: [{}]",
                rendered.join(" | "),
            );
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
