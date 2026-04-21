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
use inferadb_ledger_raft::{ConsensusHandle, RaftManager, RegionConfig, OrganizationGroup};
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
    _temp_dir: TestDir,
    /// Server task handle for cleanup.
    _server_handle: tokio::task::JoinHandle<()>,
    /// Shutdown sender — kept alive so the server doesn't immediately exit.
    /// When dropped, the watch receiver in `serve_with_shutdown` resolves,
    /// triggering graceful shutdown.
    _shutdown_tx: tokio::sync::watch::Sender<bool>,
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
    pub fn system_region(&self) -> Arc<OrganizationGroup> {
        self.manager.system_region().expect("system region exists")
    }

    /// Returns a region group by region.
    pub fn region_group(&self, region: inferadb_ledger_types::Region) -> Option<Arc<OrganizationGroup>> {
        self.manager.get_region_group(region).ok()
    }

    /// Returns every shard group registered for `region` on this node.
    ///
    /// Multi-shard regions run N independent Raft groups; convergence
    /// helpers (`data_regions_synced`) iterate all shards rather than
    /// just shard 0 because writes route across shards via `ShardRouter`,
    /// and waiting for shard 0 alone leaves later shards racing the
    /// assertion.
    pub fn shard_groups(
        &self,
        region: inferadb_ledger_types::Region,
    ) -> Vec<Arc<OrganizationGroup>> {
        self.manager
            .list_shards()
            .into_iter()
            .filter(|(r, _)| *r == region)
            .filter_map(|(r, s)| self.manager.get_shard_group(r, s).ok())
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
}

/// Builds a Raft config with aggressive timeouts for fast test execution.
///
/// Production defaults (100ms heartbeat, 300-500ms election) are tuned for
/// real networks. Tests run on localhost but under heavy parallelism (8 threads,
/// each spawning 1-3 tokio runtimes), so we use 50ms heartbeat and 150-300ms
/// election timeouts — 2x faster than production while remaining stable under
/// CPU contention from parallel test execution.
fn test_rate_limit_config() -> inferadb_ledger_types::config::RateLimitConfig {
    inferadb_ledger_types::config::RateLimitConfig::builder()
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
}

impl TestCluster {
    /// Creates a new test cluster with the given number of nodes and 1 data region.
    ///
    /// The first node bootstraps the cluster, and other nodes join via
    /// the AdminService's join_cluster RPC.
    /// All nodes use ephemeral ports on localhost.
    pub async fn new(size: usize) -> Self {
        Self::build(size, 1, true).await
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
        assert!(num_data_regions >= 1, "cluster must have at least 1 data region");

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

        let config = inferadb_ledger_server::config::Config {
            listen,
            socket: socket_path.clone(),
            metrics_addr: None,
            data_dir: Some(data_dir.clone()),
            backup: Some(backup_config),
            raft: Some(test_raft_config()),
            saga: inferadb_ledger_types::config::SagaConfig { poll_interval_secs: 2 },
            token_maintenance_interval_secs: 3,
            rate_limit: Some(rate_limit_override.clone().unwrap_or_else(test_rate_limit_config)),
            email_blinding_key: if include_blinding_key {
                Some("deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string())
            } else {
                None
            },
            // Phase A multi-shard: production default is 16. Existing
            // tests assume single-shard semantics in their cluster-join
            // and convergence helpers. A dedicated `tests/multi_shard.rs`
            // (Task 7) validates the >1 shard routing path against an
            // explicit `shards_per_region: N` cluster. Leaving the
            // generic `TestCluster` at 1 keeps those legacy tests stable
            // while the multi-shard infrastructure (route_request,
            // ShardRouter, per-shard storage) is exercised end-to-end by
            // the dedicated test.
            shards_per_region: 1,
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
            _temp_dir: temp_dir,
            _server_handle: server_handle,
            _shutdown_tx: shutdown_tx,
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
                let shards = manager_clone
                    .list_shards()
                    .into_iter()
                    .filter(|(r, _)| *r == dr);
                let mut any_shard = false;
                for (r, shard) in shards {
                    any_shard = true;
                    match manager_clone.get_shard_group(r, shard) {
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
                socket: join_socket,
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
                // Match the bootstrap node's shards_per_region: every node
                // MUST agree on the shard count or ShardRouter produces
                // divergent routing decisions. See bootstrap-node config
                // for the rationale on the test default.
                shards_per_region: 1,
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
                _temp_dir: temp_dir,
                _server_handle: server_handle,
                _shutdown_tx: shutdown_tx,
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
                let shards: Vec<inferadb_ledger_types::OrganizationId> =
                    bootstrap_manager
                        .list_shards()
                        .into_iter()
                        .filter(|(r, _)| *r == data_region)
                        .map(|(_, s)| s)
                        .collect();

                // Register transport once per node pair — channels are
                // multiplexed across shards by the consensus engine.
                let joining_addr_str = join_addr.clone();
                let bootstrap_addr_str = nodes[0].addr.clone();
                if let Ok(bootstrap_rg_zero) =
                    bootstrap_manager.get_shard_group(data_region, shards[0])
                    && let Some(bt) = bootstrap_rg_zero.consensus_transport()
                {
                    let _ = bt.set_peer_via_registry(joining_node_id, &joining_addr_str).await;
                }
                if let Ok(joining_rg_zero) =
                    joining_manager.get_shard_group(data_region, shards[0])
                    && let Some(jt) = joining_rg_zero.consensus_transport()
                {
                    let _ = jt.set_peer_via_registry(nodes[0].id, &bootstrap_addr_str).await;
                }

                for shard in shards {
                    if let Ok(bootstrap_rg) =
                        bootstrap_manager.get_shard_group(data_region, shard)
                    {
                        let bootstrap_handle = bootstrap_rg.handle();

                        // Per-shard transport registration — each shard's
                        // consensus engine owns its own peer routing table.
                        if let Some(bt) = bootstrap_rg.consensus_transport() {
                            let _ = bt
                                .set_peer_via_registry(joining_node_id, &joining_addr_str)
                                .await;
                        }
                        if let Ok(joining_rg) =
                            joining_manager.get_shard_group(data_region, shard)
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

        Self { nodes, num_data_regions, _socket_dir: socket_dir }
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
    fn data_regions_synced(&self) -> bool {
        let data_regions = &inferadb_ledger_types::ALL_REGIONS[1..];
        for &dr in data_regions.iter().take(self.num_data_regions) {
            // Use the leader node as the reference for shard membership;
            // every node should have the same set of `(region, shard)`
            // groups registered.
            let reference_node = &self.nodes[0];
            for shard_group in reference_node.shard_groups(dr) {
                let organization_id = shard_group.organization_id();
                let mut indices = Vec::new();
                for node in &self.nodes {
                    if let Some(rg) = node
                        .manager
                        .get_shard_group(dr, organization_id)
                        .ok()
                    {
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

    // Create org with admin (retry if saga orchestrator not ready)
    let slug = loop {
        let mut client = create_organization_client(addr).await?;
        let result = client
            .create_organization(inferadb_ledger_proto::proto::CreateOrganizationRequest {
                name: name.to_string(),
                region: 10, // US_EAST_VA
                tier: None,
                caller: Some(inferadb_ledger_proto::proto::UserSlug { slug: admin_slug }),
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
                    || status.code() == tonic::Code::FailedPrecondition =>
            {
                // Org not yet ready — retry
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
    use inferadb_ledger_raft::types::{RaftPayload, LedgerRequest, OrganizationRequest, RegionRequest, SystemRequest};

    let blinding_key = test_blinding_key();
    let email_hmac = inferadb_ledger_types::compute_email_hmac(&blinding_key, email);
    let user_slug =
        inferadb_ledger_types::snowflake::generate_user_slug().expect("generate user slug");

    // Step 0: Register email HMAC (reserves uniqueness in GLOBAL)
    let register_req = LedgerRequest::System(SystemRequest::RegisterEmailHash {
        hmac_hex: email_hmac,
        user_id: inferadb_ledger_types::UserId::new(0), /* placeholder — CreateUser allocates
                                                         * real ID */
    });
    let _ = node
        .handle
        .propose_and_wait(RaftPayload::system(register_req), Duration::from_secs(5))
        .await;

    // Step 1: Create user directory entry (allocates UserId, registers slug)
    let create_req = LedgerRequest::System(SystemRequest::CreateUser {
        user: inferadb_ledger_types::UserId::new(0), // 0 = auto-allocate from sequence
        admin: false,
        slug: user_slug,
        region: inferadb_ledger_types::Region::US_EAST_VA,
    });
    let result =
        node.handle.propose_and_wait(RaftPayload::system(create_req), Duration::from_secs(5)).await;

    match result {
        Ok(resp) => match resp {
            inferadb_ledger_raft::types::LedgerResponse::UserCreated { user_id, slug } => {
                // Step 2: Activate the user directory entry
                let activate_req =
                    LedgerRequest::System(SystemRequest::UpdateUserDirectoryStatus {
                        user_id,
                        status: inferadb_ledger_state::system::UserDirectoryStatus::Active,
                        region: Some(inferadb_ledger_types::Region::US_EAST_VA),
                    });
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
                let slug_write = LedgerRequest::Organization(OrganizationRequest::Write {
                    organization: inferadb_ledger_types::OrganizationId::new(0),
                    vault: inferadb_ledger_types::VaultId::new(0),
                    transactions: vec![slug_txn],
                    idempotency_key: [0; 16],
                    request_hash: 0,
                });
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
                let user_write = LedgerRequest::Organization(OrganizationRequest::Write {
                    organization: inferadb_ledger_types::OrganizationId::new(0),
                    vault: inferadb_ledger_types::VaultId::new(0),
                    transactions: vec![user_txn],
                    idempotency_key: [0; 16],
                    request_hash: 0,
                });
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

    // Create org with the admin user
    let org_slug = loop {
        let mut client = create_organization_client(addr).await.expect("connect org");
        let result = client
            .create_organization(inferadb_ledger_proto::proto::CreateOrganizationRequest {
                name: org_name.to_string(),
                region: 10, // US_EAST_VA
                tier: None,
                caller: Some(inferadb_ledger_proto::proto::UserSlug { slug: admin_slug }),
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
/// Retries on "not found" and "provisioned" errors for up to 30 seconds.
pub async fn create_vault_with_retry(
    vault_client: &mut inferadb_ledger_proto::proto::vault_service_client::VaultServiceClient<
        tonic::transport::Channel,
    >,
    organization: inferadb_ledger_types::OrganizationSlug,
    caller_slug: u64,
) -> inferadb_ledger_types::VaultSlug {
    for attempt in 0..60 {
        let request = inferadb_ledger_proto::proto::CreateVaultRequest {
            organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                slug: organization.value(),
            }),
            replication_factor: 0,
            initial_nodes: vec![],
            retention_policy: None,
            caller: Some(inferadb_ledger_proto::proto::UserSlug { slug: caller_slug }),
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
                    || e.message().contains("provisioning"))
                    && attempt < 59 =>
            {
                tokio::time::sleep(Duration::from_millis(500)).await;
            },
            Err(e) => panic!("create vault: {e}"),
        }
    }
    panic!("create vault timed out after 60 attempts");
}
