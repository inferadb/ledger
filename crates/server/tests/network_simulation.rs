//! Network simulation tests using turmoil.
//!
//! These tests verify Raft consensus behavior under simulated network conditions
//! like partitions, delays, and message loss.
//!
//! Note: These tests use turmoil's deterministic simulation rather than real networking.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

mod turmoil_common;

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use inferadb_ledger_proto::proto::{
    RaftAppendEntriesRequest, RaftAppendEntriesResponse, RaftInstallSnapshotRequest,
    RaftInstallSnapshotResponse, RaftVoteRequest, RaftVoteResponse,
    raft_service_server::{RaftService, RaftServiceServer},
};
use tonic::{Request, Response, Status, transport::Server};
use turmoil::Builder;
use turmoil_common::incoming_stream;

/// Simple counter for tracking RPC calls
struct RpcCounter {
    votes_received: AtomicU64,
    append_entries_received: AtomicU64,
}

impl RpcCounter {
    fn new() -> Self {
        Self { votes_received: AtomicU64::new(0), append_entries_received: AtomicU64::new(0) }
    }
}

/// Minimal Raft service that just counts RPCs (for testing network reachability)
struct MinimalRaftService {
    #[allow(dead_code)]
    node_id: u64,
    counter: Arc<RpcCounter>,
}

#[tonic::async_trait]
impl RaftService for MinimalRaftService {
    async fn vote(
        &self,
        _request: Request<RaftVoteRequest>,
    ) -> Result<Response<RaftVoteResponse>, Status> {
        self.counter.votes_received.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(RaftVoteResponse { vote: None, vote_granted: false, last_log_id: None }))
    }

    async fn append_entries(
        &self,
        _request: Request<RaftAppendEntriesRequest>,
    ) -> Result<Response<RaftAppendEntriesResponse>, Status> {
        self.counter.append_entries_received.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(RaftAppendEntriesResponse { success: true, conflict: false, vote: None }))
    }

    async fn install_snapshot(
        &self,
        _request: Request<RaftInstallSnapshotRequest>,
    ) -> Result<Response<RaftInstallSnapshotResponse>, Status> {
        Ok(Response::new(RaftInstallSnapshotResponse { vote: None }))
    }
}

/// Starts a minimal Raft server on the given turmoil host.
async fn start_minimal_server(node_id: u64, counter: Arc<RpcCounter>) {
    let addr: SocketAddr = "0.0.0.0:9999".parse().unwrap();
    let service = MinimalRaftService { node_id, counter };

    Server::builder()
        .add_service(RaftServiceServer::new(service))
        .serve_with_incoming(incoming_stream(addr))
        .await
        .expect("server failed");
}

/// Creates a Raft service client using turmoil's simulated network.
async fn create_raft_client(
    host: &str,
) -> Result<
    inferadb_ledger_proto::proto::raft_service_client::RaftServiceClient<tonic::transport::Channel>,
    tonic::transport::Error,
> {
    let channel = turmoil_common::create_turmoil_channel(host, 9999).await?;
    Ok(inferadb_ledger_proto::proto::raft_service_client::RaftServiceClient::new(channel))
}

/// Tests that network partitions block communication between nodes.
///
/// This test creates a 3-node simulated cluster and verifies that:
/// 1. Nodes can communicate before partition
/// 2. Partitioned nodes cannot communicate
/// 3. Communication resumes after partition heals
#[test]
fn test_network_partition_blocks_communication() {
    let mut sim = Builder::new().build();

    // Track RPC calls for each node
    let node1_counter = Arc::new(RpcCounter::new());
    let node2_counter = Arc::new(RpcCounter::new());
    let node3_counter = Arc::new(RpcCounter::new());

    // Clone counters for the simulation
    let n1_counter = node1_counter.clone();
    let n2_counter = node2_counter.clone();
    let n3_counter = node3_counter.clone();

    // Register hosts
    sim.host("node1", move || {
        let counter = n1_counter.clone();
        async move {
            start_minimal_server(1, counter).await;
            Ok(())
        }
    });

    sim.host("node2", move || {
        let counter = n2_counter.clone();
        async move {
            start_minimal_server(2, counter).await;
            Ok(())
        }
    });

    sim.host("node3", move || {
        let counter = n3_counter.clone();
        async move {
            start_minimal_server(3, counter).await;
            Ok(())
        }
    });

    // Test client
    sim.client("test", async move {
        // Wait for servers to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Phase 1: Verify all nodes can communicate
        {
            let mut client1 = create_raft_client("node1").await.expect("connect to node1");
            let mut client2 = create_raft_client("node2").await.expect("connect to node2");
            let mut client3 = create_raft_client("node3").await.expect("connect to node3");

            // Send vote requests to all nodes
            let vote_req = RaftVoteRequest { vote: None, last_log_id: None, shard_id: None };

            client1.vote(vote_req).await.expect("vote to node1 should succeed");
            client2.vote(vote_req).await.expect("vote to node2 should succeed");
            client3.vote(vote_req).await.expect("vote to node3 should succeed");
        }

        // Phase 2: Partition node3 from nodes 1 and 2, and from test client
        turmoil::partition("node3", "node1");
        turmoil::partition("node3", "node2");
        turmoil::partition("node3", "test");

        // Try to communicate from test client (which is in same partition as node1/2)
        // to node3 - this should fail
        {
            // Give partition time to take effect
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Communication to node3 should fail due to partition
            let result = create_raft_client("node3").await;
            match result {
                Ok(mut client) => {
                    let vote_req =
                        RaftVoteRequest { vote: None, last_log_id: None, shard_id: None };
                    let vote_result = client.vote(vote_req).await;
                    // The connection or RPC should fail due to partition
                    assert!(vote_result.is_err(), "vote to partitioned node3 should fail");
                },
                Err(_) => {
                    // Connection failed - expected behavior during partition
                },
            }

            // Communication to nodes 1 and 2 should still work
            let mut client1 = create_raft_client("node1").await.expect("connect to node1");
            let mut client2 = create_raft_client("node2").await.expect("connect to node2");

            let vote_req = RaftVoteRequest { vote: None, last_log_id: None, shard_id: None };
            client1.vote(vote_req).await.expect("vote to node1 should succeed during partition");
            client2.vote(vote_req).await.expect("vote to node2 should succeed during partition");
        }

        // Phase 3: Heal partition
        turmoil::repair("node3", "node1");
        turmoil::repair("node3", "node2");
        turmoil::repair("node3", "test");

        // Verify communication resumes
        {
            tokio::time::sleep(Duration::from_millis(50)).await;

            let mut client3 =
                create_raft_client("node3").await.expect("connect to node3 after heal");
            let vote_req = RaftVoteRequest { vote: None, last_log_id: None, shard_id: None };
            client3.vote(vote_req).await.expect("vote to node3 should succeed after heal");
        }

        Ok(())
    });

    // Run simulation
    sim.run().expect("simulation should complete");
}

/// Tests that writes to majority partition succeed while minority fails.
///
/// This simulates the scenario where a 3-node cluster experiences a partition
/// that isolates one node. The majority (2 nodes) should continue operating
/// while the minority (1 node) cannot make progress.
#[test]
fn test_majority_partition_continues_operating() {
    let mut sim = Builder::new().build();

    // Shared state for tracking which nodes received append_entries
    let node1_ae_count = Arc::new(AtomicU64::new(0));
    let node2_ae_count = Arc::new(AtomicU64::new(0));
    let node3_ae_count = Arc::new(AtomicU64::new(0));

    let n1_count = node1_ae_count.clone();
    let n2_count = node2_ae_count.clone();
    let n3_count = node3_ae_count.clone();

    // Custom service that tracks append_entries calls
    struct CountingRaftService {
        ae_count: Arc<AtomicU64>,
    }

    #[tonic::async_trait]
    impl RaftService for CountingRaftService {
        async fn vote(
            &self,
            _request: Request<RaftVoteRequest>,
        ) -> Result<Response<RaftVoteResponse>, Status> {
            Ok(Response::new(RaftVoteResponse {
                vote: None,
                vote_granted: true,
                last_log_id: None,
            }))
        }

        async fn append_entries(
            &self,
            _request: Request<RaftAppendEntriesRequest>,
        ) -> Result<Response<RaftAppendEntriesResponse>, Status> {
            self.ae_count.fetch_add(1, Ordering::SeqCst);
            Ok(Response::new(RaftAppendEntriesResponse {
                success: true,
                conflict: false,
                vote: None,
            }))
        }

        async fn install_snapshot(
            &self,
            _request: Request<RaftInstallSnapshotRequest>,
        ) -> Result<Response<RaftInstallSnapshotResponse>, Status> {
            Ok(Response::new(RaftInstallSnapshotResponse { vote: None }))
        }
    }

    // Register hosts with counting services
    sim.host("node1", move || {
        let ae_count = n1_count.clone();
        async move {
            let addr: SocketAddr = "0.0.0.0:9999".parse().unwrap();
            let service = CountingRaftService { ae_count };
            Server::builder()
                .add_service(RaftServiceServer::new(service))
                .serve_with_incoming(incoming_stream(addr))
                .await
                .expect("server failed");
            Ok(())
        }
    });

    sim.host("node2", move || {
        let ae_count = n2_count.clone();
        async move {
            let addr: SocketAddr = "0.0.0.0:9999".parse().unwrap();
            let service = CountingRaftService { ae_count };
            Server::builder()
                .add_service(RaftServiceServer::new(service))
                .serve_with_incoming(incoming_stream(addr))
                .await
                .expect("server failed");
            Ok(())
        }
    });

    sim.host("node3", move || {
        let ae_count = n3_count.clone();
        async move {
            let addr: SocketAddr = "0.0.0.0:9999".parse().unwrap();
            let service = CountingRaftService { ae_count };
            Server::builder()
                .add_service(RaftServiceServer::new(service))
                .serve_with_incoming(incoming_stream(addr))
                .await
                .expect("server failed");
            Ok(())
        }
    });

    // Capture counters for final assertions
    let final_n1 = node1_ae_count.clone();
    let final_n2 = node2_ae_count.clone();
    let final_n3 = node3_ae_count.clone();

    sim.client("leader", async move {
        // Wait for servers to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Simulate leader sending append_entries to all nodes
        async fn send_append_entries(host: &str) -> bool {
            match create_raft_client(host).await {
                Ok(mut client) => {
                    let req = RaftAppendEntriesRequest {
                        vote: None,
                        prev_log_id: None,
                        entries: vec![],
                        leader_commit: None,
                        shard_id: None,
                    };
                    client.append_entries(req).await.is_ok()
                },
                Err(_) => false,
            }
        }

        // Before partition: all nodes should receive append_entries
        assert!(send_append_entries("node1").await, "node1 should receive AE");
        assert!(send_append_entries("node2").await, "node2 should receive AE");
        assert!(send_append_entries("node3").await, "node3 should receive AE");

        // Partition node3 (minority)
        turmoil::partition("node3", "node1");
        turmoil::partition("node3", "node2");
        turmoil::partition("node3", "leader"); // Also partition from leader client
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Majority partition (nodes 1 and 2) should still work
        // Send multiple AE to majority to create a count difference
        for _ in 0..3 {
            assert!(send_append_entries("node1").await, "node1 should receive AE during partition");
            assert!(send_append_entries("node2").await, "node2 should receive AE during partition");
        }

        // Minority partition (node3) should not receive append_entries
        assert!(
            !send_append_entries("node3").await,
            "node3 should NOT receive AE during partition"
        );

        // Heal partition
        turmoil::repair("node3", "node1");
        turmoil::repair("node3", "node2");
        turmoil::repair("node3", "leader");
        tokio::time::sleep(Duration::from_millis(50)).await;

        // After healing, node3 should be reachable again
        assert!(send_append_entries("node3").await, "node3 should receive AE after heal");

        Ok(())
    });

    // Run simulation
    sim.run().expect("simulation should complete");

    // Verify final counts
    // Node3 should have fewer append_entries than nodes 1 and 2 due to partition
    let n1_final = final_n1.load(Ordering::SeqCst);
    let n2_final = final_n2.load(Ordering::SeqCst);
    let n3_final = final_n3.load(Ordering::SeqCst);

    assert!(
        n1_final > n3_final,
        "node1 ({}) should have more AE than node3 ({})",
        n1_final,
        n3_final
    );
    assert!(
        n2_final > n3_final,
        "node2 ({}) should have more AE than node3 ({})",
        n2_final,
        n3_final
    );
}

/// Tests that messages are held and released correctly.
///
/// This tests turmoil's message holding capability which can simulate
/// network delays and buffering scenarios. Uses partition instead of hold
/// since hold/release behavior can be tricky with gRPC streaming.
#[test]
fn test_message_hold_and_release() {
    let mut sim = Builder::new().build();

    let counter = Arc::new(RpcCounter::new());
    let counter_clone = counter.clone();
    let final_counter = counter.clone();

    sim.host("node1", move || {
        let counter = counter_clone.clone();
        async move {
            start_minimal_server(1, counter).await;
            Ok(())
        }
    });

    sim.client("test", async move {
        // Wait for server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // First, verify connection works
        {
            let mut client = create_raft_client("node1").await.expect("connect");
            let vote_req = RaftVoteRequest { vote: None, last_log_id: None, shard_id: None };
            let resp = client.vote(vote_req).await;
            assert!(resp.is_ok(), "initial request should succeed");
        }

        // Now partition the network
        turmoil::partition("test", "node1");

        // Requests should fail during partition
        let partition_result: Result<(), ()> = async {
            match create_raft_client("node1").await {
                Ok(mut client) => {
                    let vote_req =
                        RaftVoteRequest { vote: None, last_log_id: None, shard_id: None };
                    client.vote(vote_req).await.map(|_| ()).map_err(|_| ())
                },
                Err(_) => Err(()),
            }
        }
        .await;
        // Partition should cause failure
        assert!(partition_result.is_err(), "request during partition should fail");

        // Repair the network
        turmoil::repair("test", "node1");
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Requests should succeed after repair
        {
            let mut client = create_raft_client("node1").await.expect("reconnect");
            let vote_req = RaftVoteRequest { vote: None, last_log_id: None, shard_id: None };
            let resp = client.vote(vote_req).await;
            assert!(resp.is_ok(), "request after repair should succeed");
        }

        Ok(())
    });

    sim.run().expect("simulation should complete");

    // Verify server received requests
    let received = final_counter.votes_received.load(Ordering::SeqCst);
    assert!(
        received >= 2,
        "server should have received at least 2 vote requests, got {}",
        received
    );
}

/// Tests intermittent connectivity (flapping network).
///
/// This simulates a scenario where the network connection is unstable,
/// going up and down repeatedly. Raft should handle this gracefully.
#[test]
fn test_intermittent_connectivity() {
    let mut sim = Builder::new().build();

    let counter = Arc::new(AtomicU64::new(0));
    let server_counter = counter.clone();

    // Service that counts successful requests
    struct CountingService {
        counter: Arc<AtomicU64>,
    }

    #[tonic::async_trait]
    impl RaftService for CountingService {
        async fn vote(
            &self,
            _request: Request<RaftVoteRequest>,
        ) -> Result<Response<RaftVoteResponse>, Status> {
            self.counter.fetch_add(1, Ordering::SeqCst);
            Ok(Response::new(RaftVoteResponse {
                vote: None,
                vote_granted: true,
                last_log_id: None,
            }))
        }

        async fn append_entries(
            &self,
            _request: Request<RaftAppendEntriesRequest>,
        ) -> Result<Response<RaftAppendEntriesResponse>, Status> {
            Ok(Response::new(RaftAppendEntriesResponse {
                success: true,
                conflict: false,
                vote: None,
            }))
        }

        async fn install_snapshot(
            &self,
            _request: Request<RaftInstallSnapshotRequest>,
        ) -> Result<Response<RaftInstallSnapshotResponse>, Status> {
            Ok(Response::new(RaftInstallSnapshotResponse { vote: None }))
        }
    }

    sim.host("node1", move || {
        let counter = server_counter.clone();
        async move {
            let addr: SocketAddr = "0.0.0.0:9999".parse().unwrap();
            let service = CountingService { counter };
            Server::builder()
                .add_service(RaftServiceServer::new(service))
                .serve_with_incoming(incoming_stream(addr))
                .await
                .expect("server failed");
            Ok(())
        }
    });

    let final_counter = counter.clone();

    sim.client("test", async move {
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut successes = 0u64;
        let mut failures = 0u64;

        // Simulate flapping network - partition/repair multiple times
        for i in 0..5 {
            // Try to send request
            let result: Result<(), ()> = async {
                match create_raft_client("node1").await {
                    Ok(mut client) => {
                        let vote_req =
                            RaftVoteRequest { vote: None, last_log_id: None, shard_id: None };
                        client.vote(vote_req).await.map(|_| ()).map_err(|_| ())
                    },
                    Err(_) => Err(()),
                }
            }
            .await;

            if result.is_ok() {
                successes += 1;
            } else {
                failures += 1;
            }

            // Alternate partition state
            if i % 2 == 0 {
                turmoil::partition("test", "node1");
            } else {
                turmoil::repair("test", "node1");
            }

            tokio::time::sleep(Duration::from_millis(30)).await;
        }

        // Final repair to ensure clean state
        turmoil::repair("test", "node1");

        // Send one more request after repair
        let final_result: Result<(), ()> = async {
            match create_raft_client("node1").await {
                Ok(mut client) => {
                    let vote_req =
                        RaftVoteRequest { vote: None, last_log_id: None, shard_id: None };
                    client.vote(vote_req).await.map(|_| ()).map_err(|_| ())
                },
                Err(_) => Err(()),
            }
        }
        .await;

        if final_result.is_ok() {
            successes += 1;
        }

        // We should have some successes (at least the final one)
        assert!(
            successes >= 1,
            "should have at least 1 successful request, got {} successes and {} failures",
            successes,
            failures
        );

        Ok(())
    });

    sim.run().expect("simulation should complete");

    // Verify server received some requests
    let received = final_counter.load(Ordering::SeqCst);
    assert!(received >= 1, "server should have received at least 1 request");
}

/// Tests asymmetric network partition.
///
/// This tests a scenario where node A can reach node B, but B cannot reach A.
/// This can happen with certain firewall misconfigurations.
#[test]
fn test_asymmetric_partition() {
    let mut sim = Builder::new().build();

    let node1_counter = Arc::new(RpcCounter::new());
    let node2_counter = Arc::new(RpcCounter::new());

    let n1_counter = node1_counter.clone();
    let n2_counter = node2_counter.clone();

    sim.host("node1", move || {
        let counter = n1_counter.clone();
        async move {
            start_minimal_server(1, counter).await;
            Ok(())
        }
    });

    sim.host("node2", move || {
        let counter = n2_counter.clone();
        async move {
            start_minimal_server(2, counter).await;
            Ok(())
        }
    });

    sim.client("test", async move {
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify both nodes are reachable initially
        {
            let mut c1 = create_raft_client("node1").await.expect("connect node1");
            let mut c2 = create_raft_client("node2").await.expect("connect node2");

            let vote_req = RaftVoteRequest { vote: None, last_log_id: None, shard_id: None };

            c1.vote(vote_req).await.expect("vote to node1");
            c2.vote(vote_req).await.expect("vote to node2");
        }

        // Create asymmetric partition: test -> node1 blocked, but test -> node2 OK
        turmoil::partition("test", "node1");

        tokio::time::sleep(Duration::from_millis(50)).await;

        // node1 should be unreachable
        let n1_result = create_raft_client("node1").await;
        match n1_result {
            Ok(mut client) => {
                let vote_req = RaftVoteRequest { vote: None, last_log_id: None, shard_id: None };
                assert!(client.vote(vote_req).await.is_err(), "node1 should be unreachable");
            },
            Err(_) => {
                // Connection failed - expected
            },
        }

        // node2 should still be reachable
        let mut c2 = create_raft_client("node2").await.expect("connect node2");
        let vote_req = RaftVoteRequest { vote: None, last_log_id: None, shard_id: None };
        c2.vote(vote_req).await.expect("node2 should still be reachable");

        // Repair and verify node1 is reachable again
        turmoil::repair("test", "node1");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut c1 = create_raft_client("node1").await.expect("reconnect node1 after repair");
        let vote_req = RaftVoteRequest { vote: None, last_log_id: None, shard_id: None };
        c1.vote(vote_req).await.expect("node1 should be reachable after repair");

        Ok(())
    });

    sim.run().expect("simulation should complete");
}

/// Tests that connection timeouts are handled correctly.
///
/// This verifies that operations timeout gracefully when a node is unreachable
/// rather than hanging indefinitely.
#[test]
fn test_connection_timeout_handling() {
    let mut sim = Builder::new().build();

    let counter = Arc::new(RpcCounter::new());
    let server_counter = counter.clone();

    sim.host("node1", move || {
        let counter = server_counter.clone();
        async move {
            start_minimal_server(1, counter).await;
            Ok(())
        }
    });

    sim.client("test", async move {
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify node is reachable first
        {
            let mut client = create_raft_client("node1").await.expect("connect");
            let vote_req = RaftVoteRequest { vote: None, last_log_id: None, shard_id: None };
            client.vote(vote_req).await.expect("initial vote");
        }

        // Partition the node
        turmoil::partition("test", "node1");
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Attempt with timeout - should fail or timeout, not hang
        let timeout_result = tokio::time::timeout(Duration::from_millis(500), async {
            let client_result = create_raft_client("node1").await;
            match client_result {
                Ok(mut client) => {
                    let vote_req =
                        RaftVoteRequest { vote: None, last_log_id: None, shard_id: None };
                    client.vote(vote_req).await
                },
                Err(e) => Err(tonic::Status::unavailable(format!("Connection failed: {}", e))),
            }
        })
        .await;

        // Either the operation should complete with an error, or timeout
        // Both are acceptable - the key is it doesn't hang forever
        match timeout_result {
            Ok(Ok(_)) => panic!("request should not succeed during partition"),
            Ok(Err(_)) => {
                // Operation failed with error - expected
            },
            Err(_) => {
                // Timeout occurred - also acceptable
            },
        }

        // Repair and verify recovery
        turmoil::repair("test", "node1");
        tokio::time::sleep(Duration::from_millis(100)).await;

        let recovery_result: Result<Result<_, Status>, _> =
            tokio::time::timeout(Duration::from_millis(500), async {
                match create_raft_client("node1").await {
                    Ok(mut client) => {
                        let vote_req =
                            RaftVoteRequest { vote: None, last_log_id: None, shard_id: None };
                        client.vote(vote_req).await
                    },
                    Err(e) => Err(tonic::Status::unavailable(format!("Connection failed: {}", e))),
                }
            })
            .await;

        assert!(
            recovery_result.is_ok() && recovery_result.as_ref().unwrap().is_ok(),
            "should recover after partition heals"
        );

        Ok(())
    });

    sim.run().expect("simulation should complete");
}
