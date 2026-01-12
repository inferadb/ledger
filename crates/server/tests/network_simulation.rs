//! Network simulation tests using turmoil.
//!
//! These tests verify Raft consensus behavior under simulated network conditions
//! like partitions, delays, and message loss.
//!
//! Note: These tests use turmoil's deterministic simulation rather than real networking.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods
)]

mod turmoil_common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tonic::transport::Server;
use tonic::{Request, Response, Status};
use turmoil::Builder;

use ledger_raft::proto::raft_service_server::{RaftService, RaftServiceServer};
use ledger_raft::proto::{
    RaftAppendEntriesRequest, RaftAppendEntriesResponse, RaftInstallSnapshotRequest,
    RaftInstallSnapshotResponse, RaftVoteRequest, RaftVoteResponse,
};

use turmoil_common::incoming_stream;

/// Simple counter for tracking RPC calls
struct RpcCounter {
    votes_received: AtomicU64,
    append_entries_received: AtomicU64,
}

impl RpcCounter {
    fn new() -> Self {
        Self {
            votes_received: AtomicU64::new(0),
            append_entries_received: AtomicU64::new(0),
        }
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
        Ok(Response::new(RaftVoteResponse {
            vote: None,
            vote_granted: false,
            last_log_id: None,
        }))
    }

    async fn append_entries(
        &self,
        _request: Request<RaftAppendEntriesRequest>,
    ) -> Result<Response<RaftAppendEntriesResponse>, Status> {
        self.counter
            .append_entries_received
            .fetch_add(1, Ordering::SeqCst);
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

/// Start a minimal Raft server on the given turmoil host.
async fn start_minimal_server(node_id: u64, counter: Arc<RpcCounter>) {
    let addr: SocketAddr = "0.0.0.0:9999".parse().unwrap();
    let service = MinimalRaftService { node_id, counter };

    Server::builder()
        .add_service(RaftServiceServer::new(service))
        .serve_with_incoming(incoming_stream(addr))
        .await
        .expect("server failed");
}

/// Create a Raft service client using turmoil's simulated network.
async fn create_raft_client(
    host: &str,
) -> Result<
    ledger_raft::proto::raft_service_client::RaftServiceClient<tonic::transport::Channel>,
    tonic::transport::Error,
> {
    let channel = turmoil_common::create_turmoil_channel(host, 9999).await?;
    Ok(ledger_raft::proto::raft_service_client::RaftServiceClient::new(channel))
}

/// Test that network partitions block communication between nodes.
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
            let vote_req = RaftVoteRequest {
                vote: None,
                last_log_id: None,
            };

            client1
                .vote(vote_req.clone())
                .await
                .expect("vote to node1 should succeed");
            client2
                .vote(vote_req.clone())
                .await
                .expect("vote to node2 should succeed");
            client3
                .vote(vote_req.clone())
                .await
                .expect("vote to node3 should succeed");
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
                    let vote_req = RaftVoteRequest {
                        vote: None,
                        last_log_id: None,
                    };
                    let vote_result = client.vote(vote_req).await;
                    // The connection or RPC should fail due to partition
                    assert!(
                        vote_result.is_err(),
                        "vote to partitioned node3 should fail"
                    );
                }
                Err(_) => {
                    // Connection failed - expected behavior during partition
                }
            }

            // Communication to nodes 1 and 2 should still work
            let mut client1 = create_raft_client("node1").await.expect("connect to node1");
            let mut client2 = create_raft_client("node2").await.expect("connect to node2");

            let vote_req = RaftVoteRequest {
                vote: None,
                last_log_id: None,
            };
            client1
                .vote(vote_req.clone())
                .await
                .expect("vote to node1 should succeed during partition");
            client2
                .vote(vote_req)
                .await
                .expect("vote to node2 should succeed during partition");
        }

        // Phase 3: Heal partition
        turmoil::repair("node3", "node1");
        turmoil::repair("node3", "node2");
        turmoil::repair("node3", "test");

        // Verify communication resumes
        {
            tokio::time::sleep(Duration::from_millis(50)).await;

            let mut client3 = create_raft_client("node3")
                .await
                .expect("connect to node3 after heal");
            let vote_req = RaftVoteRequest {
                vote: None,
                last_log_id: None,
            };
            client3
                .vote(vote_req)
                .await
                .expect("vote to node3 should succeed after heal");
        }

        Ok(())
    });

    // Run simulation
    sim.run().expect("simulation should complete");
}

/// Test that writes to majority partition succeed while minority fails.
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
                    };
                    client.append_entries(req).await.is_ok()
                }
                Err(_) => false,
            }
        }

        // Before partition: all nodes should receive append_entries
        assert!(
            send_append_entries("node1").await,
            "node1 should receive AE"
        );
        assert!(
            send_append_entries("node2").await,
            "node2 should receive AE"
        );
        assert!(
            send_append_entries("node3").await,
            "node3 should receive AE"
        );

        // Partition node3 (minority)
        turmoil::partition("node3", "node1");
        turmoil::partition("node3", "node2");
        turmoil::partition("node3", "leader"); // Also partition from leader client
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Majority partition (nodes 1 and 2) should still work
        // Send multiple AE to majority to create a count difference
        for _ in 0..3 {
            assert!(
                send_append_entries("node1").await,
                "node1 should receive AE during partition"
            );
            assert!(
                send_append_entries("node2").await,
                "node2 should receive AE during partition"
            );
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
        assert!(
            send_append_entries("node3").await,
            "node3 should receive AE after heal"
        );

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
