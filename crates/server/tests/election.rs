//! Leader election integration tests.
//!
//! Tests leader election behavior in a Raft cluster.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods,
    clippy::manual_range_contains
)]

mod common;

use std::time::Duration;

use common::TestCluster;
use serial_test::serial;

/// Test that a single-node cluster immediately elects itself as leader.
#[serial]
#[tokio::test]
async fn test_single_node_self_election() {
    let cluster = TestCluster::new(1).await;

    // Should quickly elect itself
    let leader_id = cluster.wait_for_leader_timeout(Duration::from_secs(5)).await;

    assert_eq!(leader_id, Some(1), "single node should elect itself as leader");
}

/// Test that a three-node cluster elects a leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_three_node_leader_election() {
    let cluster = TestCluster::new(3).await;

    // Wait for leader election with agreement across all nodes
    // wait_for_leader() now waits for ALL nodes to agree
    let leader_id = cluster.wait_for_leader().await;

    // Leader should be one of the nodes
    assert!(
        leader_id >= 1 && leader_id <= 3,
        "leader should be one of the nodes, got {}",
        leader_id
    );

    // Double-check leader consistency (should pass since wait_for_leader ensures agreement)
    for node in cluster.nodes() {
        let node_view = node.current_leader();
        assert_eq!(
            node_view,
            Some(leader_id),
            "node {} should agree on leader (saw {:?})",
            node.id,
            node_view
        );
    }
}

/// Test that all nodes eventually agree on the same term.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_term_agreement() {
    let cluster = TestCluster::new(3).await;
    let _leader_id = cluster.wait_for_leader().await;

    // Give a moment for term to propagate
    tokio::time::sleep(Duration::from_millis(100)).await;

    let terms: Vec<u64> = cluster.nodes().iter().map(|n| n.current_term()).collect();

    // All terms should be the same
    let first_term = terms[0];
    for (i, term) in terms.iter().enumerate() {
        assert_eq!(
            *term,
            first_term,
            "node {} term {} should match node 0 term {}",
            i + 1,
            term,
            first_term
        );
    }
}

/// Test that leader has higher or equal term than followers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_leader_term_dominance() {
    let cluster = TestCluster::new(3).await;
    let leader_id = cluster.wait_for_leader().await;

    // Get leader node by ID (more reliable than leader() which depends on is_leader())
    let leader = cluster.node(leader_id).expect("leader node should exist in cluster");
    let leader_term = leader.current_term();

    // Check all other nodes have term <= leader term
    for node in cluster.nodes() {
        if node.id == leader_id {
            continue;
        }
        assert!(
            node.current_term() <= leader_term,
            "node {} term {} should not exceed leader term {}",
            node.id,
            node.current_term(),
            leader_term
        );
    }
}
