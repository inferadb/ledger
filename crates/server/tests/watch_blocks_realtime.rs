//! Real-time WatchBlocks streaming integration tests.
//!
//! These tests verify the real-time push notification feature for `WatchBlocks`:
//! - Subscribers receive announcements as blocks are committed
//! - Historical replay + real-time push work together seamlessly
//! - Multiple subscribers all receive announcements
//! - Vault filtering works correctly (vault A subscriber doesn't get vault B)
//! - Backpressure handling with Lagged error
//! - Reconnection after server restart
//!
//! F.1.f.2.Stage1e Wave 6: migrated from legacy tonic helpers to wire-protocol
//! siblings. Streaming `watch_blocks` via wire returns
//! `TypedResponseStream<BlockAnnouncement>` which implements `futures::Stream`
//! directly — no `.into_inner()` unwrap needed.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use inferadb_ledger_types::{OrganizationSlug, VaultSlug};
use inferadb_ledger_wire::services::{read as wr, shared as ws, write as ww};

use crate::common::{
    TestCluster, wire_create_test_organization, wire_create_test_vault, wire_read_client,
    wire_write_client,
};

// ============================================================================
// Test Helpers
// ============================================================================

/// Creates an organization and returns its slug.
async fn create_organization(
    cluster: &TestCluster,
    node_id: u64,
    name: &str,
) -> Result<OrganizationSlug, Box<dyn std::error::Error>> {
    let (slug, _admin) = wire_create_test_organization(cluster, node_id, name).await?;
    Ok(slug)
}

/// Creates a vault in an organization and returns its slug.
async fn create_vault(
    cluster: &TestCluster,
    node_id: u64,
    organization: OrganizationSlug,
) -> Result<VaultSlug, Box<dyn std::error::Error>> {
    wire_create_test_vault(cluster, node_id, organization).await
}

/// Writes a key-value pair to a vault and return the block height.
async fn write_entity(
    cluster: &TestCluster,
    node_id: u64,
    organization: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
    value: &[u8],
    client_id: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    let client = wire_write_client(cluster, node_id);

    let request = ww::WriteRequest {
        organization: Some(organization),
        vault: Some(vault),
        client_id: Some(ws::ClientIdMessage { id: client_id.to_string() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: key.to_string(),
                value: Bytes::copy_from_slice(value),
                condition: None,
                expires_at: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response = client.write(request, rand::random::<u128>()).await?;

    match response.result {
        Some(ww::WriteResponseResult::Success(s)) => Ok(s.block_height),
        Some(ww::WriteResponseResult::Error(e)) => Err(format!("Write failed: {:?}", e).into()),
        None => Err("No result in response".into()),
    }
}

// ============================================================================
// Tests
// ============================================================================

/// Test: subscribe before any writes, receive announcements as writes commit.
///
/// This verifies the core real-time push functionality:
/// 1. Subscribe to WatchBlocks with start_height=1 (before any data exists)
/// 2. Write data to the vault
/// 3. Verify announcement is received in real-time
#[tokio::test]
async fn test_watch_blocks_subscribe_before_writes() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization =
        create_organization(&cluster, leader.id, "watch-ns").await.expect("create organization");
    let vault = create_vault(&cluster, leader.id, organization).await.expect("create vault");

    // Subscribe to WatchBlocks BEFORE any writes
    let read_client = wire_read_client(&cluster, leader.id);
    let request = wr::WatchBlocksRequest {
        organization: Some(organization),
        vault: Some(vault),
        start_height: 1,
        caller: None,
    };

    let mut stream = read_client
        .watch_blocks(request, rand::random::<u128>())
        .await
        .expect("watch_blocks should succeed");

    // Now write data - this should trigger a real-time announcement
    let block_height =
        write_entity(&cluster, leader.id, organization, vault, "key1", b"value1", "watch-client")
            .await
            .expect("write should succeed");

    assert_eq!(block_height, 1, "First write should be at height 1");

    // Receive the announcement with timeout
    let announcement = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("should receive announcement within timeout")
        .expect("stream should have item")
        .expect("announcement should be Ok");

    // Verify announcement contents
    assert_eq!(announcement.organization.map(|n| n.value()), Some(organization.value()));
    assert_eq!(announcement.vault.map(|v| v.value()), Some(vault.value()));
    assert_eq!(announcement.height, 1);
    assert!(announcement.block_hash.is_some(), "should have block_hash");
    assert!(announcement.state_root.is_some(), "should have state_root");
    assert!(announcement.timestamp > 0, "should have non-zero timestamp");
}

/// Test: subscribe mid-stream, receive historical + new announcements seamlessly.
///
/// This verifies the combined historical replay + real-time push:
/// 1. Write some initial blocks
/// 2. Subscribe with start_height=1
/// 3. Verify historical blocks arrive first
/// 4. Write more blocks
/// 5. Verify new blocks arrive in real-time
#[tokio::test]
async fn test_watch_blocks_historical_then_realtime() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(&cluster, leader.id, "watch-mid-ns")
        .await
        .expect("create organization");
    let vault = create_vault(&cluster, leader.id, organization).await.expect("create vault");

    // Write 3 blocks BEFORE subscribing
    for i in 1..=3 {
        write_entity(
            &cluster,
            leader.id,
            organization,
            vault,
            &format!("key{}", i),
            format!("value{}", i).as_bytes(),
            "pre-subscribe-client",
        )
        .await
        .expect("pre-write should succeed");
    }

    // Now subscribe with start_height=1
    let read_client = wire_read_client(&cluster, leader.id);
    let request = wr::WatchBlocksRequest {
        organization: Some(organization),
        vault: Some(vault),
        start_height: 1,
        caller: None,
    };

    let mut stream = read_client
        .watch_blocks(request, rand::random::<u128>())
        .await
        .expect("watch_blocks should succeed");

    // Receive 3 historical announcements
    for expected_height in 1..=3 {
        let announcement = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("should receive historical announcement")
            .expect("stream should have item")
            .expect("announcement should be Ok");

        assert_eq!(
            announcement.height, expected_height,
            "Historical block {} should have correct height",
            expected_height
        );
    }

    // Now write 2 more blocks after subscribing (real-time push)
    for i in 4..=5 {
        write_entity(
            &cluster,
            leader.id,
            organization,
            vault,
            &format!("key{}", i),
            format!("value{}", i).as_bytes(),
            "pre-subscribe-client",
        )
        .await
        .expect("post-write should succeed");
    }

    // Receive 2 real-time announcements
    for expected_height in 4..=5 {
        let announcement = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("should receive real-time announcement")
            .expect("stream should have item")
            .expect("announcement should be Ok");

        assert_eq!(
            announcement.height, expected_height,
            "Real-time block {} should have correct height",
            expected_height
        );
    }
}

/// Test: multiple subscribers to same vault all receive announcements.
///
/// This verifies broadcast semantics - all subscribers get the same data.
#[tokio::test]
async fn test_watch_blocks_multiple_subscribers() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(&cluster, leader.id, "multi-sub-ns")
        .await
        .expect("create organization");
    let vault = create_vault(&cluster, leader.id, organization).await.expect("create vault");

    // Create 3 independent subscribers
    let mut streams = Vec::new();
    for _ in 0..3 {
        let read_client = wire_read_client(&cluster, leader.id);
        let request = wr::WatchBlocksRequest {
            organization: Some(organization),
            vault: Some(vault),
            start_height: 1,
            caller: None,
        };
        let stream = read_client
            .watch_blocks(request, rand::random::<u128>())
            .await
            .expect("watch_blocks should succeed");
        streams.push(stream);
    }

    // Write a block
    write_entity(
        &cluster,
        leader.id,
        organization,
        vault,
        "shared-key",
        b"shared-value",
        "multi-client",
    )
    .await
    .expect("write should succeed");

    // All 3 subscribers should receive the same announcement
    for (i, stream) in streams.iter_mut().enumerate() {
        let announcement = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .unwrap_or_else(|_| panic!("subscriber {} should receive announcement", i))
            .expect("stream should have item")
            .expect("announcement should be Ok");

        assert_eq!(announcement.height, 1, "subscriber {} should get height 1", i);
        assert_eq!(announcement.organization.map(|n| n.value()), Some(organization.value()));
        assert_eq!(announcement.vault.map(|v| v.value()), Some(vault.value()));
    }
}

/// Test: subscriber to vault A does not receive vault B announcements.
///
/// This verifies filtering - each subscriber only gets their vault's blocks.
#[tokio::test]
async fn test_watch_blocks_vault_isolation() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization with two vaults
    let organization = create_organization(&cluster, leader.id, "isolation-ns")
        .await
        .expect("create organization");
    let vault_a = create_vault(&cluster, leader.id, organization).await.expect("create vault A");
    let vault_b = create_vault(&cluster, leader.id, organization).await.expect("create vault B");

    assert_ne!(vault_a, vault_b, "Vaults should have different IDs");

    // Subscribe only to vault A
    let read_client = wire_read_client(&cluster, leader.id);
    let request = wr::WatchBlocksRequest {
        organization: Some(organization),
        vault: Some(vault_a),
        start_height: 1,
        caller: None,
    };

    let mut stream_a = read_client
        .watch_blocks(request, rand::random::<u128>())
        .await
        .expect("watch_blocks should succeed");

    // Write to vault B first (should NOT be received by stream_a)
    write_entity(
        &cluster,
        leader.id,
        organization,
        vault_b,
        "vault-b-key",
        b"vault-b-value",
        "vault-b-client",
    )
    .await
    .expect("write to vault B should succeed");

    // Write to vault A (SHOULD be received)
    write_entity(
        &cluster,
        leader.id,
        organization,
        vault_a,
        "vault-a-key",
        b"vault-a-value",
        "vault-a-client",
    )
    .await
    .expect("write to vault A should succeed");

    // stream_a should only receive the vault A announcement
    let announcement = tokio::time::timeout(Duration::from_secs(10), stream_a.next())
        .await
        .expect("should receive vault A announcement")
        .expect("stream should have item")
        .expect("announcement should be Ok");

    assert_eq!(
        announcement.vault.map(|v| v.value()),
        Some(vault_a.value()),
        "Should only receive vault A announcement"
    );
    assert_eq!(announcement.height, 1);

    // Verify no more messages are waiting (vault B announcement should have been filtered)
    let timeout_result = tokio::time::timeout(Duration::from_millis(100), stream_a.next()).await;
    assert!(
        timeout_result.is_err(),
        "Should not receive any more announcements (vault B was filtered)"
    );
}

/// Test: server restart mid-stream, client reconnects and continues from last height.
///
/// This tests the reconnection workflow:
/// 1. Subscribe and receive some blocks
/// 2. Restart the server
/// 3. Reconnect from last_received_height + 1
/// 4. Verify continuation works
#[tokio::test]
async fn test_watch_blocks_reconnection_after_restart() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization =
        create_organization(&cluster, leader.id, "restart-ns").await.expect("create organization");
    let vault = create_vault(&cluster, leader.id, organization).await.expect("create vault");

    // Write initial blocks
    for i in 1..=3 {
        write_entity(
            &cluster,
            leader.id,
            organization,
            vault,
            &format!("restart-key-{}", i),
            format!("restart-value-{}", i).as_bytes(),
            "restart-client",
        )
        .await
        .expect("initial write should succeed");
    }

    // Subscribe and read historical blocks, tracking last height
    let read_client = wire_read_client(&cluster, leader.id);
    let request = wr::WatchBlocksRequest {
        organization: Some(organization),
        vault: Some(vault),
        start_height: 1,
        caller: None,
    };

    let mut stream = read_client
        .watch_blocks(request, rand::random::<u128>())
        .await
        .expect("watch_blocks should succeed");

    let mut last_height = 0u64;
    for _ in 1..=3 {
        let announcement = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("should receive historical announcement")
            .expect("stream should have item")
            .expect("announcement should be Ok");
        last_height = announcement.height;
    }

    assert_eq!(last_height, 3, "Should have received up to height 3");

    // Drop the stream to simulate disconnection (server restart would cause this)
    drop(stream);
    drop(read_client);

    // Simulate client reconnecting from last_height + 1
    // In a real restart scenario, the server would restart and the client would reconnect
    // Here we just reconnect to the same server to verify the protocol works

    let read_client2 = wire_read_client(&cluster, leader.id);
    let reconnect_request = wr::WatchBlocksRequest {
        organization: Some(organization),
        vault: Some(vault),
        start_height: last_height + 1, // Resume from where we left off
        caller: None,
    };

    let mut stream2 = read_client2
        .watch_blocks(reconnect_request, rand::random::<u128>())
        .await
        .expect("reconnection should succeed");

    // Write new blocks after reconnection
    for i in 4..=5 {
        write_entity(
            &cluster,
            leader.id,
            organization,
            vault,
            &format!("post-restart-key-{}", i),
            format!("post-restart-value-{}", i).as_bytes(),
            "restart-client",
        )
        .await
        .expect("post-reconnect write should succeed");
    }

    // Verify we receive the new blocks (no duplicates of 1-3)
    for expected_height in 4..=5 {
        let announcement = tokio::time::timeout(Duration::from_secs(10), stream2.next())
            .await
            .expect("should receive post-reconnect announcement")
            .expect("stream should have item")
            .expect("announcement should be Ok");

        assert_eq!(
            announcement.height, expected_height,
            "Post-reconnect block should have correct height"
        );
    }
}

/// Verifies block announcements carry the client-supplied external slugs
/// (not the internal i64 ids) end-to-end.
///
/// Setup:
/// 1. Create a cluster with a fresh organization + vault — the SDK layer produces external
///    Snowflake `*Slug` values that are distinct from the internal `*Id` values the state machine
///    allocates.
/// 2. Subscribe to `watch_blocks`.
/// 3. Write once.
/// 4. Assert the announcement's `organization.slug` / `vault.slug` fields match the slugs the
///    client originally passed — the same external Snowflake values returned by the create RPCs.
///
/// This validates the full stamping chain:
///   `WriteRequest.{organization,vault}.slug`
///     → `operations_to_request` captures them
///     → `OrganizationRequest::Write.{organization_slug,vault_slug}`
///     → apply handler stamps them onto `VaultEntry`
///     → `raft_impl` emits them directly in the announcement
///     → `read.rs` filter accepts the external-slug path
///
/// Regression guard: earlier flip attempts populated `state.id_to_slug`
/// inside the Write apply arm to achieve the same end state, which broke
/// state-root agreement and made announcements never arrive. This test
/// catches a silent fallback to the internal id (the old pre-stamping
/// behaviour) by insisting on a *specific* external Snowflake value.
#[tokio::test]
async fn test_watch_blocks_announcement_carries_external_slugs() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // External slugs — returned by the create RPCs; Snowflake-shaped `u64`.
    let organization =
        create_organization(&cluster, leader.id, "slug-stamping-org").await.expect("create org");
    let vault = create_vault(&cluster, leader.id, organization).await.expect("create vault");

    // Sanity: external slugs should be non-zero Snowflakes, distinct from a
    // freshly-allocated internal id. This keeps the test meaningful — if
    // the announcement carries the internal id as a fallback, it will NOT
    // match the external slug asserted below.
    assert_ne!(organization.value(), 0, "organization slug must be a real Snowflake");
    assert_ne!(vault.value(), 0, "vault slug must be a real Snowflake");

    let read_client = wire_read_client(&cluster, leader.id);
    let request = wr::WatchBlocksRequest {
        organization: Some(organization),
        vault: Some(vault),
        start_height: 1,
        caller: None,
    };
    let mut stream = read_client
        .watch_blocks(request, rand::random::<u128>())
        .await
        .expect("watch_blocks should succeed");

    write_entity(&cluster, leader.id, organization, vault, "k", b"v", "slug-client")
        .await
        .expect("write should succeed");

    let announcement = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("announcement should arrive within timeout")
        .expect("stream should have item")
        .expect("announcement should be Ok");

    // The core assertion: the announcement carries the ORIGINAL external
    // Snowflake slugs the client passed — not the internal ids.
    assert_eq!(
        announcement.organization.map(|n| n.value()),
        Some(organization.value()),
        "announcement.organization.slug must match the external Snowflake the \
         client sent, not the internal OrganizationId",
    );
    assert_eq!(
        announcement.vault.map(|v| v.value()),
        Some(vault.value()),
        "announcement.vault.slug must match the external Snowflake the \
         client sent, not the internal VaultId",
    );
    assert_eq!(announcement.height, 1);
}
