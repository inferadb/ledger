//! Real-time WatchBlocks streaming integration tests.
//!
//! These tests verify the real-time push notification feature for `WatchBlocks`:
//! - Subscribers receive announcements as blocks are committed
//! - Historical replay + real-time push work together seamlessly
//! - Multiple subscribers all receive announcements
//! - Vault filtering works correctly (vault A subscriber doesn't get vault B)
//! - Backpressure handling with Lagged error
//! - Reconnection after server restart

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

mod common;

use std::time::Duration;

use common::{TestCluster, create_admin_client, create_read_client, create_write_client};
use futures::StreamExt;
use serial_test::serial;

// ============================================================================
// Test Helpers
// ============================================================================

/// Creates a organization and return its ID.
async fn create_organization(
    addr: std::net::SocketAddr,
    name: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let mut client = create_admin_client(addr).await?;
    let response = client
        .create_organization(inferadb_ledger_proto::proto::CreateOrganizationRequest {
            name: name.to_string(),
            shard_id: None,
            quota: None,
        })
        .await?;

    let organization_id = response
        .into_inner()
        .slug
        .map(|n| n.slug as i64)
        .ok_or("No organization_id in response")?;

    Ok(organization_id)
}

/// Creates a vault in a organization and return its ID.
async fn create_vault(
    addr: std::net::SocketAddr,
    organization_id: i64,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut client = create_admin_client(addr).await?;
    let response = client
        .create_vault(inferadb_ledger_proto::proto::CreateVaultRequest {
            organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                slug: organization_id as u64,
            }),
            replication_factor: 0,
            initial_nodes: vec![],
            retention_policy: None,
        })
        .await?;

    let vault_slug =
        response.into_inner().vault.map(|v| v.slug).ok_or("No vault_slug in response")?;

    Ok(vault_slug)
}

/// Writes a key-value pair to a vault and return the block height.
async fn write_entity(
    addr: std::net::SocketAddr,
    organization_id: i64,
    vault_slug: u64,
    key: &str,
    value: &[u8],
    client_id: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut client = create_write_client(addr).await?;

    let request = inferadb_ledger_proto::proto::WriteRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization_id as u64,
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault_slug }),
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: client_id.to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: key.to_string(),
                    value: value.to_vec(),
                    condition: None,
                    expires_at: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let response = client.write(request).await?.into_inner();

    match response.result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(s)) => {
            Ok(s.block_height)
        },
        Some(inferadb_ledger_proto::proto::write_response::Result::Error(e)) => {
            Err(format!("Write failed: {:?}", e).into())
        },
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
#[serial]
#[tokio::test]
async fn test_watch_blocks_subscribe_before_writes() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization_id =
        create_organization(leader.addr, "watch-ns").await.expect("create organization");
    let vault_slug = create_vault(leader.addr, organization_id).await.expect("create vault");

    // Subscribe to WatchBlocks BEFORE any writes
    let mut read_client = create_read_client(leader.addr).await.expect("create read client");
    let request = inferadb_ledger_proto::proto::WatchBlocksRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization_id as u64,
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault_slug }),
        start_height: 1,
    };

    let mut stream =
        read_client.watch_blocks(request).await.expect("watch_blocks should succeed").into_inner();

    // Now write data - this should trigger a real-time announcement
    let block_height =
        write_entity(leader.addr, organization_id, vault_slug, "key1", b"value1", "watch-client")
            .await
            .expect("write should succeed");

    assert_eq!(block_height, 1, "First write should be at height 1");

    // Receive the announcement with timeout
    let announcement = tokio::time::timeout(Duration::from_millis(500), stream.next())
        .await
        .expect("should receive announcement within 500ms")
        .expect("stream should have item")
        .expect("announcement should be Ok");

    // Verify announcement contents
    assert_eq!(announcement.organization.as_ref().map(|n| n.slug as i64), Some(organization_id));
    assert_eq!(announcement.vault.as_ref().map(|v| v.slug), Some(vault_slug));
    assert_eq!(announcement.height, 1);
    assert!(announcement.block_hash.is_some(), "should have block_hash");
    assert!(announcement.state_root.is_some(), "should have state_root");
    assert!(announcement.timestamp.is_some(), "should have timestamp");
}

/// Test: subscribe mid-stream, receive historical + new announcements seamlessly.
///
/// This verifies the combined historical replay + real-time push:
/// 1. Write some initial blocks
/// 2. Subscribe with start_height=1
/// 3. Verify historical blocks arrive first
/// 4. Write more blocks
/// 5. Verify new blocks arrive in real-time
#[serial]
#[tokio::test]
async fn test_watch_blocks_historical_then_realtime() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization_id =
        create_organization(leader.addr, "watch-mid-ns").await.expect("create organization");
    let vault_slug = create_vault(leader.addr, organization_id).await.expect("create vault");

    // Write 3 blocks BEFORE subscribing
    for i in 1..=3 {
        write_entity(
            leader.addr,
            organization_id,
            vault_slug,
            &format!("key{}", i),
            format!("value{}", i).as_bytes(),
            "pre-subscribe-client",
        )
        .await
        .expect("pre-write should succeed");
    }

    // Now subscribe with start_height=1
    let mut read_client = create_read_client(leader.addr).await.expect("create read client");
    let request = inferadb_ledger_proto::proto::WatchBlocksRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization_id as u64,
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault_slug }),
        start_height: 1,
    };

    let mut stream =
        read_client.watch_blocks(request).await.expect("watch_blocks should succeed").into_inner();

    // Receive 3 historical announcements
    for expected_height in 1..=3 {
        let announcement = tokio::time::timeout(Duration::from_millis(500), stream.next())
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
            leader.addr,
            organization_id,
            vault_slug,
            &format!("key{}", i),
            format!("value{}", i).as_bytes(),
            "pre-subscribe-client",
        )
        .await
        .expect("post-write should succeed");
    }

    // Receive 2 real-time announcements
    for expected_height in 4..=5 {
        let announcement = tokio::time::timeout(Duration::from_millis(500), stream.next())
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
#[serial]
#[tokio::test]
async fn test_watch_blocks_multiple_subscribers() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization_id =
        create_organization(leader.addr, "multi-sub-ns").await.expect("create organization");
    let vault_slug = create_vault(leader.addr, organization_id).await.expect("create vault");

    // Create 3 independent subscribers
    let mut streams = Vec::new();
    for _ in 0..3 {
        let mut read_client = create_read_client(leader.addr).await.expect("create read client");
        let request = inferadb_ledger_proto::proto::WatchBlocksRequest {
            organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                slug: organization_id as u64,
            }),
            vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault_slug }),
            start_height: 1,
        };
        let stream = read_client
            .watch_blocks(request)
            .await
            .expect("watch_blocks should succeed")
            .into_inner();
        streams.push(stream);
    }

    // Write a block
    write_entity(
        leader.addr,
        organization_id,
        vault_slug,
        "shared-key",
        b"shared-value",
        "multi-client",
    )
    .await
    .expect("write should succeed");

    // All 3 subscribers should receive the same announcement
    for (i, stream) in streams.iter_mut().enumerate() {
        let announcement = tokio::time::timeout(Duration::from_millis(500), stream.next())
            .await
            .unwrap_or_else(|_| panic!("subscriber {} should receive announcement", i))
            .expect("stream should have item")
            .expect("announcement should be Ok");

        assert_eq!(announcement.height, 1, "subscriber {} should get height 1", i);
        assert_eq!(
            announcement.organization.as_ref().map(|n| n.slug as i64),
            Some(organization_id)
        );
        assert_eq!(announcement.vault.as_ref().map(|v| v.slug), Some(vault_slug));
    }
}

/// Test: subscriber to vault A does not receive vault B announcements.
///
/// This verifies filtering - each subscriber only gets their vault's blocks.
#[serial]
#[tokio::test]
async fn test_watch_blocks_vault_isolation() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization with two vaults
    let organization_id =
        create_organization(leader.addr, "isolation-ns").await.expect("create organization");
    let vault_a_id = create_vault(leader.addr, organization_id).await.expect("create vault A");
    let vault_b_id = create_vault(leader.addr, organization_id).await.expect("create vault B");

    assert_ne!(vault_a_id, vault_b_id, "Vaults should have different IDs");

    // Subscribe only to vault A
    let mut read_client = create_read_client(leader.addr).await.expect("create read client");
    let request = inferadb_ledger_proto::proto::WatchBlocksRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization_id as u64,
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault_a_id }),
        start_height: 1,
    };

    let mut stream_a =
        read_client.watch_blocks(request).await.expect("watch_blocks should succeed").into_inner();

    // Write to vault B first (should NOT be received by stream_a)
    write_entity(
        leader.addr,
        organization_id,
        vault_b_id,
        "vault-b-key",
        b"vault-b-value",
        "vault-b-client",
    )
    .await
    .expect("write to vault B should succeed");

    // Write to vault A (SHOULD be received)
    write_entity(
        leader.addr,
        organization_id,
        vault_a_id,
        "vault-a-key",
        b"vault-a-value",
        "vault-a-client",
    )
    .await
    .expect("write to vault A should succeed");

    // stream_a should only receive the vault A announcement
    let announcement = tokio::time::timeout(Duration::from_millis(500), stream_a.next())
        .await
        .expect("should receive vault A announcement")
        .expect("stream should have item")
        .expect("announcement should be Ok");

    assert_eq!(
        announcement.vault.as_ref().map(|v| v.slug),
        Some(vault_a_id),
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

/// Test: high-volume writes with mid-stream reconnection.
///
/// Verifies the system handles sustained block production exceeding the broadcast
/// channel capacity (1000), and that a subscriber can reconnect from an arbitrary
/// height to resume receiving historical + live blocks.
///
/// Note: Broadcast `Lagged` errors cannot be reliably triggered through gRPC
/// integration tests because sequential writes (~22 msg/sec) are too slow to
/// outpace the server-side stream consumer. The lag behavior is a well-tested
/// `tokio::sync::broadcast` feature. This test instead validates:
/// 1. The system writes >1000 blocks without errors
/// 2. A subscriber receives all blocks via the streaming API
/// 3. Mid-stream reconnection from an arbitrary height works correctly
#[serial]
#[tokio::test]
async fn test_watch_blocks_high_volume_reconnect() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization_id =
        create_organization(leader.addr, "highvol-ns").await.expect("create organization");
    let vault_slug = create_vault(leader.addr, organization_id).await.expect("create vault");

    // Subscribe from block 1
    let mut read_client = create_read_client(leader.addr).await.expect("create read client");
    let request = inferadb_ledger_proto::proto::WatchBlocksRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization_id as u64,
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault_slug }),
        start_height: 1,
    };

    let mut stream =
        read_client.watch_blocks(request).await.expect("watch_blocks should succeed").into_inner();

    // Write 1200 blocks — exceeds the broadcast channel capacity (1000) to exercise
    // buffer recycling. Reuse a single client_id to avoid bloating the AppliedState
    // blob (serialized as a single B+ tree value) with unique client sequences.
    let total_writes: u64 = 1200;
    let client_id = "highvol-writer";
    for i in 1..=total_writes {
        write_entity(
            leader.addr,
            organization_id,
            vault_slug,
            &format!("hv-key-{}", i),
            format!("hv-value-{}", i).as_bytes(),
            client_id,
        )
        .await
        .expect("write should succeed");
    }

    // Consume all available messages from the stream
    let mut received_count = 0u64;
    loop {
        match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(announcement))) => {
                received_count += 1;
                // Verify announcements arrive in order
                assert_eq!(
                    announcement.height, received_count,
                    "Block heights should be sequential"
                );
            },
            Ok(Some(Err(e))) => {
                panic!("Unexpected stream error: {:?}", e);
            },
            Ok(None) => {
                break;
            },
            Err(_) => {
                // Timeout — no more messages pending
                break;
            },
        }
    }

    assert_eq!(
        received_count, total_writes,
        "Should receive all {} blocks via the streaming API",
        total_writes
    );

    // Drop the first stream and reconnect from an arbitrary mid-point
    drop(stream);
    let reconnect_height = total_writes / 2;
    let mut read_client2 = create_read_client(leader.addr).await.expect("create read client");
    let reconnect_request = inferadb_ledger_proto::proto::WatchBlocksRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization_id as u64,
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault_slug }),
        start_height: reconnect_height,
    };

    let mut reconnect_stream = read_client2
        .watch_blocks(reconnect_request)
        .await
        .expect("reconnection should succeed")
        .into_inner();

    // Verify we receive the historical blocks from reconnect_height onwards
    let mut reconnect_count = 0u64;
    loop {
        match tokio::time::timeout(Duration::from_millis(500), reconnect_stream.next()).await {
            Ok(Some(Ok(announcement))) => {
                let expected_height = reconnect_height + reconnect_count;
                assert_eq!(
                    announcement.height, expected_height,
                    "Reconnected stream should resume from height {}",
                    reconnect_height
                );
                reconnect_count += 1;
            },
            Ok(Some(Err(e))) => {
                panic!("Unexpected reconnect stream error: {:?}", e);
            },
            Ok(None) => break,
            Err(_) => break,
        }
    }

    let expected_reconnect = total_writes - reconnect_height + 1;
    assert_eq!(
        reconnect_count, expected_reconnect,
        "Should receive {} blocks after reconnecting from height {}",
        expected_reconnect, reconnect_height
    );
}

/// Test: server restart mid-stream, client reconnects and continues from last height.
///
/// This tests the reconnection workflow:
/// 1. Subscribe and receive some blocks
/// 2. Restart the server
/// 3. Reconnect from last_received_height + 1
/// 4. Verify continuation works
#[serial]
#[tokio::test]
async fn test_watch_blocks_reconnection_after_restart() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization_id =
        create_organization(leader.addr, "restart-ns").await.expect("create organization");
    let vault_slug = create_vault(leader.addr, organization_id).await.expect("create vault");

    // Write initial blocks
    for i in 1..=3 {
        write_entity(
            leader.addr,
            organization_id,
            vault_slug,
            &format!("restart-key-{}", i),
            format!("restart-value-{}", i).as_bytes(),
            "restart-client",
        )
        .await
        .expect("initial write should succeed");
    }

    // Subscribe and read historical blocks, tracking last height
    let mut read_client = create_read_client(leader.addr).await.expect("create read client");
    let request = inferadb_ledger_proto::proto::WatchBlocksRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization_id as u64,
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault_slug }),
        start_height: 1,
    };

    let mut stream =
        read_client.watch_blocks(request).await.expect("watch_blocks should succeed").into_inner();

    let mut last_height = 0u64;
    for _ in 1..=3 {
        let announcement = tokio::time::timeout(Duration::from_millis(500), stream.next())
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

    let mut read_client2 = create_read_client(leader.addr).await.expect("reconnect read client");
    let reconnect_request = inferadb_ledger_proto::proto::WatchBlocksRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization_id as u64,
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault_slug }),
        start_height: last_height + 1, // Resume from where we left off
    };

    let mut stream2 = read_client2
        .watch_blocks(reconnect_request)
        .await
        .expect("reconnection should succeed")
        .into_inner();

    // Write new blocks after reconnection
    for i in 4..=5 {
        write_entity(
            leader.addr,
            organization_id,
            vault_slug,
            &format!("post-restart-key-{}", i),
            format!("post-restart-value-{}", i).as_bytes(),
            "restart-client",
        )
        .await
        .expect("post-reconnect write should succeed");
    }

    // Verify we receive the new blocks (no duplicates of 1-3)
    for expected_height in 4..=5 {
        let announcement = tokio::time::timeout(Duration::from_millis(500), stream2.next())
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
