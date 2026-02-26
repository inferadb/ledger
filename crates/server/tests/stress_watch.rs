//! Watch blocks high-volume stress test extracted from `watch_blocks_realtime.rs`.
//!
//! Exercises the WatchBlocks streaming API under sustained block production
//! exceeding broadcast channel capacity, with mid-stream reconnection.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use futures::StreamExt;

use crate::common::{TestCluster, create_admin_client, create_read_client, create_write_client};

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

    let vault = response.into_inner().vault.map(|v| v.slug).ok_or("No vault in response")?;

    Ok(vault)
}

/// Writes a key-value pair to a vault and return the block height.
async fn write_entity(
    addr: std::net::SocketAddr,
    organization_id: i64,
    vault: u64,
    key: &str,
    value: &[u8],
    client_id: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut client = create_write_client(addr).await?;

    let request = inferadb_ledger_proto::proto::WriteRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization_id as u64,
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault }),
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

/// Test: high-volume writes with mid-stream reconnection.
///
/// Verifies the system handles sustained block production and that a subscriber
/// can reconnect from an arbitrary height to resume receiving historical + live
/// blocks.
///
/// Note: Broadcast `Lagged` errors cannot be reliably triggered through gRPC
/// integration tests because sequential writes (~22 msg/sec) are too slow to
/// outpace the server-side stream consumer. The lag behavior is a well-tested
/// `tokio::sync::broadcast` feature. This test validates:
/// 1. The system writes 500 blocks without errors
/// 2. A subscriber receives all blocks via the streaming API
/// 3. Mid-stream reconnection from an arbitrary height works correctly
#[tokio::test]
async fn test_watch_blocks_high_volume_reconnect() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization_id =
        create_organization(leader.addr, "highvol-ns").await.expect("create organization");
    let vault = create_vault(leader.addr, organization_id).await.expect("create vault");

    // Subscribe from block 1
    let mut read_client = create_read_client(leader.addr).await.expect("create read client");
    let request = inferadb_ledger_proto::proto::WatchBlocksRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization_id as u64,
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault }),
        start_height: 1,
    };

    let mut stream =
        read_client.watch_blocks(request).await.expect("watch_blocks should succeed").into_inner();

    // Write 500 blocks — validates streaming and reconnection without approaching
    // the B+ tree page overflow threshold. Reuse a single client_id to avoid
    // bloating the AppliedState blob with unique client sequences.
    let total_writes: u64 = 500;
    let client_id = "highvol-writer";
    for i in 1..=total_writes {
        write_entity(
            leader.addr,
            organization_id,
            vault,
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
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault }),
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
