//! Streaming example demonstrating WatchBlocks subscription.
//!
//! Run: `cargo run --example streaming -- --endpoint http://localhost:50051`
//!
//! This example shows:
//! - Subscribing to block announcements with watch_blocks
//! - Processing streaming data with async iteration
//! - Automatic reconnection on disconnect
//! - Graceful stream termination

// Examples are allowed to use expect/unwrap for brevity
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods)]

use std::time::Duration;

use futures::StreamExt;
use inferadb_ledger_sdk::{ClientConfig, LedgerClient, Operation, Result};
use tokio::time::timeout;

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let endpoint = args
        .iter()
        .position(|a| a == "--endpoint")
        .and_then(|i| args.get(i + 1))
        .map(String::as_str)
        .unwrap_or("http://localhost:50051");

    println!("Connecting to Ledger at {endpoint}");

    // -------------------------------------------------------------------------
    // 1. Create the client
    // -------------------------------------------------------------------------
    let config = ClientConfig::builder()
        .with_endpoint(endpoint)
        .with_client_id("streaming-example")
        .build()?;

    let client = LedgerClient::new(config).await?;

    // -------------------------------------------------------------------------
    // 2. Create a namespace and vault to watch
    // -------------------------------------------------------------------------
    let namespace_id = client.create_namespace("streaming_example").await?;
    println!("Created namespace: {namespace_id}");

    let vault_info = client.create_vault(namespace_id).await?;
    let vault_id = vault_info.vault_id;
    println!("Created vault: {vault_id}");

    // -------------------------------------------------------------------------
    // 3. Start watching blocks from height 1 (genesis)
    // -------------------------------------------------------------------------
    println!("\nSubscribing to block announcements (watching for 10 seconds)...");
    println!("Writing some data to generate blocks...\n");

    // Clone client for the writer task
    let writer_client = client.clone();
    let writer_ns = namespace_id;
    let writer_vault = vault_id;

    // Spawn a task to write data periodically (generates blocks)
    let writer_handle = tokio::spawn(async move {
        for i in 1..=5 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let key = format!("stream-test:{i}");
            let value = format!("Value written at iteration {i}");

            match writer_client
                .write(
                    writer_ns,
                    Some(writer_vault),
                    vec![Operation::set_entity(&key, value.into_bytes())],
                )
                .await
            {
                Ok(result) => println!("[Writer] Wrote '{}' at block {}", key, result.block_height),
                Err(e) => println!("[Writer] Error: {}", e),
            }
        }
    });

    // -------------------------------------------------------------------------
    // 4. Subscribe to block stream and process announcements
    // -------------------------------------------------------------------------
    let mut stream = client.watch_blocks(namespace_id, vault_id, 1).await?;

    // Process blocks for 10 seconds
    let result = timeout(Duration::from_secs(10), async {
        let mut block_count = 0;
        while let Some(announcement) = stream.next().await {
            match announcement {
                Ok(block) => {
                    block_count += 1;
                    println!(
                        "[Stream] Block {} at height {} (state root: {:?})",
                        block_count,
                        block.height,
                        hex_preview(&block.state_root)
                    );

                    // Print transaction count if available
                    println!("         block_hash: {:?}", hex_preview(&block.block_hash));
                },
                Err(e) => {
                    println!("[Stream] Error: {}", e);
                    // The stream will attempt to reconnect automatically
                },
            }
        }
        block_count
    })
    .await;

    match result {
        Ok(count) => println!("\nStream ended after receiving {count} blocks"),
        Err(_) => println!("\nTimeout reached (10 seconds)"),
    }

    // Wait for writer to finish
    let _ = writer_handle.await;

    // -------------------------------------------------------------------------
    // 5. Demonstrate reconnection behavior
    // -------------------------------------------------------------------------
    println!("\n--- Reconnection Notes ---");
    println!("The WatchBlocks stream automatically reconnects on disconnect:");
    println!("  - Uses exponential backoff between attempts");
    println!("  - Resumes from last_seen_height + 1 (no gaps or duplicates)");
    println!("  - Continues until max retry attempts exhausted");
    println!("  - Stream errors are yielded to caller for custom handling");

    println!("\nStreaming example completed!");
    Ok(())
}

/// Helper to show first few bytes of a hash as hex
fn hex_preview(bytes: &[u8]) -> String {
    use std::fmt::Write;
    if bytes.is_empty() {
        return "empty".to_string();
    }
    let preview = bytes.iter().take(4).fold(String::with_capacity(8), |mut acc, b| {
        let _ = write!(acc, "{b:02x}");
        acc
    });
    format!("{preview}...")
}
