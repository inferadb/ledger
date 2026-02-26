//! Basic usage example demonstrating read/write operations.
//!
//! Run: `cargo run --example basic_usage -- --endpoint http://localhost:50051`
//!
//! This example shows:
//! - Client configuration and connection
//! - Creating a organization and vault
//! - Writing entities with automatic idempotency
//! - Reading values with different consistency levels
//! - Error handling patterns

// Examples are allowed to use expect/unwrap for brevity
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods)]

use inferadb_ledger_sdk::{ClientConfig, LedgerClient, Operation, Result, ServerSource};

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
    // 1. Create a client with configuration
    // -------------------------------------------------------------------------
    let config = ClientConfig::builder()
        .servers(ServerSource::from_static([endpoint]))
        .client_id("basic-usage-example")
        .timeout(std::time::Duration::from_secs(10))
        .compression(true)
        .build()?;

    let client = LedgerClient::new(config).await?;
    println!("Client connected with ID: {}", client.client_id());

    // -------------------------------------------------------------------------
    // 2. Create a organization and vault for our data
    // -------------------------------------------------------------------------
    let org = client.create_organization("example_organization").await?;
    let organization = org.slug;
    println!("Created organization with slug: {organization}");

    let vault_info = client.create_vault(organization).await?;
    let vault = vault_info.vault;
    println!("Created vault: {vault}");

    // -------------------------------------------------------------------------
    // 3. Write a single entity
    // -------------------------------------------------------------------------
    let user_key = "user:alice";
    let user_data = serde_json::json!({
        "name": "Alice",
        "email": "alice@example.com",
        "created_at": chrono::Utc::now().to_rfc3339()
    });

    let write_result = client
        .write(
            organization,
            Some(vault),
            vec![Operation::set_entity(
                user_key,
                serde_json::to_vec(&user_data).expect("serialize"),
            )],
        )
        .await?;

    println!(
        "Wrote entity '{}' at block {}, tx: {}",
        user_key, write_result.block_height, write_result.tx_id
    );

    // -------------------------------------------------------------------------
    // 4. Read the value back with eventual consistency (fast)
    // -------------------------------------------------------------------------
    let value = client.read(organization, Some(vault), user_key).await?;

    match value {
        Some(bytes) => {
            let parsed: serde_json::Value = serde_json::from_slice(&bytes).expect("deserialize");
            println!("Read (eventual): {}", serde_json::to_string_pretty(&parsed).expect("format"));
        },
        None => println!("Key not found (eventual read)"),
    }

    // -------------------------------------------------------------------------
    // 5. Read with linearizable consistency (strong, reads from leader)
    // -------------------------------------------------------------------------
    let value = client.read_consistent(organization, Some(vault), user_key).await?;

    match value {
        Some(bytes) => {
            let parsed: serde_json::Value = serde_json::from_slice(&bytes).expect("deserialize");
            println!("Read (linearizable): name = {}", parsed["name"]);
        },
        None => println!("Key not found (linearizable read)"),
    }

    // -------------------------------------------------------------------------
    // 6. Write multiple entities in a single transaction
    // -------------------------------------------------------------------------
    let operations = vec![
        Operation::set_entity("user:bob", b"Bob's data".to_vec()),
        Operation::set_entity("user:charlie", b"Charlie's data".to_vec()),
        Operation::create_relationship("doc:readme", "viewer", "user:alice"),
        Operation::create_relationship("doc:readme", "editor", "user:bob"),
    ];

    let result = client.write(organization, Some(vault), operations).await?;
    println!("Multi-entity write at block {}, tx: {}", result.block_height, result.tx_id);

    // -------------------------------------------------------------------------
    // 7. Batch read multiple keys
    // -------------------------------------------------------------------------
    let keys = vec!["user:alice", "user:bob", "user:charlie", "user:nonexistent"];
    let results = client.batch_read(organization, Some(vault), keys).await?;

    println!("Batch read results:");
    for (key, value) in results {
        match value {
            Some(v) => println!("  {}: {} bytes", key, v.len()),
            None => println!("  {}: not found", key),
        }
    }

    // -------------------------------------------------------------------------
    // 8. Demonstrate idempotency (same sequence = cached result)
    // -------------------------------------------------------------------------
    // Note: The SDK handles idempotency automatically via sequence tracking.
    // If a write is retried with the same sequence number, the server returns
    // the cached result (ALREADY_COMMITTED) rather than applying a duplicate.

    println!("\nExample completed successfully!");
    Ok(())
}
