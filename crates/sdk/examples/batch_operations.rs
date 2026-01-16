//! Batch operations example demonstrating atomic multi-operation writes.
//!
//! Run: `cargo run --example batch_operations -- --endpoint http://localhost:50051`
//!
//! This example shows:
//! - Batch writes with all-or-nothing atomicity
//! - Organizing operations into logical groups
//! - Conditional writes with SetCondition (CAS operations)
//! - Relationships and entity operations in a single transaction

// Examples are allowed to use expect/unwrap for brevity
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods)]

use ledger_sdk::{ClientConfig, LedgerClient, Operation, Result, SetCondition};

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
        .with_client_id("batch-operations-example")
        .build()?;

    let client = LedgerClient::new(config).await?;

    // -------------------------------------------------------------------------
    // 2. Create a namespace and vault
    // -------------------------------------------------------------------------
    let namespace_id = client.create_namespace("batch_example").await?;
    let vault_info = client.create_vault(namespace_id).await?;
    let vault_id = vault_info.vault_id;

    println!("Using namespace={namespace_id}, vault={vault_id}\n");

    // -------------------------------------------------------------------------
    // 3. Simple batch write - multiple operations, single transaction
    // -------------------------------------------------------------------------
    println!("=== Example 1: Simple Batch Write ===");

    let result = client
        .write(
            namespace_id,
            Some(vault_id),
            vec![
                Operation::set_entity("team:engineering", b"Engineering Team".to_vec()),
                Operation::set_entity("team:design", b"Design Team".to_vec()),
                Operation::set_entity("team:product", b"Product Team".to_vec()),
            ],
        )
        .await?;

    println!(
        "Created 3 teams atomically at block {}, tx: {}\n",
        result.block_height, result.tx_id
    );

    // -------------------------------------------------------------------------
    // 4. Batch write with logical groups
    // -------------------------------------------------------------------------
    println!("=== Example 2: Batch Write with Groups ===");

    // batch_write takes Vec<Vec<Operation>> - each inner Vec is a logical group
    // All groups are applied atomically in array order
    let result = client
        .batch_write(
            namespace_id,
            Some(vault_id),
            vec![
                // Group 1: Create user entities
                vec![
                    Operation::set_entity(
                        "user:alice",
                        serde_json::to_vec(&serde_json::json!({
                            "name": "Alice",
                            "role": "engineer"
                        }))
                        .expect("serialize"),
                    ),
                    Operation::set_entity(
                        "user:bob",
                        serde_json::to_vec(&serde_json::json!({
                            "name": "Bob",
                            "role": "designer"
                        }))
                        .expect("serialize"),
                    ),
                ],
                // Group 2: Assign users to teams
                vec![
                    Operation::create_relationship("team:engineering", "member", "user:alice"),
                    Operation::create_relationship("team:design", "member", "user:bob"),
                ],
                // Group 3: Grant document permissions
                vec![
                    Operation::create_relationship("doc:roadmap", "viewer", "team:engineering"),
                    Operation::create_relationship("doc:roadmap", "viewer", "team:design"),
                    Operation::create_relationship("doc:roadmap", "editor", "user:alice"),
                ],
            ],
        )
        .await?;

    println!(
        "Batch write (3 groups) committed at block {}\n",
        result.block_height
    );

    // -------------------------------------------------------------------------
    // 5. Conditional writes with SetCondition
    // -------------------------------------------------------------------------
    println!("=== Example 3: Conditional Writes (CAS) ===");

    // Create an entity that must not exist (CREATE IF NOT EXISTS)
    let result = client
        .write(
            namespace_id,
            Some(vault_id),
            vec![Operation::set_entity_if(
                "config:settings",
                b"default_settings".to_vec(),
                SetCondition::NotExists,
            )],
        )
        .await?;

    println!(
        "Created config:settings (NotExists condition) at block {}",
        result.block_height
    );

    // Update an entity that must exist (UPDATE IF EXISTS)
    let result = client
        .write(
            namespace_id,
            Some(vault_id),
            vec![Operation::set_entity_if(
                "config:settings",
                b"updated_settings".to_vec(),
                SetCondition::MustExist,
            )],
        )
        .await?;

    println!(
        "Updated config:settings (MustExist condition) at block {}\n",
        result.block_height
    );

    // -------------------------------------------------------------------------
    // 6. Atomic user provisioning workflow
    // -------------------------------------------------------------------------
    println!("=== Example 4: Atomic User Provisioning ===");

    // This is a common pattern: create a user and grant all their initial
    // permissions in a single atomic transaction
    let new_user_id = "user:charlie";
    let result = client
        .batch_write(
            namespace_id,
            Some(vault_id),
            vec![
                // Step 1: Create the user entity
                vec![Operation::set_entity(
                    new_user_id,
                    serde_json::to_vec(&serde_json::json!({
                        "name": "Charlie",
                        "role": "product_manager",
                        "onboarded_at": chrono::Utc::now().to_rfc3339()
                    }))
                    .expect("serialize"),
                )],
                // Step 2: Add to default team
                vec![Operation::create_relationship(
                    "team:product",
                    "member",
                    new_user_id,
                )],
                // Step 3: Grant standard permissions
                vec![
                    Operation::create_relationship("doc:roadmap", "viewer", new_user_id),
                    Operation::create_relationship("folder:product", "viewer", new_user_id),
                ],
            ],
        )
        .await?;

    println!(
        "Provisioned user '{}' with team membership and permissions at block {}",
        new_user_id, result.block_height
    );

    // -------------------------------------------------------------------------
    // 7. Verify data was written
    // -------------------------------------------------------------------------
    println!("\n=== Verification ===");

    let value = client
        .read(namespace_id, Some(vault_id), new_user_id)
        .await?;

    if let Some(bytes) = value {
        let user: serde_json::Value = serde_json::from_slice(&bytes).expect("deserialize");
        println!("User data: {}", serde_json::to_string_pretty(&user).expect("format"));
    }

    // -------------------------------------------------------------------------
    // Summary
    // -------------------------------------------------------------------------
    println!("\n=== Key Takeaways ===");
    println!("1. write() accepts Vec<Operation> for simple multi-operation transactions");
    println!("2. batch_write() accepts Vec<Vec<Operation>> for logically grouped operations");
    println!("3. All operations are atomic - either all succeed or none are applied");
    println!("4. SetCondition enables optimistic concurrency (NotExists, MustExist, Version)");
    println!("5. Each batch uses ONE sequence number for idempotency");

    println!("\nBatch operations example completed!");
    Ok(())
}
