//! Verified read example demonstrating Merkle proof verification.
//!
//! Run: `cargo run --example verified_read -- --endpoint http://localhost:50051`
//!
//! This example shows:
//! - Reading values with cryptographic proofs
//! - Client-side Merkle proof verification
//! - Trustless operation without relying on server honesty
//! - Chain proof verification for historical reads

// Examples are allowed to use expect/unwrap for brevity
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods)]

use inferadb_ledger_sdk::{ClientConfig, LedgerClient, Operation, Result, VerifyOpts};

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
        .with_client_id("verified-read-example")
        .build()?;

    let client = LedgerClient::new(config).await?;

    // -------------------------------------------------------------------------
    // 2. Create a namespace and vault with some data
    // -------------------------------------------------------------------------
    let namespace_id = client.create_namespace("verified_example").await?;
    let vault_info = client.create_vault(namespace_id).await?;
    let vault_id = vault_info.vault_id;

    println!("Using namespace={namespace_id}, vault={vault_id}\n");

    // Write some test data
    let test_key = "verified:test";
    let test_value = b"This value can be cryptographically verified".to_vec();

    let write_result = client
        .write(
            namespace_id,
            Some(vault_id),
            vec![Operation::set_entity(test_key, test_value.clone())],
        )
        .await?;

    println!(
        "Wrote test data at block {}, tx: {}\n",
        write_result.block_height, write_result.tx_id
    );

    // -------------------------------------------------------------------------
    // 3. Read with Merkle proof (basic verification)
    // -------------------------------------------------------------------------
    println!("=== Example 1: Basic Verified Read ===");

    let result =
        client.verified_read(namespace_id, Some(vault_id), test_key, VerifyOpts::new()).await?;

    match result {
        Some(verified) => {
            if let Some(ref value) = verified.value {
                println!("Value: {:?}", String::from_utf8_lossy(value));
            }
            println!("Block height: {}", verified.block_height);

            // Verify the Merkle proof client-side
            match verified.verify() {
                Ok(true) => println!("✓ Merkle proof verified successfully!"),
                Ok(false) => println!("✗ Merkle proof verification failed"),
                Err(e) => println!("✗ Verification error: {}", e),
            }

            println!("\nProof details:");
            println!("  Leaf hash: {}", hex_string(&verified.merkle_proof.leaf_hash));
            println!("  Sibling count: {}", verified.merkle_proof.siblings.len());
            println!("  State root in header: {}", hex_string(&verified.block_header.state_root));
        },
        None => println!("Key not found"),
    }

    // -------------------------------------------------------------------------
    // 4. Read at a specific block height
    // -------------------------------------------------------------------------
    println!("\n=== Example 2: Read at Specific Height ===");

    // Write another value to advance the block height
    let result2 = client
        .write(
            namespace_id,
            Some(vault_id),
            vec![Operation::set_entity(test_key, b"Updated value".to_vec())],
        )
        .await?;

    println!("Updated value at block {}\n", result2.block_height);

    // Read at the ORIGINAL block height (before the update)
    let opts = VerifyOpts::new().at_height(write_result.block_height);
    let historical = client.verified_read(namespace_id, Some(vault_id), test_key, opts).await?;

    match historical {
        Some(verified) => {
            let value_str = verified
                .value
                .as_ref()
                .map(|v| String::from_utf8_lossy(v).to_string())
                .unwrap_or_else(|| "none".to_string());
            println!("Historical value (at block {}): {:?}", verified.block_height, value_str);

            // Verify this historical proof
            match verified.verify() {
                Ok(true) => println!("✓ Historical proof verified!"),
                Ok(false) => println!("✗ Historical proof verification failed"),
                Err(e) => println!("✗ Verification error: {}", e),
            }
        },
        None => println!("Historical key not found at that height"),
    }

    // Read at the CURRENT block height (after the update)
    let current =
        client.verified_read(namespace_id, Some(vault_id), test_key, VerifyOpts::new()).await?;

    if let Some(verified) = current {
        let value_str = verified
            .value
            .as_ref()
            .map(|v| String::from_utf8_lossy(v).to_string())
            .unwrap_or_else(|| "none".to_string());
        println!("Current value (at block {}): {:?}", verified.block_height, value_str);
    }

    // -------------------------------------------------------------------------
    // 5. Understanding verification
    // -------------------------------------------------------------------------
    println!("\n=== How Verification Works ===");
    println!(
        "
1. The server returns:
   - The entity value
   - A block header containing the state_root
   - A Merkle proof (path from leaf to root)

2. Client-side verification:
   - Hash the key+value to get leaf_hash
   - Walk the proof siblings, combining hashes
   - Compare computed root with state_root in header

3. If roots match, the value is authentic:
   - The value existed at that block height
   - The value hasn't been tampered with
   - You don't need to trust the server

4. Optional chain proof:
   - Links block headers from trusted_height to current
   - Verifies blockchain continuity
   - Useful for light clients with checkpoint trust
"
    );

    // -------------------------------------------------------------------------
    // Summary
    // -------------------------------------------------------------------------
    println!("=== Key Takeaways ===");
    println!("• verified_read() returns value + cryptographic proof");
    println!("• Call .verify() to check proof client-side (no server trust needed)");
    println!("• Use .at_height() to read historical values with proofs");
    println!("• Merkle proofs guarantee value authenticity at specific block height");

    println!("\nVerified read example completed!");
    Ok(())
}

/// Convert bytes to hex string for display
fn hex_string(bytes: &[u8]) -> String {
    use std::fmt::Write;
    if bytes.is_empty() {
        return "empty".to_string();
    }
    bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut acc, b| {
        let _ = write!(acc, "{b:02x}");
        acc
    })
}
