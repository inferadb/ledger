# inferadb-ledger-sdk

Production-grade Rust SDK for InferaDB Ledger.

## Overview

A high-level client library for interacting with InferaDB's Ledger blockchain database:

- **Type-safe API**: Strong typing for all operations
- **Automatic idempotency**: Client-side sequence tracking per vault
- **Resilient connectivity**: Exponential backoff retry and failover
- **Streaming**: `WatchBlocks` with automatic reconnection
- **Connection pooling**: Efficient channel management

## Installation

```toml
[dependencies]
inferadb-ledger-sdk = "0.1"
```

## Quick Start

```rust
use inferadb_ledger_sdk::{LedgerClient, ClientConfig, Operation};

#[tokio::main]
async fn main() -> inferadb_ledger_sdk::Result<()> {
    let config = ClientConfig::builder()
        .endpoints(vec!["http://localhost:50051".into()])
        .client_id("my-app")
        .build()?;

    let client = LedgerClient::new(config).await?;

    // Read an entity
    let entity = client.read(1, 0, "user:123").await?;

    // Write with automatic idempotency
    let ops = vec![
        Operation::set_entity("user:456", b"data".to_vec()),
        Operation::set_relationship("doc:1", "viewer", "user:456"),
    ];
    let result = client.write(1, 0, ops).await?;

    // Stream new blocks
    let mut stream = client.watch_blocks(1, 0, None).await?;
    while let Some(block) = stream.next().await {
        println!("Block {}: {} txs", block.height, block.tx_count);
    }

    Ok(())
}
```

## Architecture

```text
LedgerClient (public API)
    │
SequenceTracker (idempotency)
    │
Retry Layer (backoff)
    │
ConnectionPool
    │
gRPC (tonic)
```

## License

MIT OR Apache-2.0
