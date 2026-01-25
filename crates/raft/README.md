# inferadb-ledger-raft

Raft consensus and gRPC services for InferaDB Ledger.

## Overview

This crate provides the distributed consensus layer:

- **OpenRaft integration**: Log storage backed by `inferadb-ledger-store`
- **gRPC services**: Read, Write, Admin, Health, Discovery
- **Multi-Raft**: Multiple Raft groups for shard isolation
- **Batching**: Transaction batching for throughput optimization
- **Idempotency**: Server-side deduplication cache

## Services

| Service            | Purpose                                 |
| ------------------ | --------------------------------------- |
| `ReadService`      | Entity/relationship queries, proofs     |
| `WriteService`     | Transactions with Raft replication      |
| `AdminService`     | Namespace/vault management, cluster ops |
| `HealthService`    | Liveness and readiness checks           |
| `DiscoveryService` | Peer discovery via DNS                  |

## Usage

```rust
use inferadb_ledger_raft::{LedgerServer, BatchConfig};
use inferadb_ledger_state::StateLayer;
use std::time::Duration;

// Configure batching with builder pattern
let batch_config = BatchConfig::builder()
    .max_batch_size(100)
    .max_batch_delay(Duration::from_millis(5))
    .build();

let state = StateLayer::new(engine);
let server = LedgerServer::new(state, raft_config).await?;

// Start gRPC server
server.serve("[::]:50051").await?;
```

## Architecture

```text
gRPC Services
    │
BatchWriter (coalesces writes)
    │
OpenRaft (consensus)
    │
RaftLogStore + StateLayer (storage)
```

## License

MIT OR Apache-2.0
