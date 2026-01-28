# Quickstart

Get Ledger running in under 5 minutes.

## Prerequisites

- Rust 1.92+ (for building from source)
- OR Docker (for containerized deployment)
- grpcurl (for testing the API)

**Installing grpcurl:**

```bash
# macOS
brew install grpcurl

# Linux (via Go)
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# Or download from https://github.com/fullstorydev/grpcurl/releases
```

## Option 1: Docker (Fastest)

```bash
# Run single-node Ledger
docker run -d --name ledger \
  -p 50051:50051 \
  -p 9090:9090 \
  -e INFERADB__LEDGER__LISTEN=0.0.0.0:50051 \
  -e INFERADB__LEDGER__METRICS=0.0.0.0:9090 \
  -e INFERADB__LEDGER__CLUSTER=1 \
  inferadb/ledger:latest
```

## Option 2: Build from Source

```bash
# Clone and build
git clone https://github.com/inferadb/ledger
cd ledger
cargo build --release

# Run single-node
./target/release/inferadb-ledger --single --data /tmp/ledger
```

## Verify It's Running

```bash
# Health check
grpcurl -plaintext localhost:50051 ledger.v1.HealthService/Check
```

Expected output:

```json
{
  "status": "SERVING"
}
```

## First Operations

### Create a Namespace

```bash
grpcurl -plaintext \
  -d '{"name": "my_app"}' \
  localhost:50051 ledger.v1.AdminService/CreateNamespace
```

Response:

```json
{
  "namespaceId": { "id": "1" },
  "shardId": { "id": 1 }
}
```

### Create a Vault

```bash
grpcurl -plaintext \
  -d '{"namespace_id": {"id": "1"}}' \
  localhost:50051 ledger.v1.AdminService/CreateVault
```

Response:

```json
{
  "vaultId": { "id": "1" },
  "genesis": {
    "height": "0",
    "stateRoot": "..."
  }
}
```

### Write an Entity

```bash
grpcurl -plaintext \
  -d '{
    "namespace_id": {"id": "1"},
    "client_id": {"id": "quickstart"},
    "sequence": "1",
    "operations": [{
      "set_entity": {
        "key": "user:alice",
        "value": "eyJuYW1lIjogIkFsaWNlIn0="
      }
    }]
  }' \
  localhost:50051 ledger.v1.WriteService/Write
```

### Read It Back

```bash
grpcurl -plaintext \
  -d '{"namespace_id": {"id": "1"}, "key": "user:alice"}' \
  localhost:50051 ledger.v1.ReadService/Read
```

### Create a Relationship

```bash
grpcurl -plaintext \
  -d '{
    "namespace_id": {"id": "1"},
    "vault_id": {"id": "1"},
    "client_id": {"id": "quickstart"},
    "sequence": "2",
    "operations": [{
      "create_relationship": {
        "resource": "doc:readme",
        "relation": "viewer",
        "subject": "user:alice"
      }
    }]
  }' \
  localhost:50051 ledger.v1.WriteService/Write
```

### Query Relationships

```bash
# Who can view doc:readme?
grpcurl -plaintext \
  -d '{
    "namespace_id": {"id": "1"},
    "vault_id": {"id": "1"},
    "resource": "doc:readme",
    "relation": "viewer"
  }' \
  localhost:50051 ledger.v1.ReadService/ListRelationships
```

## Using the Rust SDK

Add to `Cargo.toml`:

```toml
[dependencies]
inferadb-ledger-sdk = "0.1"
tokio = { version = "1", features = ["full"] }
```

Basic usage:

```rust
use inferadb_ledger_sdk::{Client, ClientConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to Ledger
    let client = Client::connect(
        ClientConfig::builder()
            .endpoints(vec!["http://localhost:50051".into()])
            .build()
    ).await?;

    // Create namespace
    let ns = client.create_namespace("my_app").await?;
    println!("Created namespace: {}", ns.id);

    // Create vault
    let vault = client.create_vault(ns.id).await?;
    println!("Created vault: {}", vault.id);

    // Write entity
    client.write(ns.id, None)
        .set_entity("user:alice", b"Alice")
        .send()
        .await?;

    // Read it back
    let entity = client.read(ns.id, "user:alice").await?;
    println!("Read: {:?}", entity);

    Ok(())
}
```

## Next Steps

- [Client API](client/api.md) - Full API reference
- [Deployment Guide](operations/deployment.md) - Production setup
- [Architecture Overview](overview.md) - System design
