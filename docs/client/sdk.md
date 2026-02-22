[Documentation](../README.md) > Client API > Rust SDK

# Rust SDK

Production-grade Rust client for InferaDB Ledger.

## Installation

```toml
[dependencies]
inferadb-ledger-sdk = "0.1"
tokio = { version = "1", features = ["full"] }
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

    // Write an entity
    let ops = vec![Operation::set_entity("user:123", b"data".to_vec())];
    let result = client.write(1, None, ops).await?;
    println!("Committed at block {}", result.block_height);

    // Read it back
    let value = client.read(1, None, "user:123").await?;
    println!("Value: {:?}", value);

    Ok(())
}
```

## Configuration

### Basic Configuration

```rust
use inferadb_ledger_sdk::{ClientConfig, RetryPolicy};
use std::time::Duration;

let config = ClientConfig::builder()
    .endpoints(vec![
        "http://node1:50051".into(),
        "http://node2:50051".into(),
        "http://node3:50051".into(),
    ])
    .client_id("my-service-instance-1")
    .timeout(Duration::from_secs(30))           // Request timeout
    .connect_timeout(Duration::from_secs(5))    // Connection timeout
    .compression(true)                           // Enable gzip compression
    .retry_policy(RetryPolicy::default())        // Retry configuration
    .build()?;
```

### Configuration Options

| Option            | Default | Description                                    |
| ----------------- | ------- | ---------------------------------------------- |
| `endpoints`       | -       | Server URLs (required, at least one)           |
| `client_id`       | -       | Unique identifier for idempotency (required)   |
| `timeout`         | 30s     | Request timeout                                |
| `connect_timeout` | 5s      | Connection establishment timeout               |
| `compression`     | false   | Enable gzip compression                        |
| `retry_policy`    | default | Retry configuration for transient failures     |
| `tls`             | None    | TLS configuration for secure connections       |
| `trace`           | off     | Distributed tracing configuration              |

### Retry Policy

Configure automatic retry behavior for transient failures:

```rust
use inferadb_ledger_sdk::RetryPolicy;
use std::time::Duration;

let retry = RetryPolicy::builder()
    .max_attempts(5)                              // Maximum retry attempts
    .initial_backoff(Duration::from_millis(100))  // First retry delay
    .max_backoff(Duration::from_secs(5))          // Maximum backoff cap
    .multiplier(2.0)                              // Exponential multiplier
    .jitter(0.1)                                  // Random jitter factor
    .build();
```

**Default retry policy:**

| Parameter         | Default | Description                            |
| ----------------- | ------- | -------------------------------------- |
| `max_attempts`    | 3       | Total attempts including first try     |
| `initial_backoff` | 100ms   | Delay before first retry               |
| `max_backoff`     | 5s      | Maximum delay between retries          |
| `multiplier`      | 2.0     | Backoff multiplier (exponential)       |
| `jitter`          | 0.1     | Random variance (10% by default)       |

Retryable errors: `UNAVAILABLE`, `INTERNAL`, `RESOURCE_EXHAUSTED`.

### TLS Configuration

For secure connections to production clusters:

```rust
use inferadb_ledger_sdk::{ClientConfig, TlsConfig};

// Option 1: Use system CA certificates
let tls = TlsConfig::with_native_roots()?;

// Option 2: Custom CA certificate
let tls = TlsConfig::builder()
    .ca_cert_path("/path/to/ca.crt")
    .build()?;

// Option 3: Mutual TLS (client certificates)
let tls = TlsConfig::builder()
    .ca_cert_path("/path/to/ca.crt")
    .client_cert_path("/path/to/client.crt")
    .client_key_path("/path/to/client.key")
    .build()?;

let config = ClientConfig::builder()
    .endpoints(vec!["https://ledger.example.com:50051".into()])
    .client_id("secure-client")
    .tls(tls)
    .build()?;
```

### Distributed Tracing

Enable W3C Trace Context propagation for end-to-end distributed tracing:

```rust
use inferadb_ledger_sdk::{ClientConfig, TraceConfig};

let config = ClientConfig::builder()
    .endpoints(vec!["http://localhost:50051".into()])
    .client_id("traced-service")
    .trace(TraceConfig::enabled())
    .build()?;
```

When tracing is enabled:

1. The SDK extracts trace context from the current OpenTelemetry span
2. Injects `traceparent` and `tracestate` headers into all gRPC requests
3. If no span exists, generates a new trace context

**Integration with OpenTelemetry:**

```rust
use opentelemetry::global;
use tracing_subscriber::prelude::*;

// Initialize OpenTelemetry (example with OTLP exporter)
let tracer = opentelemetry_otlp::new_pipeline()
    .tracing()
    .with_exporter(opentelemetry_otlp::new_exporter().tonic())
    .install_batch(opentelemetry_sdk::runtime::Tokio)?;

// Set up tracing subscriber
tracing_subscriber::registry()
    .with(tracing_opentelemetry::layer().with_tracer(tracer))
    .init();

// Now SDK calls within instrumented spans propagate trace context
#[tracing::instrument]
async fn process_request(client: &LedgerClient) -> Result<()> {
    // This read will include traceparent header linking to current span
    let value = client.read(1, None, "key").await?;
    Ok(())
}
```

**Viewing traces:**

With Jaeger or similar backends, you'll see connected spans:

```
my-service::process_request
  └── inferadb-ledger::ReadService/Read
        └── raft::apply_read
```

## Client Lifecycle

### Creating a Client

```rust
// Async initialization (connects to cluster)
let client = LedgerClient::new(config).await?;

// Or use connect() for explicit connection
let client = LedgerClient::connect(config).await?;
```

### Graceful Shutdown

```rust
// Shutdown cancels pending requests and flushes state
client.shutdown().await;

// Check if client is shut down
if client.is_shutdown() {
    // Handle shutdown state
}
```

After shutdown:
- Pending requests receive `SdkError::Shutdown`
- New requests immediately fail
- Sequence tracker state is persisted (if using file storage)
- Connections are closed

## Read Operations

### Single Read

```rust
// Read from organization (entity)
let value = client.read(organization_slug, None, "user:123").await?;

// Read from vault (relationship context)
let value = client.read(organization_slug, Some(vault_id), "key").await?;
```

### Consistent Read

For linearizable reads (always from leader):

```rust
let value = client.read_consistent(organization_slug, None, "user:123").await?;
```

### Batch Read

Read multiple keys efficiently:

```rust
let keys = vec!["user:1".into(), "user:2".into(), "user:3".into()];
let results = client.batch_read(organization_slug, None, keys).await?;

for (key, value) in results {
    match value {
        Some(data) => println!("{}: {} bytes", key, data.len()),
        None => println!("{}: not found", key),
    }
}
```

### Verified Read

Read with cryptographic proof:

```rust
use inferadb_ledger_sdk::VerifyOpts;

let verified = client.verified_read(
    organization_slug,
    vault_id,
    "user:123",
    VerifyOpts::default(),
).await?;

println!("Value: {:?}", verified.value);
println!("Block height: {}", verified.block_height);
println!("State root: {:?}", verified.state_root);
println!("Proof valid: {}", verified.proof.verify(&verified.state_root));
```

### List Operations

**List entities:**

```rust
use inferadb_ledger_sdk::ListEntitiesOpts;

let opts = ListEntitiesOpts::builder()
    .prefix("user:")
    .limit(100)
    .build();

let page = client.list_entities(organization_slug, opts).await?;
for entity in page.items {
    println!("{}: {} bytes", entity.key, entity.value.len());
}

// Paginate through results
if let Some(token) = page.next_token {
    let next_opts = ListEntitiesOpts::builder()
        .prefix("user:")
        .page_token(token)
        .build();
    let next_page = client.list_entities(organization_slug, next_opts).await?;
}
```

**List relationships:**

```rust
use inferadb_ledger_sdk::ListRelationshipsOpts;

let opts = ListRelationshipsOpts::builder()
    .resource("doc:readme")
    .relation("viewer")
    .limit(100)
    .build();

let page = client.list_relationships(organization_slug, vault_id, opts).await?;
for rel in page.items {
    println!("{} is {} of {}", rel.subject, rel.relation, rel.resource);
}
```

**List resources by subject:**

```rust
use inferadb_ledger_sdk::{ListResourcesOpts, Direction};

let opts = ListResourcesOpts::builder()
    .subject("user:alice")
    .direction(Direction::Outgoing)  // Resources alice can access
    .limit(100)
    .build();

let page = client.list_resources(organization_slug, vault_id, opts).await?;
```

## Write Operations

### Building Operations

```rust
use inferadb_ledger_sdk::{Operation, SetCondition};

// Entity operations
let set = Operation::set_entity("user:123", b"data".to_vec());
let set_with_ttl = Operation::set_entity_with_ttl(
    "session:abc",
    b"token".to_vec(),
    3600, // expires in 1 hour
);
let delete = Operation::delete_entity("user:old");

// Conditional writes
let create_only = Operation::set_entity_if(
    "user:new",
    b"data".to_vec(),
    SetCondition::NotExists,
);
let update_only = Operation::set_entity_if(
    "user:123",
    b"updated".to_vec(),
    SetCondition::MustExist,
);
let cas = Operation::set_entity_if(
    "counter",
    b"11".to_vec(),
    SetCondition::version(10), // Only if current version is 10
);

// Relationship operations
let create_rel = Operation::create_relationship(
    "doc:readme",
    "viewer",
    "user:alice",
);
let delete_rel = Operation::delete_relationship(
    "doc:readme",
    "viewer",
    "user:alice",
);
```

### Single Write

```rust
let ops = vec![
    Operation::set_entity("user:123", b"updated".to_vec()),
    Operation::create_relationship("doc:1", "owner", "user:123"),
];

let result = client.write(organization_slug, Some(vault_id), ops).await?;
println!("Transaction ID: {}", result.tx_id);
println!("Block height: {}", result.block_height);
```

### Batch Write

Atomic multi-operation transactions:

```rust
let ops = vec![
    // Create user with unique email constraint
    Operation::set_entity_if(
        "idx:email:alice@example.com",
        b"user:123".to_vec(),
        SetCondition::NotExists,
    ),
    Operation::set_entity("user:123", user_data),
    // Grant permissions
    Operation::create_relationship("org:acme", "member", "user:123"),
];

let result = client.batch_write(organization_slug, Some(vault_id), ops).await?;
```

If any conditional operation fails, the entire batch is rejected atomically.

## Streaming

### Watch Blocks

Subscribe to new blocks in real-time:

```rust
use futures::StreamExt;

let mut stream = client.watch_blocks(organization_slug, vault_id, start_height).await?;

while let Some(result) = stream.next().await {
    match result {
        Ok(block) => {
            println!("Block {}: {} transactions", block.height, block.tx_count);
            println!("State root: {:?}", block.state_root);
        }
        Err(e) => {
            eprintln!("Stream error: {}", e);
            // Stream automatically reconnects on transient errors
        }
    }
}
```

**Automatic reconnection:**

The SDK handles stream reconnection automatically:
- Tracks last received block height
- Reconnects on network errors
- Resumes from last height + 1
- No duplicate blocks delivered

**Starting from current tip:**

```rust
// Get current tip, then subscribe from next block
let vault_info = client.get_vault(organization_slug, vault_id).await?;
let stream = client.watch_blocks(
    organization_slug,
    vault_id,
    vault_info.height + 1,
).await?;
```

## Admin Operations

### Organization Management

```rust
// Create organization
let ns = client.create_organization("my_app").await?;
println!("Organization ID: {}", ns.id);

// Get organization info
let info = client.get_organization(ns.id).await?;
println!("Status: {:?}", info.status);

// List organizations
let organizations = client.list_organizations(None).await?;
```

### Vault Management

```rust
// Create vault
let vault = client.create_vault(organization_slug).await?;
println!("Vault ID: {}", vault.id);

// Get vault info
let info = client.get_vault(organization_slug, vault.id).await?;
println!("Height: {}, State root: {:?}", info.height, info.state_root);

// List vaults
let vaults = client.list_vaults(organization_slug, None).await?;
```

### Health Checks

```rust
use inferadb_ledger_sdk::HealthStatus;

// Basic health check
let status = client.health_check().await?;
assert_eq!(status, HealthStatus::Healthy);

// Detailed health check
let result = client.health_check_detailed().await?;
println!("Status: {:?}", result.status);
println!("Leader: {}", result.is_leader);

// Vault-specific health
let vault_health = client.health_check_vault(organization_slug, vault_id).await?;
```

## Error Handling

### Error Types

```rust
use inferadb_ledger_sdk::{SdkError, Result};

async fn handle_errors(client: &LedgerClient) -> Result<()> {
    match client.read(1, None, "key").await {
        Ok(value) => println!("Got: {:?}", value),

        // Network/transport errors
        Err(SdkError::Transport { message, .. }) => {
            eprintln!("Network error: {}", message);
        }

        // Server returned an error
        Err(SdkError::Server { code, message, .. }) => {
            eprintln!("Server error {}: {}", code, message);
        }

        // Client shutdown
        Err(SdkError::Shutdown) => {
            eprintln!("Client is shut down");
        }

        // Configuration error
        Err(SdkError::Config { message, .. }) => {
            eprintln!("Config error: {}", message);
        }

        // Other errors
        Err(e) => eprintln!("Error: {}", e),
    }
    Ok(())
}
```

### Handling Write Errors

```rust
use inferadb_ledger_sdk::SdkError;

match client.write(ns, vault, ops).await {
    Ok(result) => {
        println!("Committed: {}", result.tx_id);
    }

    // Conditional write failed
    Err(SdkError::ConditionFailed { key, code, .. }) => {
        match code.as_str() {
            "KEY_EXISTS" => println!("{} already exists", key),
            "KEY_NOT_FOUND" => println!("{} doesn't exist", key),
            "VERSION_MISMATCH" => println!("{} was modified", key),
            _ => println!("Condition failed: {}", code),
        }
    }

    // Sequence gap (client state out of sync)
    Err(SdkError::SequenceGap { expected, got, .. }) => {
        eprintln!("Sequence gap: expected {}, got {}", expected, got);
        // SDK handles this automatically on retry
    }

    Err(e) => return Err(e),
}
```

### Retry Patterns

The SDK automatically retries transient errors. For custom retry logic:

```rust
use inferadb_ledger_sdk::with_retry;

let result = with_retry(&retry_policy, || async {
    client.read(ns, None, "key").await
}).await?;
```

## Idempotency

The SDK provides automatic client-side idempotency tracking.

### How It Works

1. Each write gets a monotonically increasing sequence number
2. Server rejects duplicates with same `(client_id, sequence)`
3. SDK handles sequence tracking transparently
4. Retried writes return cached server response

### Sequence Persistence

For crash recovery, persist sequence state:

```rust
use inferadb_ledger_sdk::{LedgerClient, ClientConfig, FileSequenceStorage};

// Use file-based sequence storage
let storage = FileSequenceStorage::new("/var/lib/myapp/sequences")?;

let config = ClientConfig::builder()
    .endpoints(vec!["http://localhost:50051".into()])
    .client_id("my-app")
    .build()?;

let client = LedgerClient::with_sequence_storage(config, storage).await?;
```

### Manual Sequence Management

For advanced use cases:

```rust
// Get current sequence state
let tracker = client.sequences();
let seq = tracker.get(organization_slug, vault_id).await;

// Recover from server if state is lost
let state = client.get_client_state(organization_slug, vault_id).await?;
println!("Last committed: {}", state.last_committed_sequence);
```

## Best Practices

### Client ID Strategy

Use unique, stable client IDs:

```rust
// Good: includes instance identifier
.client_id("payment-service-pod-abc123")

// Good: includes environment
.client_id("order-processor-prod-us-east-1a")

// Bad: generic, may conflict
.client_id("my-app")

// Bad: random per request (breaks idempotency)
.client_id(uuid::Uuid::new_v4().to_string())
```

### Connection Management

Reuse clients across requests:

```rust
// Good: shared client
lazy_static! {
    static ref CLIENT: LedgerClient = {
        let config = ClientConfig::builder()...build().unwrap();
        LedgerClient::new(config).block_on().unwrap()
    };
}

// Bad: new client per request
async fn handle_request() {
    let client = LedgerClient::new(config).await?; // Expensive!
}
```

### Error Handling

Distinguish transient vs permanent failures:

```rust
match result {
    // Transient: SDK will retry automatically
    Err(SdkError::Transport { .. }) => { /* wait and retry */ }
    Err(SdkError::Server { code: "UNAVAILABLE", .. }) => { /* wait */ }

    // Permanent: don't retry
    Err(SdkError::Server { code: "NOT_FOUND", .. }) => { /* handle */ }
    Err(SdkError::ConditionFailed { .. }) => { /* handle */ }

    // Logic error: fix the code
    Err(SdkError::Config { .. }) => { /* bug */ }
}
```

### Batch Operations

Prefer batch operations for bulk work:

```rust
// Good: single round-trip
let results = client.batch_read(ns, None, keys).await?;

// Bad: N round-trips
for key in keys {
    let result = client.read(ns, None, key).await?;
}
```

## See Also

- [API Reference](api.md) - Full gRPC API documentation
- [Error Reference](errors.md) - Detailed error codes
- [Idempotency](idempotency.md) - Sequence tracking details
- [Health Checks](health.md) - Health monitoring
