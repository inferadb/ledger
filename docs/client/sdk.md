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
use inferadb_ledger_sdk::{LedgerClient, ClientConfig, ServerSource, Operation};

#[tokio::main]
async fn main() -> inferadb_ledger_sdk::Result<()> {
    let config = ClientConfig::builder()
        .servers(ServerSource::from_static(["http://localhost:50051"]))
        .client_id("my-app")
        .build()?;

    let client = LedgerClient::new(config).await?;

    // Write an entity
    let ops = vec![Operation::set_entity("user:123", b"data".to_vec(), None, None)];
    let result = client.write(organization_slug, None, ops, None).await?;
    println!("Committed at block {}", result.block_height);

    // Read it back
    let value = client.read(organization_slug, None, "user:123", None, None).await?;
    println!("Value: {:?}", value);

    Ok(())
}
```

## Configuration

### Basic Configuration

```rust
use inferadb_ledger_sdk::{ClientConfig, RetryPolicy, ServerSource};
use std::time::Duration;

let config = ClientConfig::builder()
    .servers(ServerSource::from_static([
        "http://node1:50051",
        "http://node2:50051",
        "http://node3:50051",
    ]))
    .client_id("my-service-instance-1")
    .timeout(Duration::from_secs(30))           // Request timeout
    .connect_timeout(Duration::from_secs(5))    // Connection timeout
    .compression(true)                           // Enable gzip compression
    .retry_policy(RetryPolicy::default())        // Retry configuration
    .build()?;
```

### Configuration Options

| Option             | Default | Description                                     |
| ------------------ | ------- | ----------------------------------------------- |
| `servers`          | -       | `ServerSource` for cluster discovery (required) |
| `client_id`        | -       | Unique identifier for idempotency (required)    |
| `timeout`          | 30s     | Request timeout                                 |
| `connect_timeout`  | 5s      | Connection establishment timeout                |
| `compression`      | false   | Enable gzip compression                         |
| `retry_policy`     | default | Retry configuration for transient failures      |
| `tls`              | None    | TLS configuration for secure connections        |
| `trace`            | off     | Distributed tracing configuration               |
| `validation`       | default | Client-side input validation configuration      |
| `circuit_breaker`  | None    | Per-endpoint circuit breaker configuration      |
| `metrics`          | noop    | SDK-side metrics collector                      |
| `preferred_region` | None    | Preferred region for latency optimization       |

### Retry Policy

Configure automatic retry behavior for transient failures:

```rust
use inferadb_ledger_sdk::RetryPolicy;
use std::time::Duration;

let retry = RetryPolicy::builder()
    .max_attempts(5)                              // Maximum retry attempts
    .initial_backoff(Duration::from_millis(100))  // First retry delay
    .max_backoff(Duration::from_secs(10))         // Maximum backoff cap
    .multiplier(2.0)                              // Exponential multiplier
    .jitter(0.25)                                 // Random jitter factor
    .build();
```

**Default retry policy:**

| Parameter         | Default | Description                        |
| ----------------- | ------- | ---------------------------------- |
| `max_attempts`    | 3       | Total attempts including first try |
| `initial_backoff` | 100ms   | Delay before first retry           |
| `max_backoff`     | 10s     | Maximum delay between retries      |
| `multiplier`      | 2.0     | Backoff multiplier (exponential)   |
| `jitter`          | 0.25    | Random variance (25% by default)   |

Retryable errors: `UNAVAILABLE`, `INTERNAL`, `RESOURCE_EXHAUSTED`.

### TLS Configuration

For secure connections to production clusters:

```rust
use inferadb_ledger_sdk::{ClientConfig, TlsConfig, CertificateData, ServerSource};

// Option 1: Use system CA certificates
let tls = TlsConfig::with_native_roots()?;

// Option 2: Custom CA certificate (PEM bytes)
let ca_pem = std::fs::read("/path/to/ca.crt")?;
let tls = TlsConfig::builder()
    .ca_cert(CertificateData::Pem(ca_pem))
    .build()?;

// Option 3: Mutual TLS (client certificates)
let ca_pem = std::fs::read("/path/to/ca.crt")?;
let client_pem = std::fs::read("/path/to/client.crt")?;
let client_key = std::fs::read("/path/to/client.key")?;
let tls = TlsConfig::builder()
    .ca_cert(CertificateData::Pem(ca_pem))
    .client_cert(CertificateData::Pem(client_pem))
    .client_key(client_key)
    .build()?;

let config = ClientConfig::builder()
    .servers(ServerSource::from_static(["https://ledger.example.com:50051"]))
    .client_id("secure-client")
    .tls(tls)
    .build()?;
```

### Distributed Tracing

Enable W3C Trace Context propagation for end-to-end distributed tracing:

```rust
use inferadb_ledger_sdk::{ClientConfig, TraceConfig, ServerSource};

let config = ClientConfig::builder()
    .servers(ServerSource::from_static(["http://localhost:50051"]))
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
    let value = client.read(organization_slug, None, "key").await?;
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

// Or use connect() for single-endpoint convenience
let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
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
// Read from organization (entity) with eventual consistency
let value = client.read(organization_slug, None, "user:123", None, None).await?;

// Read from vault (relationship context)
let value = client.read(organization_slug, Some(vault_slug), "key", None, None).await?;

// Read with linearizable consistency (always from leader)
let value = client.read(
    organization_slug, None, "user:123",
    Some(ReadConsistency::Linearizable), None,
).await?;

// Read with a cancellation token
let token = tokio_util::sync::CancellationToken::new();
let value = client.read(organization_slug, None, "key", None, Some(token)).await?;
```

### Batch Read

Read multiple keys efficiently:

```rust
let keys = vec!["user:1".into(), "user:2".into(), "user:3".into()];
let results = client.batch_read(organization_slug, None, keys, None, None).await?;

for (key, value) in results {
    match value {
        Some(data) => println!("{}: {} bytes", key, data.len()),
        None => println!("{}: not found", key),
    }
}

// Batch read with linearizable consistency
let results = client.batch_read(
    organization_slug, None, keys,
    Some(ReadConsistency::Linearizable), None,
).await?;
```

### Verified Read

Read with cryptographic proof:

```rust
use inferadb_ledger_sdk::VerifyOpts;

let verified = client.verified_read(
    organization_slug,
    vault_slug,
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

let page = client.list_relationships(organization_slug, vault_slug, opts).await?;
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

let page = client.list_resources(organization_slug, vault_slug, opts).await?;
```

## Write Operations

### Building Operations

```rust
use inferadb_ledger_sdk::{Operation, SetCondition};

// Entity operations
let set = Operation::set_entity("user:123", b"data".to_vec(), None, None);
let set_with_ttl = Operation::set_entity(
    "session:abc",
    b"token".to_vec(),
    Some(3600), // expires in 1 hour
    None,
);
let delete = Operation::delete_entity("user:old");

// Conditional writes
let create_only = Operation::set_entity(
    "user:new",
    b"data".to_vec(),
    None,
    Some(SetCondition::NotExists),
);
let update_only = Operation::set_entity(
    "user:123",
    b"updated".to_vec(),
    None,
    Some(SetCondition::MustExist),
);
let cas = Operation::set_entity(
    "counter",
    b"11".to_vec(),
    None,
    Some(SetCondition::version(10)), // Only if current version is 10
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
    Operation::set_entity("user:123", b"updated".to_vec(), None, None),
    Operation::create_relationship("doc:1", "owner", "user:123"),
];

let result = client.write(organization_slug, Some(vault_slug), ops, None).await?;
println!("Transaction ID: {}", result.tx_id);
println!("Block height: {}", result.block_height);
```

### Batch Write

Atomic multi-operation transactions:

```rust
let ops = vec![
    // Create user with unique email constraint
    Operation::set_entity(
        "idx:email:alice@example.com",
        b"user:123".to_vec(),
        None,
        Some(SetCondition::NotExists),
    ),
    Operation::set_entity("user:123", user_data, None, None),
    // Grant permissions
    Operation::create_relationship("org:acme", "member", "user:123"),
];

let result = client.batch_write(organization_slug, Some(vault_slug), ops, None).await?;
```

If any conditional operation fails, the entire batch is rejected atomically.

## Streaming

### Watch Blocks

Subscribe to new blocks in real-time:

```rust
use futures::StreamExt;

let mut stream = client.watch_blocks(organization_slug, vault_slug, start_height).await?;

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
let vault_info = client.get_vault(organization_slug, vault_slug).await?;
let stream = client.watch_blocks(
    organization_slug,
    vault_slug,
    vault_info.height + 1,
).await?;
```

## Admin Operations

### Organization Management

```rust
use inferadb_ledger_types::Region;

// Create organization (requires a region for data residency)
let ns = client.create_organization("my_app", Region::US_EAST_VA).await?;
println!("Organization ID: {}", ns.id);

// Get organization info
let info = client.get_organization(ns.id).await?;
println!("Status: {:?}", info.status);

// List organizations
let organizations = client.list_organizations(None).await?;
```

### Vault Management

```rust
// Create vault (returns Snowflake slug)
let vault = client.create_vault(organization_slug).await?;
println!("Vault slug: {}", vault.vault_slug);

// Get vault info
let info = client.get_vault(organization_slug, vault.vault_slug).await?;
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
let vault_health = client.health_check_vault(organization_slug, vault_slug).await?;
```

## Token Operations

### User Sessions

```rust
use inferadb_ledger_sdk::token::{TokenPair, ValidatedToken};

// Create a user session (access + refresh token pair)
let pair: TokenPair = client.create_user_session(user_slug).await?;
println!("Access token: {}", pair.access_token);
println!("Expires at: {:?}", pair.access_expires_at);

// Validate an access token and get parsed claims
let claims: ValidatedToken = client.validate_token(&pair.access_token).await?;
match claims {
    ValidatedToken::UserSession { user, role } => {
        println!("User: {}, Role: {}", user, role);
    }
    ValidatedToken::VaultAccess { app, vault, scopes, .. } => {
        println!("App: {}, Vault: {}, Scopes: {:?}", app, vault, scopes);
    }
}

// Refresh a token pair (rotate-on-use: old refresh token is consumed)
let new_pair = client.refresh_token(&pair.refresh_token).await?;

// Revoke a single refresh token and its family
client.revoke_token(&pair.refresh_token).await?;

// Revoke all sessions for a user (increments token version)
let revoked_count = client.revoke_all_user_sessions(user_slug).await?;
```

### Vault Tokens

```rust
// Create a vault access token for an app
let vault_pair = client.create_vault_token(org_slug, app_slug, vault_slug).await?;
```

### Signing Key Management

```rust
use inferadb_ledger_sdk::token::PublicKeyInfo;

// Create a new signing key for a scope
let kid = client.create_signing_key(scope).await?;

// Rotate a signing key (creates replacement, marks old as rotated)
let new_kid = client.rotate_signing_key(&kid).await?;

// Revoke a signing key immediately
client.revoke_signing_key(&kid).await?;

// Get active public keys for token verification (JWKS-style)
let keys: Vec<PublicKeyInfo> = client.get_public_keys().await?;
for key in &keys {
    println!("kid: {}, status: {}", key.kid, key.status);
}
```

> **Note:** Refresh tokens use rotate-on-use semantics. Each refresh token can be used at most once — reuse triggers family poisoning (theft detection), revoking all tokens in that family.

## Organization Operations

```rust
// Create organization
let org = client.create_organization("my_app", Region::US_EAST_VA).await?;

// Get, update, delete
let info = client.get_organization(org_slug).await?;
client.update_organization(org_slug, Some("new_name"), None).await?;
client.delete_organization(org_slug).await?;

// List with pagination
let orgs = client.list_organizations(None).await?;

// Member management
let members = client.list_organization_members(org_slug, None).await?;
client.remove_organization_member(org_slug, user_slug).await?;
client.update_organization_member_role(org_slug, user_slug, role).await?;

// Team management
let teams = client.list_organization_teams(org_slug).await?;
client.create_organization_team(org_slug, "engineering").await?;
client.delete_organization_team(org_slug, team_slug).await?;

// Region migration
client.migrate_organization(org_slug, target_region).await?;
```

## User Operations

```rust
// Create, get, update, delete users
let user = client.create_user("alice@example.com", "Alice", None).await?;
let info = client.get_user(user_slug).await?;
client.update_user(user_slug, Some("Alice Smith"), None).await?;
client.delete_user(user_slug).await?;

// List and search
let users = client.list_users(None).await?;
let matches = client.search_users("alice@example.com").await?;

// Email management
client.create_user_email(user_slug, "alice@work.com").await?;
client.delete_user_email(user_slug, email_id).await?;
let email = client.verify_user_email(verification_token).await?;
let results = client.search_user_email("alice@").await?;

// Region migration and erasure
client.migrate_user_region(user_slug, target_region).await?;
client.erase_user(user_slug).await?;
```

## App Operations

```rust
// Create and manage apps
let app = client.create_app(org_slug, "my-api").await?;
let info = client.get_app(org_slug, app_slug).await?;
let apps = client.list_apps(org_slug).await?;
client.update_app(org_slug, app_slug, Some("new-name")).await?;
client.delete_app(org_slug, app_slug).await?;

// Enable/disable
client.enable_app(org_slug, app_slug).await?;
client.disable_app(org_slug, app_slug).await?;

// Credentials
client.set_app_credential_enabled(org_slug, app_slug, true).await?;
let secret = client.get_app_client_secret(org_slug, app_slug).await?;
client.rotate_app_client_secret(org_slug, app_slug).await?;

// Client assertions (mTLS/JWT client auth)
let assertions = client.list_app_client_assertions(org_slug, app_slug).await?;
client.create_app_client_assertion(org_slug, app_slug, assertion).await?;
client.delete_app_client_assertion(org_slug, app_slug, assertion_id).await?;

// Vault connections
let vaults = client.list_app_vaults(org_slug, app_slug).await?;
client.add_app_vault(org_slug, app_slug, vault_slug).await?;
client.update_app_vault(org_slug, app_slug, vault_slug, scopes).await?;
client.remove_app_vault(org_slug, app_slug, vault_slug).await?;
```

## Events Operations

```rust
// List events with filtering
let events = client.list_events(org_slug, filter).await?;
let next_page = client.list_events_next(page_token).await?;

// Get a single event
let event = client.get_event(org_slug, event_id).await?;

// Count events matching a filter
let count = client.count_events(org_slug, filter).await?;

// Ingest custom events
client.ingest_events(org_slug, events).await?;
```

## Error Handling

### Error Types

```rust
use inferadb_ledger_sdk::{SdkError, Result};

async fn handle_errors(client: &LedgerClient) -> Result<()> {
    match client.read(1, None, "key").await {
        Ok(value) => println!("Got: {:?}", value),

        // Connection errors (retryable)
        Err(SdkError::Connection { message }) => {
            eprintln!("Connection error: {}", message);
        }

        // Transport-level errors (HTTP/2, TLS)
        Err(SdkError::Transport { source }) => {
            eprintln!("Transport error: {}", source);
        }

        // gRPC RPC errors with correlation IDs
        Err(SdkError::Rpc { code, message, request_id, trace_id, .. }) => {
            eprintln!("RPC error {:?}: {}", code, message);
        }

        // Rate limited with retry-after hint
        Err(SdkError::RateLimited { message, retry_after, .. }) => {
            eprintln!("Rate limited: {}, retry after {:?}", message, retry_after);
        }

        // Client shutdown
        Err(SdkError::Shutdown) => {
            eprintln!("Client is shut down");
        }

        // Configuration error
        Err(SdkError::Config { message }) => {
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
use tonic::Code;

match client.write(ns, vault, ops, None).await {
    Ok(result) => {
        println!("Committed: {}", result.tx_id);
    }

    // Idempotency conflict (key reused with different payload)
    Err(SdkError::Idempotency { message, conflict_key, original_tx_id }) => {
        eprintln!("Idempotency error: {}", message);
    }

    // Already committed (idempotent retry detected -- not an error)
    Err(SdkError::AlreadyCommitted { tx_id, block_height }) => {
        println!("Already committed: tx {} at block {}", tx_id, block_height);
    }

    // Conditional write failures come back as Rpc with FailedPrecondition
    Err(SdkError::Rpc { code: Code::FailedPrecondition, message, .. }) => {
        eprintln!("Condition failed: {}", message);
    }

    // Retries exhausted
    Err(SdkError::RetryExhausted { attempts, last_error, .. }) => {
        eprintln!("Failed after {} attempts: {}", attempts, last_error);
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
use inferadb_ledger_sdk::{LedgerClient, ClientConfig, ServerSource, FileSequenceStorage};

// Use file-based sequence storage
let storage = FileSequenceStorage::new("/var/lib/myapp/sequences")?;

let config = ClientConfig::builder()
    .servers(ServerSource::from_static(["http://localhost:50051"]))
    .client_id("my-app")
    .build()?;

let client = LedgerClient::with_sequence_storage(config, storage).await?;
```

### Manual Sequence Management

For advanced use cases:

```rust
// Get current sequence state
let tracker = client.sequences();
let seq = tracker.get(organization_slug, vault_slug).await;

// Recover from server if state is lost
let state = client.get_client_state(organization_slug, vault_slug).await?;
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
use tonic::Code;

match result {
    // Transient: SDK will retry automatically
    Err(SdkError::Connection { .. }) => { /* wait and retry */ }
    Err(SdkError::Transport { .. }) => { /* wait and retry */ }
    Err(SdkError::Rpc { code: Code::Unavailable, .. }) => { /* wait */ }
    Err(SdkError::RateLimited { retry_after, .. }) => { /* wait retry_after */ }
    Err(SdkError::Unavailable { .. }) => { /* wait */ }

    // Permanent: don't retry
    Err(SdkError::Rpc { code: Code::NotFound, .. }) => { /* handle */ }
    Err(SdkError::Rpc { code: Code::FailedPrecondition, .. }) => { /* handle */ }
    Err(SdkError::Validation { .. }) => { /* fix request */ }

    // Logic error: fix the code
    Err(SdkError::Config { .. }) => { /* fix configuration */ }

    _ => {}
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
