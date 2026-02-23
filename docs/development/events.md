# Events SDK Guide

Developer reference for integrating with InferaDB Ledger's audit event system.

## SDK Client Methods

### Listing Events

Query events for an organization with filtering and pagination.

```rust,no_run
# use inferadb_ledger_sdk::{LedgerClient, EventFilter};
# async fn example(client: &LedgerClient) -> Result<(), Box<dyn std::error::Error>> {
let filter = EventFilter::new()
    .event_type_prefix("ledger.vault")
    .outcome_success();

let page = client.list_events(12345, filter, 100).await?;
for event in &page.entries {
    println!("{}: {} by {}", event.event_type, event.action, event.principal);
}

// Paginate through remaining results
if page.has_next_page() {
    let token = page.next_page_token.as_deref().unwrap_or_default();
    let next = client.list_events_next(12345, token).await?;
}
# Ok(())
# }
```

Pass `org_slug = 0` for system-scope events (node membership, config changes, migrations).

### Getting a Single Event

Retrieve an event by UUID string.

```rust,no_run
# use inferadb_ledger_sdk::LedgerClient;
# async fn example(client: &LedgerClient) -> Result<(), Box<dyn std::error::Error>> {
let event = client.get_event(12345, "550e8400-e29b-41d4-a716-446655440000").await?;
println!("Event: {} at {}", event.event_type, event.timestamp);

// Access the raw UUID bytes
let hex_id = event.event_id_string();
# Ok(())
# }
```

The SDK validates the UUID format client-side before making the RPC.

### Counting Events

Get a count of events matching a filter without fetching entries.

```rust,no_run
# use inferadb_ledger_sdk::{LedgerClient, EventFilter};
# async fn example(client: &LedgerClient) -> Result<(), Box<dyn std::error::Error>> {
let filter = EventFilter::new().outcome_denied();
let denied_count = client.count_events(12345, filter).await?;
println!("Denied requests: {denied_count}");
# Ok(())
# }
```

### Ingesting External Events

Engine and Control write their events via `ingest_events`.

```rust,no_run
# use inferadb_ledger_sdk::{LedgerClient, SdkIngestEventEntry, EventOutcome};
# async fn example(client: &LedgerClient) -> Result<(), Box<dyn std::error::Error>> {
let events = vec![
    SdkIngestEventEntry::new(
        "engine.authorization.checked",
        "user:alice",
        EventOutcome::Success,
    )
    .detail("resource", "document:123")
    .detail("relation", "viewer"),

    SdkIngestEventEntry::new(
        "engine.authorization.checked",
        "user:bob",
        EventOutcome::Denied { reason: "no relation".into() },
    )
    .detail("resource", "document:456")
    .detail("relation", "editor")
    .vault_slug(98765),
];

let result = client.ingest_events(12345, "engine", events).await?;
println!("Accepted: {}, Rejected: {}", result.accepted_count, result.rejected_count);

for rejection in &result.rejections {
    eprintln!("Event {} rejected: {}", rejection.index, rejection.reason);
}
# Ok(())
# }
```

## EventFilter Reference

`EventFilter` uses a builder pattern. All filters are AND'd; multiple `actions()` entries are OR'd.

| Method                                        | Description                               |
| --------------------------------------------- | ----------------------------------------- |
| `start_time(DateTime<Utc>)`                   | Events from this time forward (inclusive) |
| `end_time(DateTime<Utc>)`                     | Events before this time (exclusive)       |
| `actions(["vault_created", "vault_deleted"])` | Match any of these actions                |
| `event_type_prefix("ledger.vault")`           | Match event types starting with prefix    |
| `principal("user:alice")`                     | Events by this actor                      |
| `outcome_success()`                           | Successful operations only                |
| `outcome_failed()`                            | Failed operations only                    |
| `outcome_denied()`                            | Denied requests only                      |
| `emission_apply_phase()`                      | Replicated events only                    |
| `emission_handler_phase()`                    | Node-local events only                    |
| `correlation_id("saga-123")`                  | Events in a multi-step operation          |

### Combining Filters

```rust,no_run
# use inferadb_ledger_sdk::EventFilter;
# use chrono::Utc;
let filter = EventFilter::new()
    .start_time(Utc::now() - chrono::Duration::hours(24))
    .event_type_prefix("ledger")
    .outcome_denied()
    .emission_handler_phase();
```

## Query Patterns

### Time-Range Queries

```rust,no_run
# use inferadb_ledger_sdk::{LedgerClient, EventFilter};
# use chrono::{Utc, Duration};
# async fn example(client: &LedgerClient) -> Result<(), Box<dyn std::error::Error>> {
let last_hour = EventFilter::new()
    .start_time(Utc::now() - Duration::hours(1))
    .end_time(Utc::now());
let page = client.list_events(12345, last_hour, 500).await?;
# Ok(())
# }
```

### Cross-Service Queries

Filter by service using the event type prefix. All services write to the same store.

```rust,no_run
# use inferadb_ledger_sdk::{LedgerClient, EventFilter};
# async fn example(client: &LedgerClient) -> Result<(), Box<dyn std::error::Error>> {
// All Engine authorization decisions
let engine_events = EventFilter::new().event_type_prefix("engine");
let page = client.list_events(12345, engine_events, 100).await?;

// All Control management operations
let control_events = EventFilter::new().event_type_prefix("control");
let page = client.list_events(12345, control_events, 100).await?;

// All Ledger events (default, no prefix needed but explicit is fine)
let ledger_events = EventFilter::new().event_type_prefix("ledger");
let page = client.list_events(12345, ledger_events, 100).await?;
# Ok(())
# }
```

### Principal Audit Trail

```rust,no_run
# use inferadb_ledger_sdk::{LedgerClient, EventFilter};
# async fn example(client: &LedgerClient) -> Result<(), Box<dyn std::error::Error>> {
let user_trail = EventFilter::new().principal("user:alice");
let page = client.list_events(12345, user_trail, 100).await?;
for event in &page.entries {
    println!("{}: {} ({})", event.timestamp, event.action, event.event_type);
}
# Ok(())
# }
```

### Denial Investigation

```rust,no_run
# use inferadb_ledger_sdk::{LedgerClient, EventFilter};
# async fn example(client: &LedgerClient) -> Result<(), Box<dyn std::error::Error>> {
let denials = EventFilter::new().outcome_denied();
let page = client.list_events(12345, denials, 100).await?;
for event in &page.entries {
    println!(
        "{}: {} - {}",
        event.timestamp,
        event.action,
        event.details.get("reason").unwrap_or(&String::new()),
    );
}
# Ok(())
# }
```

### Pagination Loop

```rust,no_run
# use inferadb_ledger_sdk::{LedgerClient, EventFilter, EventPage};
# async fn example(client: &LedgerClient) -> Result<(), Box<dyn std::error::Error>> {
let filter = EventFilter::new().event_type_prefix("ledger.write");

let mut page = client.list_events(12345, filter, 500).await?;
let mut total = page.entries.len();

while page.has_next_page() {
    let token = page.next_page_token.as_deref().unwrap_or_default();
    page = client.list_events_next(12345, token).await?;
    total += page.entries.len();
}

println!("Total events: {total}");
# Ok(())
# }
```

## Blockchain Drill-Down

Committed write events include a `block_height` reference. Use it to retrieve the full cryptographic details from the blockchain.

```rust,no_run
# use inferadb_ledger_sdk::{LedgerClient, EventFilter, OrganizationSlug};
# async fn example(client: &LedgerClient) -> Result<(), Box<dyn std::error::Error>> {
# let org = OrganizationSlug::new(12345);
# let vault_slug = 98765u64;
let filter = EventFilter::new()
    .actions(["write_committed"])
    .emission_apply_phase();

let page = client.list_events(12345, filter, 10).await?;

for event in &page.entries {
    if let Some(height) = event.block_height {
        // Drill down to full block with transaction data and Merkle proof
        let block = client.get_block(org, Some(vault_slug), height).await?;
        println!(
            "Block {}: {} transactions, root {}",
            block.header.height,
            block.transactions.len(),
            hex::encode(&block.header.state_root),
        );
    }
}
# Ok(())
# }
```

The blockchain provides:

- Full transaction payloads (every operation in the write batch)
- Merkle proof for each transaction (tamper evidence)
- State root at that block height (verifiable snapshot)
- Previous block hash (immutable chain)

## Ingestion Guide

### Batching

Buffer events in memory and flush periodically. One RPC with 100 events, not 100 RPCs with one event.

```rust,no_run
# use inferadb_ledger_sdk::{LedgerClient, SdkIngestEventEntry, EventOutcome};
# use std::sync::Mutex;
# async fn example(client: &LedgerClient) -> Result<(), Box<dyn std::error::Error>> {
# let buffer: Mutex<Vec<SdkIngestEventEntry>> = Mutex::new(Vec::new());
// Accumulate events during request handling
buffer.lock().unwrap().push(
    SdkIngestEventEntry::new("engine.authorization.checked", "user:alice", EventOutcome::Success)
);

// Flush every 1-5 seconds or when buffer reaches threshold
let batch: Vec<SdkIngestEventEntry> = {
    let mut buf = buffer.lock().unwrap();
    std::mem::take(&mut *buf)
};

if !batch.is_empty() {
    let result = client.ingest_events(12345, "engine", batch).await?;
    if result.rejected_count > 0 {
        eprintln!("{} events rejected", result.rejected_count);
    }
}
# Ok(())
# }
```

### Sizing Guidance

| Parameter      | Recommendation               |
| -------------- | ---------------------------- |
| Batch size     | 50–200 events per flush      |
| Flush interval | 1–5 seconds                  |
| Max batch      | 500 events (server enforced) |
| Rate limit     | 10,000 events/sec per source |

### Sampling

Log all denials (security-critical). Sample successful operations at a configurable rate.

```rust,no_run
# use inferadb_ledger_sdk::{SdkIngestEventEntry, EventOutcome};
# fn should_sample(outcome: &EventOutcome, rate: f64) -> bool {
match outcome {
    EventOutcome::Denied { .. } | EventOutcome::Failed { .. } => true,  // always log
    EventOutcome::Success => rand::random::<f64>() < rate,               // sample
}
# }
```

### Error Handling

`IngestEvents` supports partial success. Check `rejections` for per-event failures.

```rust,no_run
# use inferadb_ledger_sdk::{LedgerClient, SdkIngestEventEntry, EventOutcome};
# async fn example(client: &LedgerClient) -> Result<(), Box<dyn std::error::Error>> {
# let events: Vec<SdkIngestEventEntry> = vec![];
let result = client.ingest_events(12345, "engine", events).await?;

if result.rejected_count > 0 {
    for r in &result.rejections {
        tracing::warn!(index = r.index, reason = %r.reason, "event rejected");
    }
    // Optionally retry rejected events with corrections
}
# Ok(())
# }
```

## Integration Patterns

### Compliance Export

Export events for external audit systems.

```rust,no_run
# use inferadb_ledger_sdk::{LedgerClient, EventFilter};
# use chrono::{Utc, Duration};
# async fn example(client: &LedgerClient) -> Result<(), Box<dyn std::error::Error>> {
let filter = EventFilter::new()
    .start_time(Utc::now() - Duration::days(30))
    .emission_apply_phase();  // replicated events only for consistency

let mut page = client.list_events(12345, filter, 1000).await?;
let mut all_events = page.entries.clone();

while page.has_next_page() {
    let token = page.next_page_token.as_deref().unwrap_or_default();
    page = client.list_events_next(12345, token).await?;
    all_events.extend(page.entries.clone());
}

// Serialize to your audit format
for event in &all_events {
    println!(
        "{},{},{},{},{}",
        event.timestamp, event.event_type, event.principal,
        event.action, event.event_id_string(),
    );
}
# Ok(())
# }
```

For compliance exports, filter to `emission_apply_phase()` to get only replicated events that are consistent across all nodes.

### Cross-Service Correlation

Use `trace_id` to correlate events across Ledger, Engine, and Control.

```rust,no_run
# use inferadb_ledger_sdk::{LedgerClient, EventFilter, SdkIngestEventEntry, EventOutcome};
# async fn example(client: &LedgerClient) -> Result<(), Box<dyn std::error::Error>> {
# let trace_id = "abc123".to_string();
// Engine logs an authorization decision with the current trace ID
let event = SdkIngestEventEntry::new(
    "engine.authorization.checked",
    "user:alice",
    EventOutcome::Success,
)
.trace_id(&trace_id);

// Later, query all events in this trace
let filter = EventFilter::new().correlation_id(&trace_id);
let page = client.list_events(12345, filter, 100).await?;
# Ok(())
# }
```

## Adding New Event Types

### Step 1: Define the Event Action

Add a variant to `EventAction` in `crates/types/src/events.rs`:

```rust,no_run
# enum EventAction {
// ... existing variants ...
/// Emitted when a vault is archived.
VaultArchived,
# }
```

### Step 2: Implement Required Methods

Each variant needs three method entries:

```rust,no_run
# struct EventAction;
# enum EventScope { Organization }
# impl EventAction {
# fn scope(&self) -> EventScope { EventScope::Organization }
# fn action_str(&self) -> &'static str { "vault_archived" }
# fn event_type(&self) -> &'static str { "ledger.vault.archived" }
# }
```

- **`scope()`** — `System` or `Organization` (compile-time enforced, no dual-writing)
- **`action_str()`** — snake_case action name for filtering
- **`event_type()`** — hierarchical `{service}.{domain}.{action}` for the catalog

### Step 3: Emit the Event

In the apply handler or gRPC service handler:

```rust,no_run
# struct EventWriter;
# struct EventHandle;
# impl EventWriter {
# fn write(&self, _: EventHandle) {}
# }
# fn example(event_writer: &EventWriter) {
// Apply-phase (deterministic, replicated)
// event_writer.write(EventHandle::new(...));

// Handler-phase (node-local)
// event_writer.write(EventHandle::new_handler(...));
# }
```

### Step 4: Update Documentation

Add the new event to the catalog table in `docs/operations/events.md`.

### Event Type Naming Convention

```
{service}.{domain}.{action}
```

- **service**: `ledger`, `engine`, or `control`
- **domain**: resource noun (`vault`, `organization`, `write`, `user`, `node`)
- **action**: past-tense verb (`created`, `deleted`, `committed`, `checked`)

Examples: `ledger.vault.created`, `engine.authorization.checked`, `control.member.invited`
