# Audit Event Protocol

Centralized audit architecture and shared event protocol for all InferaDB services.

## Architecture Rationale

Ledger is the central audit event store for all InferaDB services (Ledger, Engine, Control). Engine and Control are stateless — they have no persistent storage of their own. Ledger is their exclusive database for business data (relationships, entities, user accounts, sessions). Introducing a separate audit store would mean adding a second persistence layer to otherwise stateless services, doubling operational complexity.

Instead, Ledger provides an `IngestEvents` RPC that Engine and Control call to write their audit events into the same `Events` B+ tree table. The dashboard queries one API — `EventsService.ListEvents` — filtered by `event_type` prefix to isolate service-specific events.

## Event Envelope Format

Every event uses the `EventEntry` envelope with required and optional fields.

### Required Fields

| Field             | Type            | Description                                              |
| ----------------- | --------------- | -------------------------------------------------------- |
| `event_id`        | UUID (16 bytes) | Deterministic UUID v5 (apply-phase) or random v4         |
| `source_service`  | string          | Originating service: `"ledger"`, `"engine"`, `"control"` |
| `event_type`      | string          | Hierarchical dot-separated type                          |
| `timestamp`       | DateTime\<Utc\> | Block timestamp (apply) or wall clock (handler)          |
| `scope`           | enum            | `System` or `Organization`                               |
| `action`          | string          | Snake_case action name (e.g., `vault_created`)           |
| `emission`        | enum            | `ApplyPhase` or `HandlerPhase { node_id }`               |
| `principal`       | string          | Server-assigned actor identity                           |
| `organization_id` | i64             | Owning org (0 for system events)                         |
| `outcome`         | enum            | `Success`, `Failed`, or `Denied`                         |
| `expires_at`      | u64             | Unix timestamp for TTL (0 = no expiry)                   |

### Optional Fields

| Field               | Type                       | Description                                     |
| ------------------- | -------------------------- | ----------------------------------------------- |
| `organization_slug` | u64                        | External Snowflake slug for API responses       |
| `vault_slug`        | u64                        | Vault context (when applicable)                 |
| `details`           | BTreeMap\<String, String\> | Action-specific key-value context (bounded)     |
| `block_height`      | u64                        | Blockchain block reference (committed writes)   |
| `trace_id`          | string                     | W3C Trace Context correlation                   |
| `correlation_id`    | string                     | Business-level multi-step operation correlation |
| `operations_count`  | u32                        | Number of operations (write actions)            |
| `node_id`           | u64                        | Node that generated handler-phase events        |

## Event Type Hierarchy

All services use dot-separated hierarchical event types prefixed by service name:

```
{service}.{domain}.{action}
```

Examples:

- `ledger.vault.created` — Ledger creates a vault
- `engine.authorization.checked` — Engine evaluates a permission check
- `control.member.invited` — Control invites a user to an organization

Filter by service prefix (`event_type_prefix: "engine"`) in the unified events table. No fan-out needed — one `ListEvents` query covers all services.

## Three Durability Tiers

| Tier                         | Source                          | Replication                            | Durability                                   |
| ---------------------------- | ------------------------------- | -------------------------------------- | -------------------------------------------- |
| **Apply-phase**              | Ledger state machine            | Replicated via determinism + snapshots | Identical on all replicas, survives failover |
| **Handler-phase (internal)** | Ledger gRPC handlers            | Not replicated                         | Node-local, lost on failover                 |
| **Handler-phase (external)** | Engine/Control via IngestEvents | Not replicated                         | Node-local, lost on failover                 |

All three tiers write to the same `events.db` file on each node. Apply-phase events are identical across replicas by construction. The bottom two tiers are node-local by nature.

## Three Correlation Fields

1. **`trace_id`** (W3C Trace Context) — "What happened during this API call across all services?"
2. **`organization_id`** — "What happened in this tenant?"
3. **`principal`** (actor) — "What did this user or service account do?"

## IngestEvents Integration Guide

### Endpoint

```protobuf
rpc IngestEvents(IngestEventsRequest) returns (IngestEventsResponse);
```

### Request Format

```protobuf
message IngestEventsRequest {
  string source_service = 1;           // validated against allow-list
  OrganizationSlug organization = 2;   // target organization
  repeated IngestEventEntry entries = 3; // max batch size enforced
}
```

### Batching Recommendations

- Buffer events in memory and flush every 1–5 seconds
- Target 50–200 events per batch (one RPC with 100 events, not 100 RPCs)
- Maximum batch size: 500 events (configurable via `ingestion.max_ingest_batch_size`)

### Rate Limits

- Per-source rate limit: 10,000 events/second (configurable via `ingestion.ingest_rate_limit_per_source`)
- Source service must be in the allow-list (default: `["engine", "control"]`)

### Error Handling

The response includes per-event rejection details:

```protobuf
message IngestEventsResponse {
  uint32 accepted_count = 1;
  uint32 rejected_count = 2;
  repeated RejectedEvent rejections = 3;
}
```

Check `rejections` for individual event failures. Partial success is possible.

### SDK Usage

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
];
let result = client.ingest_events(12345, "engine", events).await?;
# Ok(())
# }
```

## Service Responsibility Boundaries

| Service     | Event Domain                                | Event Type Prefix |
| ----------- | ------------------------------------------- | ----------------- |
| **Ledger**  | Data mutations, storage denials, system ops | `ledger.*`        |
| **Engine**  | Authorization decisions                     | `engine.*`        |
| **Control** | Management operations                       | `control.*`       |

### Known Edge Case

Ledger's `DeleteUserSaga` removes membership entities directly (bypassing Control). Membership removal events during user deletion are emitted as Ledger system events (`ledger.user.deleted`), not as org-scoped Control events.

## Dashboard Aggregation Pattern

Query all events from one API:

```rust,no_run
# use inferadb_ledger_sdk::{LedgerClient, EventFilter};
# async fn example(client: &LedgerClient) -> Result<(), Box<dyn std::error::Error>> {
// All Engine events in an org
let filter = EventFilter::new().event_type_prefix("engine");
let page = client.list_events(12345, filter, 100).await?;

// All denial events across services
let filter = EventFilter::new().outcome_denied();
let page = client.list_events(12345, filter, 100).await?;
# Ok(())
# }
```

## Volume Management

1. **Client-side batching**: Engine buffers decisions and flushes to `IngestEvents` every 1–5 seconds
2. **Sampling**: Log all denials (security-critical), sample allows at a configurable rate (e.g., 10%)
3. **No Raft overhead**: `IngestEvents` writes directly to local Events B+ tree — no consensus

## Serialization

- **Wire format** (gRPC): Protobuf (`EventEntry`, `IngestEventEntry` messages)
- **Internal storage**: postcard binary encoding in B+ tree values
- **SDK**: Rust-native domain types (`SdkEventEntry`, `SdkIngestEventEntry`)

## Authorization Decision Replay

Engine logs authorization decisions with Ledger's `block_height` as a consistency token. To answer "why was this permission allowed?":

1. Engine records `block_height` with the authorization event
2. Replay the permission check against Ledger's immutable state at that block height via `GetBlock`
3. The blockchain makes replay cheap and accurate — no need to log full decision traces on every check
