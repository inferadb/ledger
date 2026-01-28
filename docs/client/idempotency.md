# Idempotency & Retry Semantics

This document covers transaction identification, sequence tracking, and retry behavior.

## Transaction Identification

Every transaction includes:

```rust
struct Transaction {
    id: TxId,              // Globally unique, leader-assigned
    client_id: ClientId,   // Identifies submitting client
    sequence: u64,         // Strictly monotonic per-client
    operations: Vec<Operation>,
    timestamp: DateTime<Utc>,
    actor: String,         // Server-assigned from auth context
}
```

## Sequence Tracking

The leader maintains sequence state per client:

- **Persistent**: `last_committed_seq` (survives restarts via Raft snapshots)
- **In-memory cache**: Recent `WriteSuccess` responses (max 10,000 entries, 5-minute TTL)

### Validation Rules

| Condition                       | Response                            | Rationale                |
| ------------------------------- | ----------------------------------- | ------------------------ |
| `seq <= last_committed_seq`     | Cached `WriteSuccess` (if in cache) | Duplicate/retry          |
| `seq == last_committed_seq + 1` | Process transaction                 | Expected next            |
| `seq > last_committed_seq + 1`  | Reject with `SEQUENCE_GAP`          | Client bug or lost state |

### Duplicate Handling

When `seq <= last_committed_seq`:

1. If response is in cache (≤5 min old, ≤10,000 entries) → return cached `WriteSuccess`
2. If cache is evicted → request proceeds to Raft (state machine handles idempotency)

**Cache limitation**: After cache eviction, the server relies on the Raft state machine to handle duplicate operations idempotently. Clients should complete retries within the cache window (5 minutes) for best UX and guaranteed cached response.

The server durably stores only the sequence number, not transaction metadata.

### Gap Handling

When `seq > last_committed_seq + 1`:

Return `WriteError` with:

- `code = SEQUENCE_GAP`
- `last_committed_sequence` = server's last committed sequence

Client should call `GetClientState` to recover, then resume from `last_committed_sequence + 1`.

## Retry Behavior

| Scenario             | Client Action                           | Server Response                  |
| -------------------- | --------------------------------------- | -------------------------------- |
| Network timeout      | Retry with same `(client_id, sequence)` | Cached `WriteSuccess` (if fresh) |
| `SEQUENCE_GAP` error | Call `GetClientState`, resume sequence  | Reject until correct sequence    |
| Success              | Increment sequence for next write       | N/A                              |

**Exactly-once semantics**: Retrying within the cache window (5 minutes) returns the cached result. After cache eviction, the state machine ensures idempotent operation handling.

## Client Requirements

1. **Persist `last_used_sequence`** to durable storage before sending
2. **Increment by exactly 1** for each new transaction
3. **Reuse sequence number only for retries** of the same transaction
4. **On restart**, resume from persisted sequence

## Server Guarantees

1. Never accept `sequence <= last_committed_seq` as new transaction
2. Reject gaps (`sequence > last_committed_seq + 1`) to catch client bugs early
3. Persist `last_committed_seq` as part of vault state (survives leader failover)

## Client Recovery

If a client loses its sequence state:

```protobuf
rpc GetClientState(GetClientStateRequest) returns (GetClientStateResponse);
```

Resume from `last_committed_sequence + 1`.

### Scope-Based Tracking

Client state is tracked at two levels:

| Write Scope         | GetClientState Call                          | Use Case                         |
| ------------------- | -------------------------------------------- | -------------------------------- |
| Namespace entities  | `GetClientState(ns_id, client_id)`           | Control writing users, sessions  |
| Vault relationships | `GetClientState(ns_id, vault_id, client_id)` | App writing authorization tuples |

A single client can have separate sequence streams for each scope.

## Batch Sequencing

Batches use batch-level idempotency, not per-write sequencing:

| Constraint    | Requirement                       |
| ------------- | --------------------------------- |
| **Client ID** | Single `client_id` per batch      |
| **Sequence**  | Batch has single sequence number  |
| **Scope**     | All writes must target same scope |

**Sequencing behavior**:

```
batch.sequence == last_committed_seq + 1  // Valid
batch.sequence <= last_committed_seq      // ALREADY_COMMITTED
batch.sequence > last_committed_seq + 1   // SEQUENCE_GAP

On success:
  last_committed_seq = batch.sequence     // Single increment

On failure (CAS, sequence error):
  last_committed_seq unchanged            // No sequence consumed
```

Clients needing per-operation idempotency should use separate `WriteRequest` calls.

## Sequence Wrap-around

Sequence numbers are `u64`. At 1M tx/sec sustained, wrap-around takes ~584,942 years.

No handling required. If a sequence approaches `u64::MAX`, rotate to a new `client_id`.

## Actor Metadata

Every transaction includes a **server-assigned** `actor` field:

```rust
fn assign_actor(auth_context: &AuthContext) -> String {
    match auth_context {
        AuthContext::Session { user_id, .. } => format!("user:{}", user_id),
        AuthContext::ApiKey { client_id, .. } => format!("client:{}", client_id),
        AuthContext::System { component } => format!("system:{}", component),
    }
}
```

| Actor Format    | Example              | Derived From       |
| --------------- | -------------------- | ------------------ |
| `user:{id}`     | `user:789`           | Session token      |
| `client:{id}`   | `client:api_key_abc` | API key            |
| `system:{name}` | `system:gc`          | Internal operation |

**Security**: Clients cannot specify actor—it's always derived from the authenticated request context. This prevents impersonation attacks.
