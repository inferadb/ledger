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

The leader maintains persistent sequence state per client:

```rust
struct ClientState {
    last_committed_seq: u64,
    last_committed_tx_id: TxId,
    last_committed_block_height: u64,
    recent_responses: LruCache<u64, WriteResponse>,  // max 1000 entries
}
```

### Validation Rules

| Condition                       | Response                               | Rationale                |
| ------------------------------- | -------------------------------------- | ------------------------ |
| `seq <= last_committed_seq`     | Cached response or `ALREADY_COMMITTED` | Duplicate/retry          |
| `seq == last_committed_seq + 1` | Process transaction                    | Expected next            |
| `seq > last_committed_seq + 1`  | Reject with `SEQUENCE_GAP`             | Client bug or lost state |

### Duplicate Handling

When `seq <= last_committed_seq`:

1. If response is in LRU cache → return cached `WriteSuccess`
2. If response is evicted → return `WriteError` with:
   - `code = ALREADY_COMMITTED`
   - `committed_tx_id` from durable state
   - `committed_block_height` from durable state

The server durably stores the last committed transaction's identity, enabling recovery even after LRU eviction.

### Gap Handling

When `seq > last_committed_seq + 1`:

Return `WriteError` with:

- `code = SEQUENCE_GAP`
- `last_committed_sequence` = server's last committed sequence

Client should call `GetClientState` to recover, then resume from `last_committed_sequence + 1`.

## Retry Behavior

| Scenario             | Client Action                           | Server Response                              |
| -------------------- | --------------------------------------- | -------------------------------------------- |
| Network timeout      | Retry with same `(client_id, sequence)` | Cached `WriteSuccess` or `ALREADY_COMMITTED` |
| `SEQUENCE_GAP` error | Call `GetClientState`, resume sequence  | Reject until correct sequence                |
| Success              | Increment sequence for next write       | N/A                                          |

**Exactly-once semantics**: Retrying any previously-committed sequence returns the cached result (if in LRU) or `ALREADY_COMMITTED` with recovery data.

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
