[Documentation](../README.md) > Client API > Idempotency

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

- **Persistent**: `ClientSequenceEntry` in the replicated `ClientSequences` B+ tree table (survives restarts and leader failover)
- **In-memory cache**: Moka TinyLFU cache of recent `WriteSuccess` responses (max 100,000 entries, 24-hour TTL)

### `ClientSequenceEntry` Fields

| Field                  | Type      | Description                                     |
| ---------------------- | --------- | ----------------------------------------------- |
| `sequence`             | `u64`     | Last committed sequence number                  |
| `last_seen`            | `i64`     | Deterministic apply-phase timestamp              |
| `last_idempotency_key` | `[u8;16]` | UUID v4 from the last write                    |
| `last_request_hash`    | `u64`     | Seahash of the last write's operation payload   |

### Validation Rules

| Condition                       | Response                            | Rationale                |
| ------------------------------- | ----------------------------------- | ------------------------ |
| `seq <= last_committed_seq`     | Cached `WriteSuccess` (if in cache) | Duplicate/retry          |
| `seq == last_committed_seq + 1` | Process transaction                 | Expected next            |
| `seq > last_committed_seq + 1`  | Reject with `SEQUENCE_GAP`          | Client bug or lost state |

### Two-Tier Duplicate Detection

When `seq <= last_committed_seq`, the server checks two tiers:

1. **Moka cache hit** (fast path): Returns the full cached `WriteSuccess` including `committed_tx_id`, block header, and transaction proof. Sub-microsecond lookup.

2. **Moka cache miss, replicated state hit** (cross-failover path): Checks the persistent `ClientSequenceEntry`. If the `last_idempotency_key` matches and `last_request_hash` matches, returns `WRITE_ERROR_CODE_ALREADY_COMMITTED` with `assigned_sequence` from the replicated entry. The `committed_tx_id` is not available in this path — only the moka cache stores the full response.

3. **Both miss** (post-eviction): If the client sequence entry has been evicted (TTL expired), the request proceeds as a new write. This is the documented limitation of the 24-hour TTL window.

### Cross-Failover Behavior

After a leader failover, the new leader's moka cache is empty. However, the `ClientSequenceEntry` is replicated via Raft and persisted in the `ClientSequences` B+ tree table. Retrying a write with the same idempotency key within the TTL window returns `ALREADY_COMMITTED` — the response includes the `assigned_sequence` but not the full `WriteSuccess` metadata.

**Idempotency key reuse detection**: If the `last_idempotency_key` matches but `last_request_hash` differs (same key, different payload), the server returns `IDEMPOTENCY_KEY_REUSED` — this indicates a client bug where the same UUID was used for different operations.

### TTL Eviction

Client sequence entries are evicted deterministically:

- **Trigger**: When `last_applied.index % eviction_interval == 0` (default: every 1,000 log entries)
- **Criteria**: Entries where `proposed_at - last_seen > ttl_seconds` (default: 86,400 seconds / 24 hours)
- **Determinism**: Eviction candidates are sorted by composite key encoding before deletion, ensuring identical behavior across all replicas regardless of HashMap iteration order

After eviction, a retried write with the same idempotency key is treated as a new request. Clients should complete retries within the 24-hour TTL window.

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
| Leader failover      | Retry with same idempotency key         | `ALREADY_COMMITTED` (replicated) |
| `SEQUENCE_GAP` error | Call `GetClientState`, resume sequence  | Reject until correct sequence    |
| Success              | Increment sequence for next write       | N/A                              |

**Exactly-once semantics**: Within the 24-hour TTL window, retries return the cached/replicated result regardless of leader changes. After TTL expiration, a retried write may execute as a new write. For operations requiring stronger guarantees, use application-level idempotency keys stored in the vault itself (via conditional writes with `SetCondition::IfNotExists`).

## Client Requirements

1. **Persist `last_used_sequence`** to durable storage before sending
2. **Increment by exactly 1** for each new transaction
3. **Reuse sequence number only for retries** of the same transaction
4. **On restart**, resume from persisted sequence

## Server Guarantees

1. Never accept `sequence <= last_committed_seq` as new transaction
2. Reject gaps (`sequence > last_committed_seq + 1`) to catch client bugs early
3. Persist `last_committed_seq` as part of replicated state (survives leader failover within TTL window)

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
| Organization entities  | `GetClientState(ns_id, client_id)`           | Control writing users, sessions  |
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
