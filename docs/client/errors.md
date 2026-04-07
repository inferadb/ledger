[Documentation](../README.md) > Client API > Errors

# Error Reference

gRPC error codes and Ledger-specific error conditions.

## gRPC Status Codes

Ledger uses standard gRPC status codes with additional context in error details.

### Common Codes

| Code | Name                  | Meaning                         | Retry?             |
| ---- | --------------------- | ------------------------------- | ------------------ |
| 0    | `OK`                  | Success                         | -                  |
| 3    | `INVALID_ARGUMENT`    | Malformed request               | No                 |
| 5    | `NOT_FOUND`           | Resource doesn't exist          | No                 |
| 6    | `ALREADY_EXISTS`      | Resource already exists         | No                 |
| 8    | `RESOURCE_EXHAUSTED`  | Rate limit or quota exceeded    | Yes (with backoff) |
| 9    | `FAILED_PRECONDITION` | State requirements not met      | Maybe              |
| 13   | `INTERNAL`            | Server error                    | Yes (with backoff) |
| 14   | `UNAVAILABLE`         | Service temporarily unavailable | Yes (with backoff) |
| 16   | `UNAUTHENTICATED`     | Missing or invalid token        | No (re-auth first) |

## Write Errors

### SEQUENCE_GAP

**Code**: `FAILED_PRECONDITION` (WriteErrorCode: `SEQUENCE_GAP`)

**Cause**: Client sent a sequence number higher than expected.

```
expected sequence 5, got 7
```

**Resolution**:

1. Call `GetClientState` to get the current sequence
2. Resume from `last_committed_sequence + 1`

```bash
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "client_id": {"id": "my-client"}}' \
  localhost:50051 ledger.v1.ReadService/GetClientState
```

**Recovery fields in error response:**

| Field                     | Type   | Description                        |
| ------------------------- | ------ | ---------------------------------- |
| `committed_tx_id`         | TxId   | Last successfully committed tx     |
| `committed_block_height`  | uint64 | Block containing last committed tx |
| `last_committed_sequence` | uint64 | Use this + 1 for next write        |

### KEY_EXISTS

**Code**: `FAILED_PRECONDITION` (WriteErrorCode: `KEY_EXISTS`)

**Cause**: Conditional write with `not_exists` condition failed because key already exists.

**Resolution**: Use `SetEntity` without condition to overwrite, or handle the existing value.

### KEY_NOT_FOUND

**Code**: `FAILED_PRECONDITION` (WriteErrorCode: `KEY_NOT_FOUND`)

**Cause**: Conditional write with `must_exists` condition failed because key doesn't exist.

**Resolution**: Create the key first, or remove the condition.

### VERSION_MISMATCH

**Code**: `FAILED_PRECONDITION` (WriteErrorCode: `VERSION_MISMATCH`)

**Cause**: Conditional write with `version` condition failed—current version differs from expected.

**Resolution**: Re-read the entity to get current version, then retry with updated version.

### VALUE_MISMATCH

**Code**: `FAILED_PRECONDITION` (WriteErrorCode: `VALUE_MISMATCH`)

**Cause**: Conditional write with `value_equals` condition failed—current value differs from expected.

**Resolution**: Re-read the entity to get current value, then decide how to proceed.

### ALREADY_COMMITTED

**Code**: `FAILED_PRECONDITION` (WriteErrorCode: `ALREADY_COMMITTED`)

**Cause**: Transaction with same `(client_id, sequence)` was already committed.

**Resolution**: This is expected for idempotent retries. The cached response is returned.

### ORGANIZATION_NOT_FOUND

**Code**: `NOT_FOUND`

**Cause**: Operation references a non-existent organization.

**Resolution**: Create the organization first with `AdminService/CreateOrganization`.

### VAULT_NOT_FOUND

**Code**: `NOT_FOUND`

**Cause**: Operation references a non-existent vault.

**Resolution**: Create the vault first with `AdminService/CreateVault`.

### VAULT_UNAVAILABLE

**Code**: `UNAVAILABLE`

**Cause**: Vault has detected state divergence and is not accepting operations.

**Resolution**:

1. Check `ledger_determinism_bug_total` metric
2. Run `AdminService/RecoverVault` to attempt recovery
3. If recovery fails, restore from healthy replica

### LEADER_NOT_AVAILABLE

**Code**: `UNAVAILABLE`

**Cause**: No Raft leader is currently elected.

**Resolution**: Retry with exponential backoff. Election should complete within seconds.

```bash
# Retry with exponential backoff until leader is elected
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "client_id": {"id": "c1"}, "sequence": "1", ...}' \
  localhost:50051 ledger.v1.WriteService/Write
```

The SDK handles retries automatically. See [SDK Guide](../client/sdk.md) for retry configuration.

### RATE_LIMITED

**Code**: `RESOURCE_EXHAUSTED`

**Cause**: Request rate exceeded organization limits.

**Resolution**: Implement rate limiting on the client side or request limit increase.

## Read Errors

### HEIGHT_UNAVAILABLE

**Code**: `FAILED_PRECONDITION`

**Cause**: Requested historical height has been pruned (compacted retention policy).

**Resolution**: Use a more recent height or omit height for current state.

```bash
# Get current tip to find available height
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "vault": {"slug": "7180591718400"}}' \
  localhost:50051 ledger.v1.ReadService/GetTip
```

## Admin Errors

### CLUSTER_NOT_READY

**Code**: `FAILED_PRECONDITION`

**Cause**: Cluster hasn't completed bootstrap.

**Resolution**: Wait for bootstrap to complete. Check `GetNodeInfo` for status.

### NODE_ALREADY_MEMBER

**Code**: `ALREADY_EXISTS`

**Cause**: `JoinCluster` called for a node that's already a member.

**Resolution**: Node is already in the cluster; no action needed.

### QUORUM_LOSS

**Code**: `UNAVAILABLE`

**Cause**: Operation would result in quorum loss.

**Resolution**: Ensure sufficient nodes remain before removing members.

### SNAPSHOT_IN_PROGRESS

**Code**: `FAILED_PRECONDITION`

**Cause**: `CreateSnapshot` called while another snapshot is in progress.

**Resolution**: Wait for current snapshot to complete.

## Token Errors

Token-related operations (via `TokenService`) return specific error conditions:

### Validation Errors

| Error                  | gRPC Code          | Description                                    |
| ---------------------- | ------------------ | ---------------------------------------------- |
| `Expired`              | `UNAUTHENTICATED`  | Token has expired                              |
| `InvalidSignature`     | `UNAUTHENTICATED`  | Token signature verification failed            |
| `Revoked`              | `UNAUTHENTICATED`  | Token was explicitly revoked                   |
| `InvalidAudience`      | `UNAUTHENTICATED`  | Token audience does not match expected service |
| `InvalidIssuer`        | `UNAUTHENTICATED`  | Token issuer does not match                    |
| `MissingClaim`         | `INVALID_ARGUMENT` | A required JWT claim is missing                |
| `InvalidTokenType`     | `INVALID_ARGUMENT` | Token type does not match expected type        |
| `TokenVersionMismatch` | `UNAUTHENTICATED`  | Session force-invalidated (version bump)       |
| `InvalidScope`         | `INVALID_ARGUMENT` | Requested scope is not allowed                 |

### Signing Key Errors

| Error                | gRPC Code             | Description                                   |
| -------------------- | --------------------- | --------------------------------------------- |
| `NoActiveSigningKey` | `FAILED_PRECONDITION` | No active signing key for the requested scope |
| `SigningKeyNotFound` | `NOT_FOUND`           | Signing key not found by `kid`                |
| `SigningKeyRevoked`  | `FAILED_PRECONDITION` | Signing key has been revoked                  |
| `SigningKeyExpired`  | `FAILED_PRECONDITION` | Signing key has expired past its grace period |

### Refresh Token Errors

| Error                 | gRPC Code         | Description                                  |
| --------------------- | ----------------- | -------------------------------------------- |
| `InvalidRefreshToken` | `UNAUTHENTICATED` | Refresh token is invalid or expired          |
| `RefreshTokenReuse`   | `UNAUTHENTICATED` | Refresh token reuse detected; family revoked |

Refresh token reuse triggers family poisoning — all tokens in the same family are eventually revoked. This is a theft detection mechanism and cannot be reversed.

## Error Details

Ledger includes structured error details when available:

```protobuf
message ErrorDetails {
  string error_code = 1;                  // Machine-readable error code (e.g., "3203")
  bool is_retryable = 2;                  // Whether the client should retry
  optional int32 retry_after_ms = 3;      // Suggested retry delay (rate-limit/backpressure)
  map<string, string> context = 4;        // Structured key-value context
  optional string suggested_action = 5;   // Human-readable recovery guidance
}
```

Example with grpcurl:

```bash
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "999"}}' \
  localhost:50051 ledger.v1.ReadService/Read 2>&1
```

Output:

```
ERROR:
  Code: NotFound
  Message: organization not found
  Details:
    - code: ORGANIZATION_NOT_FOUND
      message: organization with id 999 does not exist
      metadata:
        organization_slug: "999"
```

## Client Best Practices

### Retry Strategy

```python
# Pseudo-code retry strategy
RETRIABLE_CODES = {
    codes.UNAVAILABLE,
    codes.INTERNAL,
    codes.RESOURCE_EXHAUSTED,
}

def call_with_retry(fn, max_retries=5):
    for attempt in range(max_retries):
        try:
            return fn()
        except grpc.RpcError as e:
            if e.code() not in RETRIABLE_CODES:
                raise
            if attempt == max_retries - 1:
                raise
            sleep(min(2 ** attempt * 0.1, 5))  # Exponential backoff, max 5s
```

### Handling Sequence Gaps

```rust
// Pseudo-code for sequence recovery
async fn write_with_recovery(
    client: &LedgerClient,
    caller: UserSlug,
    org: OrganizationSlug,
    ops: Vec<Operation>,
) -> Result<()> {
    loop {
        match client.write(caller, org, None, ops.clone(), None).await {
            Ok(resp) => {
                return Ok(());
            }
            Err(e) if e.is_sequence_gap() => {
                // Sequence gap - recover and retry
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}
```

### Idempotency

All write operations are idempotent when using the same `(client_id, sequence)` pair:

```bash
# First call
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "client_id": {"id": "c1"}, "sequence": "1", ...}' \
  localhost:50051 ledger.v1.WriteService/Write

# Retry with same client_id and sequence returns cached response
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "client_id": {"id": "c1"}, "sequence": "1", ...}' \
  localhost:50051 ledger.v1.WriteService/Write
```

The idempotency cache ensures the operation is only applied once.

The idempotency cache is replicated via Raft and survives leader failover within the 24-hour TTL window. Retried requests with matching `(client_id, sequence)` return `ALREADY_COMMITTED` after leader change.
