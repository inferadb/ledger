# Logging Integration Guide

Guide for engineers adding request logging to new services or extending existing integrations.

## Overview

Request logging emits one comprehensive JSON event per gRPC request. The `RequestContext` builder accumulates fields throughout the request lifecycle and emits automatically on drop.

```rust
// Context created at method entry
let mut ctx = RequestContext::new("MyService", "my_method");

// Fields populated as information becomes available
ctx.set_client_id(&req.client_id);
ctx.set_organization_slug(req.organization_slug);

// Outcome set before return
ctx.set_success();

// Event emitted automatically when ctx drops
```

## Step-by-Step Integration

### Step 1: Add Dependencies

Your service struct needs access to the sampler and node ID:

```rust
use crate::logging::{OperationType, RequestContext, Sampler};

pub struct MyServiceImpl {
    // ... existing fields ...
    sampler: Sampler,
    node_id: u64,
}

impl MyServiceImpl {
    pub fn new(/* ... */, sampler: Sampler, node_id: u64) -> Self {
        Self {
            // ...
            sampler,
            node_id,
        }
    }
}
```

### Step 2: Create Context at Method Entry

At the start of each gRPC method:

```rust
#[tonic::async_trait]
impl MyService for MyServiceImpl {
    async fn my_method(
        &self,
        request: Request<MyRequest>,
    ) -> Result<Response<MyResponse>, Status> {
        // Create context with service and method names
        let mut ctx = RequestContext::new("MyService", "my_method");

        // Set the sampler for tail sampling
        ctx.set_sampler(self.sampler.clone());

        // Set operation type (Read, Write, or Admin)
        ctx.set_operation_type(OperationType::Read);

        // Extract trace context from incoming gRPC metadata
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        ctx.set_trace_context(&trace_ctx);

        // ... rest of implementation
    }
}
```

### Step 3: Populate Fields Progressively

Add fields as information becomes available during request processing:

```rust
// From request proto
let req = request.into_inner();
ctx.set_client_id(&req.client_id);
ctx.set_organization_slug(req.organization_slug);

// System context (from Raft state)
ctx.set_system_context(
    self.node_id,
    self.raft.is_leader(),
    self.raft.current_term(),
);

// Operation-specific fields
ctx.set_key(&req.key);
ctx.set_include_proof(req.include_proof);
```

### Step 4: Track Timing

Use timer methods around slow operations:

```rust
// Around Raft consensus
ctx.start_raft_timer();
let result = self.raft.propose(proposal).await;
ctx.end_raft_timer();

// Around storage operations
ctx.start_storage_timer();
let value = self.storage.get(&key).await;
ctx.end_storage_timer();
```

### Step 5: Set Outcome

Before the method returns, set the appropriate outcome:

```rust
match result {
    Ok(response) => {
        ctx.set_success();
        ctx.set_block_height(response.block_height);
        ctx.set_found(response.value.is_some());
        if let Some(ref value) = response.value {
            ctx.set_value_size_bytes(value.len());
        }
        Ok(Response::new(response))
    }
    Err(e) => {
        ctx.set_error(&e.code().to_string(), &e.message());
        Err(e)
    }
}
```

### Step 6: Remove Redundant Logging

Remove or consolidate existing logging that the canonical log line replaces:

```rust
// REMOVE: These are now captured in the canonical log line
// debug!("Processing request for client {}", client_id);
// info!("Request completed in {}ms", duration);

// KEEP: Error-level logs for unexpected conditions (alerts)
// error!("Unexpected state corruption: {}", details);
```

## Complete Example

```rust
async fn read(
    &self,
    request: Request<ReadRequest>,
) -> Result<Response<ReadResponse>, Status> {
    // 1. Create context
    let mut ctx = RequestContext::new("ReadService", "read");
    ctx.set_sampler(self.sampler.clone());
    ctx.set_operation_type(OperationType::Read);

    // Extract trace context
    let trace_ctx = trace_context::extract_or_generate(request.metadata());
    ctx.set_trace_context(&trace_ctx);

    let req = request.into_inner();

    // 2. Populate request fields
    ctx.set_client_id(&req.client_id);
    ctx.set_organization_slug(req.organization_slug);
    ctx.set_vault_slug(req.vault_slug);
    ctx.set_key(&req.key);
    ctx.set_consistency("linearizable");

    // 3. System context
    ctx.set_system_context(
        self.node_id,
        self.raft.is_leader(),
        self.raft.current_term(),
    );

    // 4. Execute with timing
    ctx.start_storage_timer();
    let result = self.storage.get(&req.key).await;
    ctx.end_storage_timer();

    // 5. Set outcome
    match result {
        Ok(Some(value)) => {
            ctx.set_success();
            ctx.set_found(true);
            ctx.set_value_size_bytes(value.len());
            Ok(Response::new(ReadResponse { value: Some(value) }))
        }
        Ok(None) => {
            ctx.set_success();
            ctx.set_found(false);
            Ok(Response::new(ReadResponse { value: None }))
        }
        Err(e) => {
            ctx.set_error("STORAGE_ERROR", &e.to_string());
            Err(Status::internal(e.to_string()))
        }
    }
    // Context drops here, event emitted automatically
}
```

## Testing Integration

Write integration tests that verify event emission:

```rust
#[tokio::test]
async fn test_read_emits_log_event() {
    let (events, _guard) = capture_log_events();

    let service = create_test_service();
    let request = Request::new(ReadRequest {
        client_id: "test_client".to_string(),
        organization_slug: 1,
        vault_slug: 1,
        key: "test_key".to_string(),
    });

    let _ = service.read(request).await;

    let events = events.lock();
    assert_eq!(events.len(), 1);

    let event = &events[0];
    assert_eq!(event.service, "ReadService");
    assert_eq!(event.method, "read");
    assert_eq!(event.client_id, Some("test_client".to_string()));
    assert!(event.duration_ms > 0.0);
}
```

### Test Categories

1. **Unit tests**: Each builder method in isolation
2. **Integration tests**: Full request flow emits correct fields
3. **Sampling tests**: Verify deterministic sampling decisions
4. **Error path tests**: Errors produce `outcome=error` with correct codes

## RequestContext API Reference

### Creation

| Method                                 | Description                                     |
| -------------------------------------- | ----------------------------------------------- |
| `RequestContext::new(service, method)` | Create context with auto-generated `request_id` |
| `set_sampler(sampler)`                 | Attach sampler for tail sampling decisions      |
| `set_operation_type(op_type)`          | Set as `Read`, `Write`, or `Admin`              |

### Request Metadata

| Method                 | Description                   |
| ---------------------- | ----------------------------- |
| `set_client_id(id)`    | Idempotency client identifier |
| `set_sequence(seq)`    | Per-client sequence number    |
| `set_organization_slug(id)` | Target organization              |
| `set_vault_slug(slug)` | Target vault                  |
| `set_actor(actor)`     | Identity performing operation |

### System Context

| Method                                              | Description              |
| --------------------------------------------------- | ------------------------ |
| `set_system_context(node_id, is_leader, raft_term)` | Set all system fields    |
| `set_shard_id(id)`                                  | Shard routing identifier |
| `set_is_vip(is_vip)`                                | VIP organization indicator  |

### Operation Fields

| Method                           | Description                   |
| -------------------------------- | ----------------------------- |
| `set_operations_count(count)`    | Number of operations (writes) |
| `set_operation_types(types)`     | Operation names (writes)      |
| `set_key(key)`                   | Single key (reads)            |
| `set_keys_count(count)`          | Batch key count               |
| `set_consistency(level)`         | Read consistency              |
| `set_include_proof(include)`     | Proof requested               |
| `set_idempotency_hit(hit)`       | Cache hit indicator           |
| `set_batch_coalesced(coalesced)` | Batch coalescing              |
| `set_batch_size(size)`           | Batch size                    |

### Timing

| Method                  | Description          |
| ----------------------- | -------------------- |
| `start_raft_timer()`    | Begin Raft timing    |
| `end_raft_timer()`      | End Raft timing      |
| `start_storage_timer()` | Begin storage timing |
| `end_storage_timer()`   | End storage timing   |

### Outcome

| Method                         | Description                   |
| ------------------------------ | ----------------------------- |
| `set_success()`                | Mark as successful            |
| `set_error(code, message)`     | Mark as error                 |
| `set_cached()`                 | Mark as idempotency cache hit |
| `set_rate_limited()`           | Mark as rate limited          |
| `set_precondition_failed(key)` | Mark as CAS failure           |
| `set_block_height(height)`     | Committed block height        |
| `set_block_hash(hash)`         | Block hash (auto-truncated)   |
| `set_state_root(root)`         | State root (auto-truncated)   |
| `set_found(found)`             | Key existence (reads)         |
| `set_value_size_bytes(size)`   | Response size                 |

### Tracing

| Method                   | Description                                |
| ------------------------ | ------------------------------------------ |
| `set_trace_context(ctx)` | Set trace/span/parent IDs from W3C context |

## Task-Local Context Access

For deep call stacks where passing context is impractical:

```rust
use crate::logging::with_current_context;

// In a nested function without direct context access
fn record_storage_metric(key: &str, size: usize) {
    with_current_context(|ctx| {
        ctx.set_key(key);
        ctx.set_value_size_bytes(size);
    });
}
```

The context is task-local (not thread-local), surviving across await points in async code.

## Performance Considerations

Request logging is designed for minimal overhead:

| Operation                  | Typical Time |
| -------------------------- | ------------ |
| Context creation           | ~147ns       |
| Field population (all 50+) | ~1.2μs       |
| Sampling decision          | ~2-4ns       |
| Total overhead             | ~1.1μs       |

**Guidelines:**

- Create context once per request, not per operation
- Set fields immediately when available (no batching needed)
- Timer methods have nanosecond precision
- Sampling happens at drop, not during field population

## String Field Limits

Fields are automatically truncated to prevent log injection:

| Field           | Max Length |
| --------------- | ---------- |
| `client_id`     | 128 chars  |
| `error_message` | 512 chars  |
| `error_key`     | 256 chars  |
| `key`           | 256 chars  |

Truncated strings end with `...`. Control characters are replaced with `\uFFFD`.

## Related Documentation

- [Operator Guide](../operations/logging.md) - Field reference, query cookbook
- [Configuration Reference](../operations/configuration.md#logging) - All config options
- [Testing Guide](testing.md) - Test infrastructure for request logging
