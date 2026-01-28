[Documentation](../README.md) > Development > Debugging

# Debugging Guide

Techniques for diagnosing issues in Ledger development and testing.

## Logging

### Enable Debug Logging

```bash
RUST_LOG=debug cargo run -p inferadb-ledger-server
```

### Per-Module Logging

```bash
# Raft consensus details
RUST_LOG=inferadb_ledger_raft=debug cargo run -p inferadb-ledger-server

# Storage operations
RUST_LOG=inferadb_ledger_store=trace cargo run -p inferadb-ledger-server

# Multiple modules
RUST_LOG=inferadb_ledger_raft=debug,inferadb_ledger_state=info cargo run -p inferadb-ledger-server
```

### Log Levels

| Level | Use Case                              |
| ----- | ------------------------------------- |
| error | Failures requiring attention          |
| warn  | Unexpected but recoverable conditions |
| info  | Operational events (start, stop)      |
| debug | Detailed flow for debugging           |
| trace | Very verbose (e.g., every page read)  |

## Test Debugging

### See Test Output

```bash
cargo test test_name -- --nocapture
```

### Run Tests Sequentially

For tests with shared resources or timing issues:

```bash
cargo test -- --test-threads=1
```

### Get Backtrace on Failure

```bash
RUST_BACKTRACE=1 cargo test
RUST_BACKTRACE=full cargo test  # With full symbols
```

### Run Specific Test with Logging

```bash
RUST_LOG=debug cargo test test_write_batch -- --nocapture
```

## Common Issues

### Test Hangs

**Symptom:** Test doesn't complete, no output.

**Likely causes:**

1. Deadlock - Two tasks waiting on each other
2. Missing timeout - Awaiting something that never arrives
3. Infinite loop - Logic error in retry or processing

**Debug approach:**

```bash
# Run single-threaded to isolate
cargo test test_name -- --test-threads=1 --nocapture

# Add trace logging
RUST_LOG=trace cargo test test_name -- --nocapture
```

### State Root Mismatch

**Symptom:** `ledger_determinism_bug_total` metric > 0, vault in `DIVERGED` state.

**Likely causes:**

1. Non-deterministic operation (timestamp, random, hash iteration order)
2. Floating-point arithmetic
3. Missing or inconsistent serialization

**Debug approach:**

1. Check logs for divergence details (expected vs computed hash)
2. Replay transactions manually on a single node
3. Add checksums at intermediate steps to narrow down

### Raft Election Loops

**Symptom:** Cluster never stabilizes, logs show repeated elections.

**Likely causes:**

1. Network partition or connectivity issue
2. Nodes with mismatched cluster configuration
3. Insufficient election timeout (too aggressive)

**Debug approach:**

```bash
# Check cluster state from each node
grpcurl -plaintext node1:50051 ledger.v1.AdminService/GetClusterInfo
grpcurl -plaintext node2:50051 ledger.v1.AdminService/GetClusterInfo

# Check Raft metrics
curl -s localhost:9090/metrics | grep raft
```

### Write Timeout

**Symptom:** Write operations return `UNAVAILABLE` or timeout.

**Likely causes:**

1. No leader elected
2. Leader overloaded (high proposal backlog)
3. Majority of nodes unreachable

**Debug approach:**

```bash
# Check for leader
grpcurl -plaintext localhost:50051 ledger.v1.AdminService/GetClusterInfo

# Check proposal backlog
curl -s localhost:9090/metrics | grep proposals_pending
```

## gRPC Debugging

### Inspect Requests/Responses

```bash
# Verbose mode shows wire format
grpcurl -v -plaintext localhost:50051 ledger.v1.HealthService/Check
```

### List Available Services

```bash
grpcurl -plaintext localhost:50051 list
```

### Describe Service

```bash
grpcurl -plaintext localhost:50051 describe ledger.v1.AdminService
```

## Metrics for Debugging

### Key Metrics

| Metric                                   | What It Tells You                      |
| ---------------------------------------- | -------------------------------------- |
| `inferadb_ledger_raft_is_leader`         | Whether this node is leader (0 or 1)   |
| `inferadb_ledger_raft_proposals_pending` | Backlog of uncommitted proposals       |
| `inferadb_ledger_raft_term`              | Current Raft term (increases on elect) |
| `ledger_write_latency_seconds`           | Write operation timing                 |
| `ledger_determinism_bug_total`           | State divergence count (should be 0)   |

### Query Metrics

```bash
curl -s localhost:9090/metrics | grep <pattern>

# Examples
curl -s localhost:9090/metrics | grep raft_is_leader
curl -s localhost:9090/metrics | grep write_latency
```

## IDE Integration

### VS Code

Add to `.vscode/launch.json`:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "type": "lldb",
      "request": "launch",
      "name": "Debug Server",
      "cargo": {
        "args": ["build", "-p", "inferadb-ledger-server"]
      },
      "args": [],
      "env": {
        "RUST_LOG": "debug",
        "INFERADB__LEDGER__DATA": "/tmp/ledger-debug",
        "INFERADB__LEDGER__BOOTSTRAP_EXPECT": "1"
      }
    },
    {
      "type": "lldb",
      "request": "launch",
      "name": "Debug Test",
      "cargo": {
        "args": ["test", "--no-run", "-p", "inferadb-ledger-state"]
      },
      "args": ["test_name", "--nocapture"]
    }
  ]
}
```

### rust-analyzer

Enable inlay hints for type information during debugging. Add to `settings.json`:

```json
{
  "rust-analyzer.inlayHints.typeHints.enable": true,
  "rust-analyzer.inlayHints.parameterHints.enable": true
}
```

## Performance Profiling

### CPU Profiling with perf

```bash
cargo build --release -p inferadb-ledger-server
perf record -g ./target/release/inferadb-ledger
perf report
```

### Memory Profiling with heaptrack

```bash
cargo build --release -p inferadb-ledger-server
heaptrack ./target/release/inferadb-ledger
heaptrack_gui heaptrack.inferadb-ledger.<pid>.gz
```

### Flamegraphs

```bash
cargo install flamegraph
cargo flamegraph -p inferadb-ledger-server
```

## Related Documentation

- [Testing Guide](testing.md) - Test commands and patterns
- [Troubleshooting](../troubleshooting.md) - Production issue resolution
- [Metrics Reference](../operations/metrics-reference.md) - All available metrics
