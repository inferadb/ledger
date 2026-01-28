# Configuration

Complete reference for Ledger configuration via environment variables and CLI arguments.

## Quick Reference

| Variable                           | CLI               | Default           | Description                            |
| ---------------------------------- | ----------------- | ----------------- | -------------------------------------- |
| `INFERADB__LEDGER__LISTEN`         | `--listen`        | `127.0.0.1:50051` | gRPC listen address                    |
| `INFERADB__LEDGER__METRICS`        | `--metrics`       | (disabled)        | Prometheus metrics address             |
| `INFERADB__LEDGER__DATA`           | `--data`          | (ephemeral)       | Data directory                         |
| `INFERADB__LEDGER__CLUSTER`        | `--cluster`       | `3`               | Cluster size for coordinated bootstrap |
| `INFERADB__LEDGER__PEERS`          | `--peers`         | (none)            | Peer discovery (DNS or file)           |
| `INFERADB__LEDGER__PEERS_TTL`      | `--peers-ttl`     | `3600`            | Peer cache TTL (seconds)               |
| `INFERADB__LEDGER__PEERS_TIMEOUT`  | `--peers-timeout` | `60`              | Peer discovery timeout (seconds)       |
| `INFERADB__LEDGER__PEERS_POLL`     | `--peers-poll`    | `2`               | Peer discovery poll interval (seconds) |
| `INFERADB__LEDGER__BATCH_SIZE`     | `--batch-size`    | `100`             | Max transactions per batch             |
| `INFERADB__LEDGER__BATCH_DELAY`    | `--batch-delay`   | `0.01`            | Max batch wait time (seconds)          |
| `INFERADB__LEDGER__MAX_CONCURRENT` | `--concurrent`    | `100`             | Max concurrent requests                |
| `INFERADB__LEDGER__TIMEOUT`        | `--timeout`       | `30`              | Request timeout (seconds)              |

CLI arguments take precedence over environment variables.

## Network Configuration

### Listen Address

```bash
# Localhost only (default, secure for development)
INFERADB__LEDGER__LISTEN=127.0.0.1:50051

# Accept all interfaces (required for containers/remote access)
INFERADB__LEDGER__LISTEN=0.0.0.0:50051

# Specific interface (WireGuard)
INFERADB__LEDGER__LISTEN=10.0.0.1:50051
```

### Metrics Endpoint

```bash
# Enable Prometheus metrics
INFERADB__LEDGER__METRICS=0.0.0.0:9090
```

Disabled by default. Enable for production monitoring.

## Bootstrap Modes

Ledger supports three bootstrap modes, controlled via CLI flags:

| Flag          | Env Value   | Description                           |
| ------------- | ----------- | ------------------------------------- |
| `--single`    | `CLUSTER=1` | Single-node cluster (no coordination) |
| `--join`      | `CLUSTER=0` | Wait to be added via AdminService     |
| `--cluster N` | `CLUSTER=N` | Coordinated N-node bootstrap          |

### Single-Node Mode

```bash
# CLI
inferadb-ledger --single --data /data

# Environment
INFERADB__LEDGER__CLUSTER=1 inferadb-ledger
```

Use for development, testing, or single-node deployments.

### Join Mode

```bash
# CLI
inferadb-ledger --join --data /data --peers ledger.internal

# Environment
INFERADB__LEDGER__CLUSTER=0 \
INFERADB__LEDGER__PEERS=ledger.internal \
inferadb-ledger
```

Node waits to be added via `AdminService/JoinCluster`. Use when adding nodes to an existing cluster.

### Coordinated Bootstrap

```bash
# CLI (3-node cluster)
inferadb-ledger --cluster 3 --data /data --peers ledger.internal

# Environment
INFERADB__LEDGER__CLUSTER=3 \
INFERADB__LEDGER__PEERS=ledger.internal \
inferadb-ledger
```

Nodes discover each other, exchange IDs, and the lowest-ID node bootstraps the cluster. Default mode.

## Peer Discovery

### DNS-Based (Recommended)

```bash
# Kubernetes headless service
INFERADB__LEDGER__PEERS=ledger.default.svc.cluster.local

# Custom DNS domain
INFERADB__LEDGER__PEERS=ledger.internal.example.com
```

Resolves A records to find peer IP addresses.

### File-Based

```bash
# Absolute path
INFERADB__LEDGER__PEERS=/var/lib/ledger/peers.json

# Relative path
INFERADB__LEDGER__PEERS=./peers.json
```

JSON format:

```json
{
  "cached_at": 0,
  "peers": [
    { "addr": "10.0.0.1:50051" },
    { "addr": "10.0.0.2:50051" },
    { "addr": "10.0.0.3:50051" }
  ]
}
```

Detection: Values containing `/` or `\` or ending with `.json` are treated as file paths.

### Discovery Parameters

```bash
# Cache peer list for 1 hour
INFERADB__LEDGER__PEERS_TTL=3600

# Wait up to 2 minutes for all peers during bootstrap
INFERADB__LEDGER__PEERS_TIMEOUT=120

# Poll for new peers every 5 seconds
INFERADB__LEDGER__PEERS_POLL=5
```

## Storage

### Data Directory

```bash
# Persistent storage
INFERADB__LEDGER__DATA=/var/lib/ledger

# Ephemeral (temporary directory, data lost on shutdown)
# Omit DATA or use --data without value
```

Contents:

- `node_id` - Persistent node identifier (Snowflake ID)
- `raft/` - Raft log and snapshots
- `state/` - State machine data

### Ephemeral Mode

When `DATA` is not set, Ledger creates a temporary directory with a unique Snowflake ID. Useful for:

- Local development
- Integration tests
- CI/CD pipelines

Data is lost when the process exits.

## Batching

Controls write request batching for throughput vs latency trade-off.

```bash
# High throughput (batch up to 500 requests, wait up to 50ms)
INFERADB__LEDGER__BATCH_SIZE=500
INFERADB__LEDGER__BATCH_DELAY=0.05

# Low latency (smaller batches, shorter wait)
INFERADB__LEDGER__BATCH_SIZE=10
INFERADB__LEDGER__BATCH_DELAY=0.001
```

### Eager vs Timeout Commits

- **Eager commit**: Batch flushes immediately when the request queue drains
- **Timeout commit**: Batch flushes when `BATCH_DELAY` expires

Monitor `ledger_batch_eager_commits_total` vs `ledger_batch_timeout_commits_total` to tune.

## Request Limits

```bash
# Allow 200 concurrent requests
INFERADB__LEDGER__MAX_CONCURRENT=200

# 60-second request timeout
INFERADB__LEDGER__TIMEOUT=60
```

## Complete Example

### Docker Compose

```yaml
services:
  ledger:
    image: inferadb/ledger:latest
    environment:
      INFERADB__LEDGER__LISTEN: "0.0.0.0:50051"
      INFERADB__LEDGER__METRICS: "0.0.0.0:9090"
      INFERADB__LEDGER__DATA: "/data"
      INFERADB__LEDGER__CLUSTER: "3"
      INFERADB__LEDGER__PEERS: "ledger.internal"
      INFERADB__LEDGER__BATCH_SIZE: "100"
      INFERADB__LEDGER__BATCH_DELAY: "0.01"
    volumes:
      - ledger-data:/data
    ports:
      - "50051:50051"
      - "9090:9090"
```

### Kubernetes ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: ledger-config
data:
  INFERADB__LEDGER__LISTEN: "0.0.0.0:50051"
  INFERADB__LEDGER__METRICS: "0.0.0.0:9090"
  INFERADB__LEDGER__DATA: "/data"
  INFERADB__LEDGER__CLUSTER: "3"
  INFERADB__LEDGER__PEERS: "ledger-headless.default.svc.cluster.local"
```

### CLI with All Options

```bash
inferadb-ledger \
  --listen 0.0.0.0:50051 \
  --metrics 0.0.0.0:9090 \
  --data /var/lib/ledger \
  --cluster 3 \
  --peers ledger.internal \
  --peers-ttl 3600 \
  --peers-timeout 120 \
  --peers-poll 5 \
  --batch-size 100 \
  --batch-delay 0.01 \
  --concurrent 100 \
  --timeout 30
```
