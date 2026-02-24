# Configuration

Complete reference for Ledger configuration via environment variables and CLI arguments.

## Quick Reference

| Variable                             | CLI               | Default           | Description                            |
| ------------------------------------ | ----------------- | ----------------- | -------------------------------------- |
| `INFERADB__LEDGER__LISTEN`           | `--listen`        | `127.0.0.1:50051` | gRPC listen address                    |
| `INFERADB__LEDGER__METRICS`          | `--metrics`       | (disabled)        | Prometheus metrics address             |
| `INFERADB__LEDGER__DATA`             | `--data`          | (ephemeral)       | Data directory                         |
| `INFERADB__LEDGER__CLUSTER`          | `--cluster`       | `3`               | Cluster size for coordinated bootstrap |
| `INFERADB__LEDGER__PEERS`            | `--peers`         | (none)            | Peer discovery (DNS or file)           |
| `INFERADB__LEDGER__PEERS_TTL`        | `--peers-ttl`     | `3600`            | Peer cache TTL (seconds)               |
| `INFERADB__LEDGER__PEERS_TIMEOUT`    | `--peers-timeout` | `60`              | Peer discovery timeout (seconds)       |
| `INFERADB__LEDGER__PEERS_POLL`       | `--peers-poll`    | `2`               | Peer discovery poll interval (seconds) |
| `INFERADB__LEDGER__BATCH_SIZE`       | `--batch-size`    | `100`             | Max transactions per batch             |
| `INFERADB__LEDGER__BATCH_DELAY`      | `--batch-delay`   | `0.01`            | Max batch wait time (seconds)          |
| `INFERADB__LEDGER__MAX_CONCURRENT`   | `--concurrent`    | `100`             | Max concurrent requests                |
| `INFERADB__LEDGER__TIMEOUT`          | `--timeout`       | `30`              | Request timeout (seconds)              |
| `INFERADB__LEDGER__LOG_FORMAT`       | -                 | `auto`            | Log format (text/json/auto)            |
| `INFERADB__LEDGER__LOGGING__ENABLED` | -                 | `true`            | Enable request logging                 |

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

## Logging

Request logging emits one comprehensive JSON event per request for queryable observability. See [Logging](logging.md) for complete field reference and query cookbook.

### Log Format

```bash
# JSON format for production (default when non-TTY)
INFERADB__LEDGER__LOG_FORMAT=json

# Text format for development (default when TTY)
INFERADB__LEDGER__LOG_FORMAT=text

# Auto-detect based on terminal
INFERADB__LEDGER__LOG_FORMAT=auto
```

### Core Settings

| Variable                             | Type | Default | Description                      |
| ------------------------------------ | ---- | ------- | -------------------------------- |
| `INFERADB__LEDGER__LOG_FORMAT`       | str  | `auto`  | Log format: `text`/`json`/`auto` |
| `INFERADB__LEDGER__LOGGING__ENABLED` | bool | `true`  | Enable request log emission      |

### Sampling Configuration

Control log volume while retaining critical events. Errors and slow requests are always logged (100%).

| Variable                                                       | Type | Default  | Description                       |
| -------------------------------------------------------------- | ---- | -------- | --------------------------------- |
| `INFERADB__LEDGER__LOGGING__SAMPLING__ERROR_RATE`              | f64  | `1.0`    | Sample rate for errors (0.0-1.0)  |
| `INFERADB__LEDGER__LOGGING__SAMPLING__SLOW_RATE`               | f64  | `1.0`    | Sample rate for slow requests     |
| `INFERADB__LEDGER__LOGGING__SAMPLING__VIP_RATE`                | f64  | `0.5`    | Sample rate for VIP organizations |
| `INFERADB__LEDGER__LOGGING__SAMPLING__WRITE_RATE`              | f64  | `0.1`    | Sample rate for normal writes     |
| `INFERADB__LEDGER__LOGGING__SAMPLING__READ_RATE`               | f64  | `0.01`   | Sample rate for normal reads      |
| `INFERADB__LEDGER__LOGGING__SAMPLING__SLOW_THRESHOLD_READ_MS`  | f64  | `10.0`   | Slow threshold for reads (ms)     |
| `INFERADB__LEDGER__LOGGING__SAMPLING__SLOW_THRESHOLD_WRITE_MS` | f64  | `100.0`  | Slow threshold for writes (ms)    |
| `INFERADB__LEDGER__LOGGING__SAMPLING__SLOW_THRESHOLD_ADMIN_MS` | f64  | `1000.0` | Slow threshold for admin (ms)     |

### VIP Organizations

VIP organizations receive elevated sampling rates (50% vs 10%/1% default).

| Variable                                            | Type     | Default | Description                                 |
| --------------------------------------------------- | -------- | ------- | ------------------------------------------- |
| `INFERADB__LEDGER__LOGGING__VIP_ORGANIZATIONS`      | Vec<i64> | `[]`    | Static VIP organization IDs                 |
| `INFERADB__LEDGER__LOGGING__VIP__DISCOVERY_ENABLED` | bool     | `true`  | Enable dynamic VIP discovery from `_system` |
| `INFERADB__LEDGER__LOGGING__VIP__CACHE_TTL_SECS`    | u64      | `60`    | VIP cache refresh interval                  |
| `INFERADB__LEDGER__LOGGING__VIP__TAG_NAME`          | str      | `"vip"` | Metadata tag name for VIP status            |

### OpenTelemetry Export

Export request logs as OTLP traces for visualization in Jaeger, Tempo, or Honeycomb.

| Variable                                               | Type  | Default | Description                          |
| ------------------------------------------------------ | ----- | ------- | ------------------------------------ |
| `INFERADB__LEDGER__LOGGING__OTEL__ENABLED`             | bool  | `false` | Enable OTLP trace export             |
| `INFERADB__LEDGER__LOGGING__OTEL__ENDPOINT`            | str   | -       | OTLP collector endpoint URL          |
| `INFERADB__LEDGER__LOGGING__OTEL__TRANSPORT`           | str   | `grpc`  | Transport: `grpc` or `http`          |
| `INFERADB__LEDGER__LOGGING__OTEL__BATCH_SIZE`          | usize | `512`   | Spans to batch before flush          |
| `INFERADB__LEDGER__LOGGING__OTEL__BATCH_INTERVAL_MS`   | u64   | `5000`  | Max time between flushes (ms)        |
| `INFERADB__LEDGER__LOGGING__OTEL__TIMEOUT_MS`          | u64   | `10000` | Per-export timeout (ms)              |
| `INFERADB__LEDGER__LOGGING__OTEL__SHUTDOWN_TIMEOUT_MS` | u64   | `15000` | Graceful shutdown timeout (ms)       |
| `INFERADB__LEDGER__LOGGING__OTEL__TRACE_RAFT_RPCS`     | bool  | `true`  | Propagate trace context in Raft RPCs |

### Logging Example

```bash
# Production: JSON logging with default sampling
INFERADB__LEDGER__LOG_FORMAT=json
INFERADB__LEDGER__LOGGING__ENABLED=true
INFERADB__LEDGER__LOGGING__VIP_ORGANIZATIONS=1001,1002

# With OTLP export to Jaeger
INFERADB__LEDGER__LOGGING__OTEL__ENABLED=true
INFERADB__LEDGER__LOGGING__OTEL__ENDPOINT=http://jaeger:4317

# High-volume: Reduce sampling for cost
INFERADB__LEDGER__LOGGING__SAMPLING__WRITE_RATE=0.01
INFERADB__LEDGER__LOGGING__SAMPLING__READ_RATE=0.001
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

## Storage Configuration

Snapshot and cache settings. Configured via TOML file or `UpdateConfig` RPC.

| TOML Key                        | Type     | Default     | Description                              |
| ------------------------------- | -------- | ----------- | ---------------------------------------- |
| `storage.cache_size_bytes`      | usize    | `268435456` | B+ tree page cache size (256 MB default) |
| `storage.hot_cache_snapshots`   | usize    | `3`         | Snapshots to keep in hot cache           |
| `storage.snapshot_interval`     | duration | `5m`        | Interval between automatic snapshots     |
| `storage.compression_level`     | i32      | `3`         | zstd compression level (1–22)            |

**Compression level trade-offs**: Level 3 (default) balances speed and ratio well for typical workloads (~3–4x compression). Higher levels (6–10) yield marginally better compression at increasing CPU cost. Levels above 10 are rarely worthwhile for Ledger's data patterns.

## Client Sequence Eviction

Controls how expired client sequence entries are purged from the replicated state. These entries enable cross-failover idempotency deduplication within the TTL window.

| TOML Key                                           | Type | Default | Description                                        |
| -------------------------------------------------- | ---- | ------- | -------------------------------------------------- |
| `raft.client_sequence_eviction.eviction_interval`  | u64  | `1000`  | Evict when `last_applied.index % interval == 0`    |
| `raft.client_sequence_eviction.ttl_seconds`         | i64  | `86400` | Entry TTL in seconds (24 hours default)            |

**Eviction is deterministic**: triggered by the Raft log index, ensuring all replicas evict identical entries at the same point. Lower `eviction_interval` means more frequent checks (slightly more CPU per apply cycle). Higher values mean entries may live slightly beyond TTL.

### TOML Example

```toml
[storage]
cache_size_bytes = 536870912  # 512 MB
compression_level = 3
snapshot_interval = "5m"

[raft.client_sequence_eviction]
eviction_interval = 1000
ttl_seconds = 86400
```

## Events Configuration

Audit event logging and external ingestion. See [Events Operations Guide](events.md) for architecture and event catalog.

### Event Logging

| Variable                                             | Type | Default  | Description                         |
| ---------------------------------------------------- | ---- | -------- | ----------------------------------- |
| `INFERADB__LEDGER__EVENTS__ENABLED`                  | bool | `true`   | Master switch for event logging     |
| `INFERADB__LEDGER__EVENTS__DEFAULT_TTL_DAYS`         | u32  | `90`     | TTL in days (1–3650)                |
| `INFERADB__LEDGER__EVENTS__MAX_DETAILS_SIZE_BYTES`   | u32  | `4096`   | Max details map size (256–65536)    |
| `INFERADB__LEDGER__EVENTS__SYSTEM_LOG_ENABLED`       | bool | `true`   | Enable system-scope event logging   |
| `INFERADB__LEDGER__EVENTS__ORGANIZATION_LOG_ENABLED` | bool | `true`   | Enable org-scope event logging      |
| `INFERADB__LEDGER__EVENTS__MAX_SNAPSHOT_EVENTS`      | u64  | `100000` | Max apply-phase events in snapshots |

### Ingestion

| Variable                                                            | Type | Default                | Description                       |
| ------------------------------------------------------------------- | ---- | ---------------------- | --------------------------------- |
| `INFERADB__LEDGER__EVENTS__INGESTION__INGEST_ENABLED`               | bool | `true`                 | Master switch for external ingest |
| `INFERADB__LEDGER__EVENTS__INGESTION__ALLOWED_SOURCES`              | Vec  | `["engine","control"]` | Allow-list of source services     |
| `INFERADB__LEDGER__EVENTS__INGESTION__MAX_INGEST_BATCH_SIZE`        | u32  | `500`                  | Max events per IngestEvents call  |
| `INFERADB__LEDGER__EVENTS__INGESTION__INGEST_RATE_LIMIT_PER_SOURCE` | u32  | `10000`                | Events/sec per source service     |

### TOML Example

```toml
[events]
enabled = true
default_ttl_days = 90
max_details_size_bytes = 4096

[events.ingestion]
ingest_enabled = true
allowed_sources = ["engine", "control"]
max_ingest_batch_size = 500
```

Runtime-updatable fields (via `UpdateConfig` RPC): `enabled`, `default_ttl_days`, `system_log_enabled`, `organization_log_enabled`, `ingest_enabled`, `ingest_rate_limit_per_source`.
