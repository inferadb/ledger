# Configuration

Complete reference for Ledger configuration via environment variables and CLI arguments.

## Quick Reference

| Variable                             | CLI            | Default       | Description                                  |
| ------------------------------------ | -------------- | ------------- | -------------------------------------------- |
| `INFERADB__LEDGER__LISTEN`           | `--listen`     | _(none)_     | TCP address for gRPC API                                    |
| `INFERADB__LEDGER__SOCKET`           | `--socket`     | _(none)_     | Unix domain socket path for gRPC API                        |
| `INFERADB__LEDGER__METRICS`          | `--metrics`    | _(disabled)_ | Prometheus metrics address                                  |
| `INFERADB__LEDGER__DATA`             | `--data`       | _(none)_     | Persistent data directory                                   |
| `INFERADB__LEDGER__DEV`              | `--dev`        | `false`      | Ephemeral file-backed storage at an auto-generated tempdir  |
| `INFERADB__LEDGER__JOIN`             | `--join`       | _(none)_     | Comma-separated seed addresses for discovery                |
| `INFERADB__LEDGER__REGION`           | `--region`     | `global`     | Geographic data residency region                            |
| `INFERADB__LEDGER__ADVERTISE`        | `--advertise`  | _(auto)_     | Address advertised to peers                                 |
| `INFERADB__LEDGER__MAX_CONCURRENT`   | `--concurrent` | `10000`      | Max concurrent requests                                     |
| `INFERADB__LEDGER__TIMEOUT`          | `--timeout`    | `30`         | Request timeout (seconds)                                   |
| `INFERADB__LEDGER__RATELIMIT`        | `--ratelimit`  | _(unset → honor `rate_limit.enabled`)_ | Optional override for server-side rate limiting |
| `INFERADB__LEDGER__VAULT_HIBERNATION` | `--vault-hibernation` | _(unset → honor `hibernation.enabled`)_ | Optional override for vault hibernation idle detector |
| `INFERADB__LEDGER__VAULT_METRICS`    | `--vault-metrics` | _(unset → honor `observability.vault_metrics_enabled`)_ | Optional override for per-vault Prometheus series |
| —                                    | `--log-format` | `auto`       | Log format (`text`/`json`/`auto`)                           |
| `INFERADB__LEDGER__LOGGING__ENABLED` | —              | `true`       | Enable request logging                                      |

Exactly one of `--data` or `--dev` is required at startup; supplying neither (or both) returns a CLI parse error before bootstrap. At least one of `--listen` or `--socket` must also be specified. CLI arguments take precedence over environment variables.

## Network Configuration

### Listen Address (TCP)

```bash
# Accept all interfaces (required for containers/remote access)
INFERADB__LEDGER__LISTEN=0.0.0.0:50051

# Specific interface (WireGuard)
INFERADB__LEDGER__LISTEN=10.0.0.1:50051
```

### Unix Domain Socket

```bash
# Local-only access without TCP overhead
INFERADB__LEDGER__SOCKET=/var/run/ledger.sock
```

Both `--listen` and `--socket` can be used simultaneously. At least one must be specified.

### Metrics Endpoint

```bash
# Enable Prometheus metrics
INFERADB__LEDGER__METRICS=0.0.0.0:9090
```

Disabled by default. Enable for production monitoring.

## Cluster Bootstrap

Ledger uses a `start`+`init` pattern. Nodes start their gRPC servers and wait for initialization.

### Single-Node

```bash
# Start the node
inferadb-ledger --listen 127.0.0.1:50051 --data /data

# Initialize the cluster (once)
inferadb-ledger init --host 127.0.0.1:50051
```

### Multi-Node

```bash
# Start the first node
inferadb-ledger --listen 0.0.0.0:50051 --data /data

# Start additional nodes with seed addresses
inferadb-ledger --listen 0.0.0.0:50051 --data /data --join node1:50051

# Initialize the cluster (once, from any machine)
inferadb-ledger init --host node1:50051
```

### Adding Nodes to an Existing Cluster

```bash
inferadb-ledger --listen 0.0.0.0:50051 --data /data --join node1:50051,node2:50051
```

The `--join` flag accepts a comma-separated list of seed addresses. The new node connects to the seeds, discovers the cluster, and joins as a learner.

## Storage

Exactly one of `--data <path>` or `--dev` must be supplied at startup. Supplying neither (or both) returns a clear CLI parse error before bootstrap — there is no implicit fallback.

### Persistent (`--data <path>`)

```bash
# CLI
inferadb-ledger --data /var/lib/ledger ...

# Environment variable
INFERADB__LEDGER__DATA=/var/lib/ledger inferadb-ledger ...
```

Contents:

- `node_id` - Persistent node identifier (Snowflake ID)
- `raft/` - Raft log and snapshots
- `state/` - State machine data

This is the only supported mode for production.

### Ephemeral (`--dev`)

```bash
# CLI
inferadb-ledger --dev ...

# Environment variable
INFERADB__LEDGER__DEV=true inferadb-ledger ...
```

`--dev` runs the server against an auto-generated tempdir under `std::env::temp_dir()` (e.g. `/tmp/ledger-{snowflake}`). Useful for:

- Local development
- Integration tests
- CI/CD pipelines

`--dev` is **not** an in-memory mode — the storage backend is the same `FileBackend` used in production. Only the path lifetime differs: data is lost on process exit, on tempdir purge (most Linux distros recycle `/tmp` on reboot via `tmpfiles.d`), and on every fresh launch (each restart picks a new tempdir suffix, so the previous run's directory is orphaned). Production deployments must use `--data <path>` instead.

`--data` and `--dev` are mutually exclusive — clap rejects the invocation at parse time when both are supplied.

## Batching

Controls Raft write batching for throughput vs latency trade-off. The `BatchWriter` coalesces concurrent write proposals into a single Raft proposal so the WAL fsync amortizes across many writes.

Batching is configured via `NodeConfig.batching` (set at startup) plus `RuntimeConfig` (via the `UpdateConfig` admin RPC for the runtime-tunable fields). The `BatchWriter` is **active by default**.

| Parameter        | Default | Description                                                            |
| ---------------- | ------- | ---------------------------------------------------------------------- |
| `max_batch_size` | 2000    | Upper bound on writes coalesced into one Raft proposal / one WAL fsync |
| `batch_timeout`  | 10ms    | Max wait before flushing a partial batch (caps single-client latency)  |

Internal polling cadence (`tick_interval`, not operator-facing) is derived as `batch_timeout / 2`, clamped to `[1ms, 10ms]`.

### Tuning trade-offs

- **Larger `max_batch_size`** — more proposals amortize one fsync under concurrent load. Upper bound is a memory / single-proposal-payload cap; the tail arrives once the timer fires.
- **Shorter `batch_timeout`** — tighter per-request latency floor for single-client workloads, at the cost of smaller batches under concurrency.
- **Longer `batch_timeout`** — larger batches, higher throughput under concurrency, wider latency tail under isolated / single-client load.

The current defaults are tuned for throughput: `concurrent-writes @ 32` measures at 1,350 ops/s (p50 23ms / p99 42ms). Smaller values such as `max_batch_size=100` / `batch_timeout=2ms` coalesce too few proposals to amortize the WAL fsync. Timer-only flushing is the only policy — an earlier `eager_commit` knob (flush on queue drain) defeated batching under concurrent load, because clients wait on prior responses before submitting and the queue drains every tick.

### UpdateConfig example — widen the batch timeout on write-heavy workloads

```bash
grpcurl -plaintext -d '{
  "config_json": "{\"batching\":{\"max_batch_size\":1000,\"batch_timeout\":\"20ms\"}}"
}' localhost:50051 ledger.v1.AdminService/UpdateConfig
```

### Monitoring

- `ledger_batch_coalesce_total` — counter, increments once per flushed batch.
- `ledger_batch_coalesce_size` — histogram, entries per flushed batch. Sustained low values under load suggest `batch_timeout` is too short; sustained values at `max_batch_size` suggest raising the cap.
- `ledger_batch_flush_latency_seconds` — histogram, per-flush wall-clock time (Raft submit round-trip).
- `ledger_batch_queue_depth` — gauge per region, pending writes at tick entry. A sustained high depth indicates the WAL is the bottleneck.

The older `ledger_batch_eager_commits_total` and `ledger_batch_timeout_commits_total` metrics no longer exist; there is no separate counter by trigger (the "eager" path was removed). Dashboards referencing the removed metrics will show "No data" — see the [metrics reference](metrics.md) for the current set.

## Request Limits

```bash
# Allow 200 concurrent requests
INFERADB__LEDGER__MAX_CONCURRENT=200

# 60-second request timeout
INFERADB__LEDGER__TIMEOUT=60
```

## Rate Limiting

Three-tier token-bucket admission control. **Disabled by default — opt in by setting `rate_limit.enabled = true` in YAML, or with `--ratelimit` at startup.**

| Variable                       | CLI           | Default | Description                                  |
| ------------------------------ | ------------- | ------- | -------------------------------------------- |
| `INFERADB__LEDGER__RATELIMIT`  | `--ratelimit` | _(unset → honor `rate_limit.enabled`)_ | Optional startup override for server-side throttling |

The canonical master switch is `rate_limit.enabled` (defaults to `false`). The `--ratelimit` CLI flag is an optional **`Option<bool>` override**:

| Operator input | Effective `enabled` |
| -------------- | ------------------- |
| flag absent (default) | honors `rate_limit.enabled` (canonical inner field) |
| `--ratelimit` (bare) | `true` — forces limiter on |
| `--ratelimit=true` / `--ratelimit true` | `true` |
| `--ratelimit=false` | `false` — forces limiter off |

When the resolved value is `false`, every admission check fast-paths through a single relaxed atomic load — no token-bucket math, no per-call overhead, no rejections. The `client_burst`, `organization_burst`, etc. knobs below are still loaded into the runtime config, but they have no effect until the master switch is flipped on.

### Why is rate limiting opt-in?

Surprise default throttling combined with SDK silent retry is a classic debugging trap. The SDK retries `ResourceExhausted` transparently with backoff, so a request that's actually being throttled looks like I/O wait in flamegraphs while the SDK transparently retries. Past perf investigations spent multiple weeks chasing phantom bottlenecks (HTTP/2 flow control, checkpoint thresholds, apply-pipeline parallelism) before the actual cause turned out to be the default per-client throttle hitting the test harness.

InferaDB Ledger's [trust posture](#network-configuration) — services run behind WireGuard VPN, callers are authenticated peers — means the threat model already trusts callers. Rate limiting in that environment primarily protects against runaway-client bugs rather than malicious traffic; the operators who know they have a known-bad caller know when to enable it. Production deployments that need DoS protection or per-tenant SLO enforcement opt in deliberately with thresholds tuned to the workload.

### Enabling rate limiting

```bash
# Enable with built-in defaults
inferadb-ledger --ratelimit --listen 0.0.0.0:50051 --data /data

# Or via env var
INFERADB__LEDGER__RATELIMIT=true inferadb-ledger --listen 0.0.0.0:50051 --data /data
```

Once enabled, the limiter applies the default thresholds below (or values supplied via `UpdateConfig`).

| Field                    | Default  | Unit       | Rationale                                                                                        |
| ------------------------ | -------- | ---------- | ------------------------------------------------------------------------------------------------ |
| `enabled`                | `false`  | bool       | Master opt-in switch. Canonical source of truth; `--ratelimit` is an optional startup override.    |
| `client_burst`           | `10000`  | tokens     | Per-client bucket capacity; bias-toward-throughput default. Tune down to enforce per-caller cap. |
| `client_rate`            | `10000`  | requests/s | Per-client sustained refill.                                                                     |
| `organization_burst`     | `100000` | tokens     | Per-organization bucket capacity across all callers in the org.                                  |
| `organization_rate`      | `100000` | requests/s | Per-organization sustained refill.                                                               |
| `backpressure_threshold` | `10000`  | proposals  | Pending-Raft-proposal threshold above which all new requests are rejected globally.              |

The thresholds default to high-burst values that are effectively inert for any reasonable single-tenant workload — they exist as a sensible baseline for opt-in callers, not as a per-tenant SLO surface. **Multi-tenant production deployments should tune these down via `UpdateConfig` to match the SLO model** for each tenant class (see [slo.md](slo.md)).

### UpdateConfig example — flip on with multi-tenant tuning

```bash
grpcurl -plaintext -d '{
  "config_json": "{\"rate_limit\":{\"enabled\":true,\"client_burst\":500,\"client_rate\":250.0,\"organization_burst\":5000,\"organization_rate\":2500.0,\"backpressure_threshold\":1000}}"
}' localhost:50051 ledger.v1.AdminService/UpdateConfig
```

### UpdateConfig example — disable at runtime

```bash
grpcurl -plaintext -d '{
  "config_json": "{\"rate_limit\":{\"enabled\":false}}"
}' localhost:50051 ledger.v1.AdminService/UpdateConfig
```

Flipping `enabled` `true → false → true` works without restart; the underlying token-bucket state survives the disabled window so re-enabling doesn't reset every caller's quota. Validation enforces `> 0` on every numeric field and caps (`client_*` ≤ 1,000,000; `organization_*` ≤ 10,000,000) — only `enabled` controls whether the values take effect.

## Vault Hibernation

Idle-vault Raft groups transition to a `Dormant` lifecycle state when they have not seen activity in `idle_secs`. **Disabled by default — opt in by setting `hibernation.enabled = true` in YAML, or with `--vault-hibernation` at startup.**

| Variable                              | CLI                   | Default | Description                                       |
| ------------------------------------- | --------------------- | ------- | ------------------------------------------------- |
| `INFERADB__LEDGER__VAULT_HIBERNATION` | `--vault-hibernation` | _(unset → honor `hibernation.enabled`)_ | Optional startup override |

The canonical master switch is `hibernation.enabled` (defaults to `false`). The `--vault-hibernation` CLI flag is an optional **`Option<bool>` override** with the same shape as `--ratelimit`:

| Operator input | Effective `enabled` |
| -------------- | ------------------- |
| flag absent (default) | honors `hibernation.enabled` |
| `--vault-hibernation` (bare) | `true` — activates the idle detector |
| `--vault-hibernation=true` / `--vault-hibernation true` | `true` |
| `--vault-hibernation=false` | `false` — forces the detector off |

When activated, the [`RaftManager`](https://docs.rs/inferadb-ledger-raft) runs an idle-detector task that transitions per-vault Raft groups to `Dormant` after they have not seen activity in `idle_secs` (default 5 min). The next request to a dormant vault wakes it back to `Active`. Hibernation is opt-in to keep the disciplined "no surprise throttling" default model — operators must accept the wake-time budget consciously.

```yaml
# Example YAML (recommended): operators opt in via the canonical inner field.
hibernation:
  enabled: true
  idle_secs: 300            # 5 minutes
  max_warm: 10000           # active-vault budget per node
  wake_threshold_ms: 100    # p99 wake budget (observability hint)
  scan_interval_secs: 30    # idle detector cadence
```

Hibernation tuning knobs are restart-only — there is no `UpdateConfig` arm for `hibernation` today. Flip the master switch from the CLI for ad-hoc rollouts:

```bash
inferadb-ledger --vault-hibernation --listen 0.0.0.0:50051 --data /data
```

## Per-Vault Prometheus Metrics

Per-vault metrics emit a fresh `vault_id` label set per metric, multiplying time-series count by the number of vaults per organization. **Disabled by default — opt in by setting `observability.vault_metrics_enabled = true` in YAML, or with `--vault-metrics` at startup.**

| Variable                          | CLI               | Default | Description                                       |
| --------------------------------- | ----------------- | ------- | ------------------------------------------------- |
| `INFERADB__LEDGER__VAULT_METRICS` | `--vault-metrics` | _(unset → honor `observability.vault_metrics_enabled`)_ | Optional startup override |

The canonical master switch is `observability.vault_metrics_enabled` (defaults to `false`). The `--vault-metrics` CLI flag is an optional **`Option<bool>` override** with the same shape as `--ratelimit`:

| Operator input | Effective `vault_metrics_enabled` |
| -------------- | --------------------------------- |
| flag absent (default) | honors `observability.vault_metrics_enabled` |
| `--vault-metrics` (bare) | `true` — emits per-vault series |
| `--vault-metrics=true` / `--vault-metrics true` | `true` |
| `--vault-metrics=false` | `false` — suppresses per-vault series |

Per-vault metrics are **restart-only**. Flipping the gate at runtime would change the time-series shape mid-scrape and confuse dashboards, so the resolved value is set exactly once at startup. Organization-level rollups (`org_apply_throughput_ops_total`, `org_active_vault_count`) are always-on regardless of this flag.

```yaml
# Example YAML: opt in to per-vault series for environments where the
# extra cardinality is acceptable (staging, small clusters, single-tenant deploys).
observability:
  vault_metrics_enabled: true
```

Verify your TSDB sizing covers the additional `vault_id` cardinality before deploying with this enabled in production: estimated cardinality is `(active vault count × number of per-vault metric families)` on top of the always-on org-level rollups.

## Logging

Request logging emits one comprehensive JSON event per request for queryable observability. See [Logging](../how-to/logging.md) for complete field reference and query cookbook.

### Log Format

`auto` is the default — the server detects whether stdout is a TTY and picks `text` for interactive runs, `json` otherwise. Set explicitly when you need one format regardless of context (e.g., containerized deployments that pipe logs to a collector).

```bash
# Auto-detect: text when stdout is a TTY, JSON otherwise (default)
INFERADB__LEDGER__LOG_FORMAT=auto

# Force JSON (production / log-collector deployments)
INFERADB__LEDGER__LOG_FORMAT=json

# Force text (local development, readable tailing)
INFERADB__LEDGER__LOG_FORMAT=text
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
      INFERADB__LEDGER__JOIN: "ledger-1:50051,ledger-2:50051"
    volumes:
      - ledger-data:/data
    ports:
      - "50051:50051"
      - "9090:9090"
```

After all containers start, run `inferadb-ledger init --host ledger-1:50051` once.

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
  INFERADB__LEDGER__JOIN: "ledger-headless.default.svc.cluster.local"
```

### CLI with All Options

```bash
inferadb-ledger \
  --listen 0.0.0.0:50051 \
  --socket /var/run/ledger.sock \
  --metrics 0.0.0.0:9090 \
  --data /var/lib/ledger \
  --join node1:50051,node2:50051 \
  --region us-east-va \
  --advertise node3:50051 \
  --concurrent 100 \
  --timeout 30
```

## Storage Configuration

Snapshot and cache settings. Configured via `UpdateConfig` RPC at runtime.

| Field                 | Type     | Default     | Description                              |
| --------------------- | -------- | ----------- | ---------------------------------------- |
| `cache_size_bytes`    | usize    | `268435456` | B+ tree page cache size (256 MB default) |
| `hot_cache_snapshots` | usize    | `3`         | Snapshots to keep in hot cache           |
| `snapshot_interval`   | duration | `5m`        | Interval between automatic snapshots     |
| `compression_level`   | i32      | `3`         | zstd compression level (1–22)            |

**Compression level trade-offs**: Level 3 (default) balances speed and ratio well for typical workloads (~3–4x compression). Higher levels (6–10) yield marginally better compression at increasing CPU cost. Levels above 10 are rarely worthwhile for Ledger's data patterns.

## State-DB Checkpointing

Controls the per-region `StateCheckpointer` background task that drives `Database::sync_state()` on every regional DB. Writes are WAL-durable on response; DB materialization is amortized via this checkpointer. See [durability.md](../architecture/durability.md) for the full contract.

The checkpointer syncs **four** databases per region: `state.db` (entity tables), `raft.db` (Raft applied state), `blocks.db` (blockchain archive), and `events.db` (audit events, when the region is configured to own an events shard). The dirty-page trigger reads `max()` across all four DBs, so an ingest-heavy workload that dirties `events.db` while `state.db` stays clean still fires the checkpoint. Checkpoint duration scales roughly linearly with how many of the four are dirty at tick time — expect p99 `ledger_state_checkpoint_duration_seconds` to scale accordingly on write-heavy workloads. Sync is concurrent via `tokio::join!`; accumulators advance only when every configured DB's sync succeeded.

| Field                   | Type | Default  | Description                                               |
| ----------------------- | ---- | -------- | --------------------------------------------------------- |
| `interval_ms`           | u64  | `2000`   | Time trigger — fire at least this often (50–60,000 ms)    |
| `applies_threshold`     | u64  | `50000`  | Apply-count trigger — fire after N entries applied (≥1)   |
| `dirty_pages_threshold` | u64  | `100000` | Dirty-page trigger — fire when cache dirty-pages ≥ N (≥1) |

The three triggers are OR'd — the checkpointer fires on whichever threshold hits first. Checkpoints coalesce across concurrent callers by `last_synced_snapshot_id`, so forced syncs (snapshot / backup / graceful shutdown) never duplicate work an in-flight checkpoint already covered.

**Tuning trade-offs**:

- **Tighter thresholds** (lower `interval_ms` / `applies_threshold`) — shorter post-crash replay window; more fsync pressure on the disk.
- **Looser thresholds** — less fsync churn; post-crash WAL replay runs longer at startup.
- **`dirty_pages_threshold`** is the backpressure safety valve. If applies outrun the time/apply triggers, this bounds the in-memory page cache growth.

Do not set any threshold to 0 — validation rejects it. If you want to effectively disable a trigger, set it near its maximum (`u64::MAX` for count-based, `60_000` for `interval_ms`).

**Runtime reconfiguration**: all three fields are hot-reloadable via the `UpdateConfig` RPC. Changes take effect on the checkpointer's next wake-up (≤ 1 second after the update).

```bash
grpcurl -plaintext -d '{
  "config_json": "{\"state_checkpoint\":{\"interval_ms\":250,\"applies_threshold\":2000,\"dirty_pages_threshold\":5000}}"
}' localhost:50051 ledger.v1.AdminService/UpdateConfig
```

Monitor the effect via `ledger_state_checkpoints_total{trigger}`, `ledger_state_checkpoint_duration_seconds`, `ledger_state_dirty_pages`, and `ledger_state_applies_since_checkpoint` — see [metrics.md § State Durability](metrics.md#state-durability) for the full metric set.

## Handler-Phase Event Batching

Controls the in-memory queue + background `EventFlusher` that amortizes `events.db` fsyncs on the handler-phase emission path. Handlers (RPC, admin, background-job, saga) call `EventHandle::record_handler_event`, which enqueues an `EventEntry`; the flusher drains the queue on a time / size / shutdown trigger and commits the drained batch. Durability is **batched-durable within the StateCheckpointer cadence**, not per-emission — see [durability.md § Handler-phase event flush window](../architecture/durability.md#handler-phase-event-flush-window) for the full contract and the clean-shutdown drain semantics.

| Field                  | Default | Description                                                                                           | Runtime-reconfig?         |
| ---------------------- | ------- | ----------------------------------------------------------------------------------------------------- | ------------------------- |
| `enabled`              | `true`  | Master switch. Set `false` to restore per-emission fsync (strict synchronous durability).             | Yes                       |
| `flush_interval_ms`    | `100`   | Time trigger — flush at least this often. Valid `[1, 60_000]`.                                        | Yes                       |
| `flush_size_threshold` | `1000`  | Size trigger — flush when queue depth reaches N entries. Must be `>= 1`.                              | Yes                       |
| `queue_capacity`       | `10000` | Bounded `mpsc` channel capacity. Capacity is fixed at channel construction.                           | **No — restart required** |
| `overflow_behavior`    | `drop`  | `drop` or `block` when the queue is full. `drop` matches the best-effort-on-failure contract.         | Yes                       |
| `drain_batch_max`      | `500`   | Per-flush drain cap, bounds how long each flush txn holds `events.db`'s write mutex.                  | Yes                       |

Triggers are OR'd — the flusher fires on whichever hits first. The shutdown trigger is invoked explicitly by `GracefulShutdown` Phase 5b via `EventHandle::flush_for_shutdown(5s)` and is not a time/size trigger variant.

**Tuning trade-offs**:

- **Tighter window** (smaller `flush_interval_ms` / `flush_size_threshold`) — narrower loss window on SIGKILL, more fsyncs per second on the events.db disk.
- **Looser window** (larger `flush_interval_ms`) — fewer fsyncs, wider loss window. Disabling batching entirely via `enabled=false` reverts to one fsync per emission (strict synchronous bound).
- **`queue_capacity`** is the backpressure safety valve. If the producer rate exceeds the flusher's drain rate, a bounded queue fills up; `overflow_behavior` then chooses between dropping (lossy, fast) or blocking the producer (lossless, slow).

### Tightening the flush window under latency-sensitive workloads

```bash
grpcurl -plaintext -d '{
  "config_json": "{\"event_writer_batch\":{\"flush_interval_ms\":50,\"flush_size_threshold\":500}}"
}' localhost:50051 ledger.v1.AdminService/UpdateConfig
```

### Disabling batching (strict synchronous durability)

```bash
grpcurl -plaintext -d '{
  "config_json": "{\"event_writer_batch\":{\"enabled\":false}}"
}' localhost:50051 ledger.v1.AdminService/UpdateConfig
```

With `enabled = false`, every `record_handler_event` call fsyncs before returning. Expect throughput on the event-emission hot path to fall to a ~50 ops/s ceiling on `concurrent-writes @ 32` (the fsync serializes on `events.db`'s write-txn mutex). Use this only when the audit trail's per-emission durability matters more than the ~42% active-CPU cost on the event path.

### Monitoring

Six metrics track flusher behavior. See [metrics.md § Handler Event Flush](metrics.md#handler-event-flush) for full label sets and suggested alerts.

- `ledger_event_flush_triggers_total{trigger}` — flushes by trigger (`time`, `size`, `shutdown`).
- `ledger_event_flush_duration_seconds` — per-flush fsync latency.
- `ledger_event_flush_queue_depth` — queue depth sampled at flush entry.
- `ledger_event_flush_entries_per_flush` — entries drained per flush.
- `ledger_event_flush_failures_total` — failed flushes (disk full, IO error, corrupted page).
- `ledger_event_overflow_total{cause}` — dropped events by cause (`queue_full`, `shutdown_timeout`, `channel_closed`).

## Client Sequence Eviction

Controls how expired client sequence entries are purged from the replicated state. These entries enable cross-failover idempotency deduplication within the TTL window.

| Field               | Type | Default | Description                                     |
| ------------------- | ---- | ------- | ----------------------------------------------- |
| `eviction_interval` | u64  | `1000`  | Evict when `last_applied.index % interval == 0` |
| `ttl_seconds`       | i64  | `86400` | Entry TTL in seconds (24 hours default)         |

**Eviction is deterministic**: triggered by the Raft log index, ensuring all replicas evict identical entries at the same point. Lower `eviction_interval` means more frequent checks (slightly more CPU per apply cycle). Higher values mean entries may live slightly beyond TTL.

### UpdateConfig RPC Example

```bash
grpcurl -plaintext -d '{
  "config_json": "{\"compaction\":{\"target_fill_ratio\":0.85}}"
}' localhost:50051 ledger.v1.AdminService/UpdateConfig
```

Use `GetConfig` to inspect current runtime configuration:

```bash
grpcurl -plaintext localhost:50051 ledger.v1.AdminService/GetConfig
```

## JWT Configuration

Token issuance and signing key management settings.

| Variable                                          | Default             | Description                            |
| ------------------------------------------------- | ------------------- | -------------------------------------- |
| `INFERADB__LEDGER__JWT__ISSUER`                   | `"inferadb"`        | JWT `iss` claim value                  |
| `INFERADB__LEDGER__JWT__SESSION_ACCESS_TTL_SECS`  | `1800` (30 min)     | User session access token TTL          |
| `INFERADB__LEDGER__JWT__SESSION_REFRESH_TTL_SECS` | `1209600` (14 days) | User session refresh token TTL         |
| `INFERADB__LEDGER__JWT__VAULT_ACCESS_TTL_SECS`    | `900` (15 min)      | Vault access token TTL                 |
| `INFERADB__LEDGER__JWT__VAULT_REFRESH_TTL_SECS`   | `3600` (1 hour)     | Vault refresh token TTL                |
| `INFERADB__LEDGER__JWT__CLOCK_SKEW_SECS`          | `30`                | Clock skew leeway for token validation |
| `INFERADB__LEDGER__JWT__KEY_ROTATION_GRACE_SECS`  | `14400` (4 hours)   | Rotated key validation window          |

**Validation constraints**: All TTLs > 0, refresh TTL > access TTL for each pair, clock skew < minimum access TTL, grace period > 0.

**Runtime reconfiguration**: JWT TTL values are updatable at runtime via `UpdateConfig` RPC without restart. Changes take effect for newly issued tokens; existing tokens retain their original expiry.

## Events Configuration

Audit event logging and external ingestion. See [Events Operations Guide](../architecture/events.md) for architecture and event catalog.

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

### Runtime Reconfiguration

These fields are updatable at runtime via the `UpdateConfig` RPC without restart:

- `enabled`, `default_ttl_days`, `system_log_enabled`, `organization_log_enabled`
- `ingest_enabled`, `ingest_rate_limit_per_source`

```bash
grpcurl -plaintext -d '{
  "config_json": "{\"events\":{\"default_ttl_days\":30,\"ingest_rate_limit_per_source\":5000}}"
}' localhost:50051 ledger.v1.AdminService/UpdateConfig
```
