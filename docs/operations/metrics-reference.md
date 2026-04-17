# Metrics Reference

Complete reference for Prometheus metrics exposed by Ledger.

## What's NOT a Metric

Not every observable value belongs in Prometheus. High-cardinality fields are emitted as structured log fields or wide-event attributes — not metric labels.

| Field                                   | Where it lives             | Reason                                      |
| --------------------------------------- | -------------------------- | ------------------------------------------- |
| `organization_id` / `organization_slug` | Log line, wide event       | Unbounded cardinality (one series per org)  |
| `vault_id` / `vault_slug`               | Log line, wide event       | Unbounded cardinality                       |
| `user_id`                               | Log line                   | Unbounded cardinality                       |
| `region`                                | Log line, agent-level tag  | Added by Datadog/Prometheus agent, not code |
| `request_id` / `correlation_id`         | Log line only              | Unique per request — never a label          |
| Continuous numeric values               | Histogram or log attribute | Raw values explode cardinality              |

Any code that attempts to add these as Prometheus labels will be rejected by the cardinality guard (see [Cardinality Guard](#cardinality-guard)).

## ObservabilityContext Architecture

Metrics are emitted through two context types that gate all Prometheus writes:

- **`RequestContext`** — created at gRPC handler entry, dropped on return. `Drop` emits `ledger_grpc_requests_total` and `ledger_grpc_request_latency_seconds` (plus the SLI histogram) automatically for every RPC. Individual handlers do not call metrics APIs directly.
- **`JobContext`** — created at each background job cycle start, dropped on cycle end. `Drop` emits `ledger_background_job_runs_total`, `ledger_background_job_duration_seconds`, and (if set) `ledger_background_job_items_processed_total`. Job implementations call `ctx.set_items_processed(n)` and otherwise do no metric work.

Both contexts route emission through `CardinalityTracker` before writing to Prometheus. This ensures the cardinality guard fires before a high-cardinality label set reaches the registry.

## Endpoint

```bash
curl http://localhost:9090/metrics
```

Enable with `--metrics` or `INFERADB__LEDGER__METRICS`:

```bash
INFERADB__LEDGER__METRICS=0.0.0.0:9090 inferadb-ledger --single
```

## Naming Conventions

All metrics follow the pattern: `ledger_{subsystem}_{name}_{unit}`

| Suffix     | Type              | Example                               |
| ---------- | ----------------- | ------------------------------------- |
| `_total`   | Counter           | `ledger_grpc_requests_total`          |
| `_seconds` | Histogram         | `ledger_grpc_request_latency_seconds` |
| `_bytes`   | Histogram/Counter | `ledger_storage_bytes_written_total`  |
| (none)     | Gauge             | `ledger_active_connections`           |

## gRPC Request Metrics

All 13 gRPC services emit unified request metrics via `RequestContext::drop()`. There are no per-service latency histograms — use `ledger_grpc_request_latency_seconds{service=..., method=...}` for any service-level breakdown.

| Metric                                | Type      | Labels                        | Description                   |
| ------------------------------------- | --------- | ----------------------------- | ----------------------------- |
| `ledger_grpc_requests_total`          | Counter   | `service`, `method`, `status` | gRPC requests by endpoint     |
| `ledger_grpc_request_latency_seconds` | Histogram | `service`, `method`           | gRPC request latency          |
| `ledger_grpc_sli_latency_seconds`     | Histogram | `service`, `method`, `status` | Per-RPC latency (SLI buckets) |
| `ledger_active_connections`           | Gauge     | -                             | Active gRPC connections       |

**Labels:**

- `service`: `ReadService`, `WriteService`, `AdminService`, `OrganizationService`, `VaultService`, `UserService`, `AppService`, `TokenService`, `InvitationService`, `EventsService`, `HealthService`, `DiscoveryService`, `RaftService`
- `method`: RPC method name
- `status`: gRPC status code string

**SLI histogram buckets**: 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0 seconds.

## Raft Consensus

| Metric                                         | Type      | Labels | Description                          |
| ---------------------------------------------- | --------- | ------ | ------------------------------------ |
| `inferadb_ledger_raft_proposals_total`         | Counter   | -      | Total Raft proposals submitted       |
| `inferadb_ledger_raft_proposals_pending`       | Gauge     | -      | Proposals awaiting commit            |
| `inferadb_ledger_raft_apply_latency_seconds`   | Histogram | -      | Log entry apply latency              |
| `inferadb_ledger_raft_commit_index`            | Gauge     | -      | Current commit index                 |
| `inferadb_ledger_raft_term`                    | Gauge     | -      | Current Raft term                    |
| `inferadb_ledger_raft_is_leader`               | Gauge     | -      | 1 if leader, 0 otherwise             |
| `inferadb_ledger_raft_proposal_timeouts_total` | Counter   | -      | Proposals that exceeded the deadline |

### Key Indicators

```promql
# Leader status (should be exactly 1 across cluster)
sum(inferadb_ledger_raft_is_leader)

# Proposal backlog (high = disk/network bottleneck)
inferadb_ledger_raft_proposals_pending > 50

# Term changes (high = election instability)
rate(inferadb_ledger_raft_term[5m])
```

## Leader Transfer

Emitted by `AdminService.TransferLeadership` and the graceful-shutdown path.

| Metric                                   | Type      | Labels   | Description                                        |
| ---------------------------------------- | --------- | -------- | -------------------------------------------------- |
| `ledger_leader_transfers_total`          | Counter   | `status` | Transfer attempts (`success`, `failed`, `timeout`) |
| `ledger_leader_transfer_latency_seconds` | Histogram | -        | End-to-end leader-transfer duration                |
| `ledger_trigger_elections_total`         | Counter   | `result` | `TriggerElection` RPCs (`accepted`, `rejected`)    |

## Events

Emitted by `EventsService` and the event writer pipeline.

| Metric                                    | Type      | Labels                        | Description                                 |
| ----------------------------------------- | --------- | ----------------------------- | ------------------------------------------- |
| `ledger_event_writes_total`               | Counter   | `emission`, `scope`, `action` | Events written to events.db                 |
| `ledger_events_ingest_total`              | Counter   | `source`                      | IngestEvents RPC calls accepted             |
| `ledger_events_ingest_duration_seconds`   | Histogram | -                             | IngestEvents end-to-end latency             |
| `ledger_events_ingest_rate_limited`       | Counter   | `source`                      | Ingestion requests rejected by rate limiter |
| `ledger_events_ingest_batch_size`         | Histogram | -                             | Events per IngestEvents call                |
| `ledger_events_gc_cycles_total`           | Counter   | -                             | `EventsGarbageCollector` cycles             |
| `ledger_events_gc_entries_deleted`        | Counter   | -                             | Expired events deleted                      |
| `ledger_events_gc_cycle_duration_seconds` | Histogram | -                             | Per-cycle GC duration                       |

## Onboarding Lifecycle

Counters around the user-onboarding state machine (email verification → registration). GC metrics for onboarding records are emitted via the `token_maintenance` background job — see [Background Jobs](#background-jobs).

| Metric                             | Type    | Labels            | Description                                                                                                                                         |
| ---------------------------------- | ------- | ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ledger_onboarding_requests_total` | Counter | `stage`, `status` | Onboarding stage outcomes (`stage` ∈ `initiate`, `verify`, `register`; `status` ∈ `success`, `failure`, `rate_limited`, `expired`, `totp_required`) |

## Organization Purge + Post-Erasure Compaction

Background jobs that reclaim soft-deleted organizations and evict plaintext from Raft logs after crypto-shredding. Cycle metrics are captured by the [Background Jobs](#background-jobs) family. The following counters track fault-specific outcomes not expressible via `result=failure`:

| Metric                                           | Type    | Labels              | Description                                                                                |
| ------------------------------------------------ | ------- | ------------------- | ------------------------------------------------------------------------------------------ |
| `ledger_org_purge_failures_total`                | Counter | `tier`, `exhausted` | Purge failures. `tier` ∈ `regional`, `global`; `exhausted=true` when retry budget is spent |
| `ledger_post_erasure_compaction_triggered_total` | Counter | `region`            | Proactive snapshot triggers after crypto-shredding, per Raft group                         |

## State Machine

| Metric                                          | Type      | Labels     | Description                          |
| ----------------------------------------------- | --------- | ---------- | ------------------------------------ |
| `inferadb_ledger_state_root_computations_total` | Counter   | `vault_id` | State root computations              |
| `inferadb_ledger_state_root_latency_seconds`    | Histogram | `vault_id` | State root computation time          |
| `ledger_dirty_buckets`                          | Gauge     | `vault_id` | Merkle buckets pending recomputation |

## Storage

| Metric                               | Type    | Labels | Description                |
| ------------------------------------ | ------- | ------ | -------------------------- |
| `ledger_storage_bytes_written_total` | Counter | -      | Total bytes written        |
| `ledger_storage_bytes_read_total`    | Counter | -      | Total bytes read           |
| `ledger_storage_operations_total`    | Counter | `op`   | Storage operations by type |

**Labels:**

- `op`: `read` or `write`

## Snapshots

| Metric                                    | Type      | Labels | Description            |
| ----------------------------------------- | --------- | ------ | ---------------------- |
| `ledger_snapshots_created_total`          | Counter   | -      | Snapshots created      |
| `ledger_snapshot_size_bytes`              | Histogram | -      | Snapshot size          |
| `ledger_snapshot_create_latency_seconds`  | Histogram | -      | Snapshot creation time |
| `ledger_snapshot_restore_latency_seconds` | Histogram | -      | Snapshot restore time  |

## Idempotency Cache

Idempotency cache behavior is now reported through the unified operations counter. The four individual cache metrics (`hits_total`, `misses_total`, `cache_size`, `evictions_total`) have been replaced.

| Metric                    | Type    | Labels   | Description                                             |
| ------------------------- | ------- | -------- | ------------------------------------------------------- |
| `ledger_operations_total` | Counter | `result` | Idempotency checks. `result` ∈ `hit`, `miss`, `evicted` |

### Key Indicators

```promql
# Cache hit ratio
rate(ledger_operations_total{result="hit"}[5m]) /
(rate(ledger_operations_total{result="hit"}[5m]) + rate(ledger_operations_total{result="miss"}[5m]))
```

## Batching

| Metric                               | Type      | Labels | Description                    |
| ------------------------------------ | --------- | ------ | ------------------------------ |
| `ledger_batch_coalesce_total`        | Counter   | -      | Batch coalesce events          |
| `ledger_batch_coalesce_size`         | Histogram | -      | Requests coalesced per batch   |
| `ledger_batch_flush_latency_seconds` | Histogram | -      | Batch flush duration           |
| `ledger_batch_eager_commits_total`   | Counter   | -      | Batches flushed on queue drain |
| `ledger_batch_timeout_commits_total` | Counter   | -      | Batches flushed on timeout     |

### Tuning Indicators

```promql
# Eager vs timeout commits ratio (higher eager = lower latency)
rate(ledger_batch_eager_commits_total[5m]) /
(rate(ledger_batch_eager_commits_total[5m]) + rate(ledger_batch_timeout_commits_total[5m]))
```

## Recovery

| Metric                          | Type    | Labels   | Description                             |
| ------------------------------- | ------- | -------- | --------------------------------------- |
| `ledger_recovery_success_total` | Counter | -        | Successful vault recoveries             |
| `ledger_recovery_failure_total` | Counter | `reason` | Failed recovery attempts                |
| `ledger_determinism_bug_total`  | Counter | -        | **CRITICAL**: Determinism bugs detected |

Note: `organization_id` and `vault_id` are not labels — they appear in the structured log event emitted alongside this metric.

### Critical Alert

```promql
# IMMEDIATE ATTENTION: Any increase indicates data integrity issue
ledger_determinism_bug_total > 0
```

## Learner Refresh

| Metric                                   | Type      | Labels                   | Description              |
| ---------------------------------------- | --------- | ------------------------ | ------------------------ |
| `ledger_learner_refresh_total`           | Counter   | `status`                 | Learner refresh attempts |
| `ledger_learner_refresh_latency_seconds` | Histogram | `status`                 | Refresh latency          |
| `ledger_learner_cache_stale_total`       | Counter   | -                        | Stale cache events       |
| `ledger_learner_voter_errors_total`      | Counter   | `voter_id`, `error_type` | Voter connection errors  |

## Token Service

| Metric                                    | Type      | Labels       | Description                            |
| ----------------------------------------- | --------- | ------------ | -------------------------------------- |
| `ledger_token_operations_total`           | Counter   | `op`         | Token operations by type               |
| `ledger_token_validation_latency_seconds` | Histogram | -            | Token validation latency               |
| `ledger_refresh_token_reuse_total`        | Counter   | -            | Refresh token reuse detections (theft) |
| `ledger_signing_key_transitions_total`    | Counter   | `from`, `to` | Signing key status transitions         |

**Labels:**

- `op`: `create_session`, `validate`, `refresh`, `revoke`, `revoke_all`, `create_vault_token`, `create_key`, `rotate_key`, `revoke_key`
- `from`/`to`: `active`, `rotated`, `revoked`

## Background Jobs

| Metric                                        | Type      | Labels          | Description                |
| --------------------------------------------- | --------- | --------------- | -------------------------- |
| `ledger_background_job_duration_seconds`      | Histogram | `job`           | Duration of each job cycle |
| `ledger_background_job_runs_total`            | Counter   | `job`, `result` | Total cycles executed      |
| `ledger_background_job_items_processed_total` | Counter   | `job`           | Work items processed       |

**Labels:**

- `job`: `auto_recovery`, `backup`, `block_compaction`, `btree_compaction`, `dek_rewrap`, `dependency_health`, `events_gc`, `hot_key_detector`, `integrity_scrub`, `invite_maintenance`, `learner_refresh`, `organization_purge`, `orphan_cleanup`, `post_erasure_compaction`, `resource_metrics`, `saga_orchestrator`, `state_root_verifier`, `token_maintenance`, `ttl_gc`, `user_retention` (20 jobs)
- `result`: `success`, `failure`

## Resource Saturation

| Metric                           | Type    | Labels     | Description            |
| -------------------------------- | ------- | ---------- | ---------------------- |
| `ledger_disk_total_bytes`        | Gauge   | `path`     | Total disk space       |
| `ledger_disk_available_bytes`    | Gauge   | `path`     | Available disk space   |
| `ledger_page_cache_hits_total`   | Counter | -          | Page cache hits        |
| `ledger_page_cache_misses_total` | Counter | -          | Page cache misses      |
| `ledger_btree_depth`             | Gauge   | `vault_id` | B-tree depth per vault |
| `ledger_btree_splits_total`      | Counter | -          | B-tree leaf splits     |

## Hot Key Detection

| Metric                          | Type    | Labels     | Description                |
| ------------------------------- | ------- | ---------- | -------------------------- |
| `ledger_hot_key_detected_total` | Counter | `vault_id` | Hot key detection events   |
| `ledger_hot_key_current_count`  | Gauge   | -          | Currently tracked hot keys |

## Rate Limiting

| Metric                             | Type    | Labels | Description                       |
| ---------------------------------- | ------- | ------ | --------------------------------- |
| `ledger_rate_limit_exceeded_total` | Counter | -      | Rate limit violations             |
| `ledger_rate_limit_queue_depth`    | Gauge   | -      | Pending proposals in rate limiter |

Note: `organization_id` is not a label. It appears in the structured log event emitted with each rate limit violation.

## Quota

| Metric                        | Type    | Labels     | Description      |
| ----------------------------- | ------- | ---------- | ---------------- |
| `ledger_quota_exceeded_total` | Counter | `resource` | Quota violations |

Note: `organization_id` is not a label. It appears in the structured log event emitted with each quota violation.

## Consensus Transport

Metrics emitted by the per-peer bounded send queue in Raft consensus transport.
Each registered peer has a queue of capacity 1024; on overflow, the oldest
message is dropped (Raft retransmits on the next heartbeat, so dropped
heartbeats are self-healing).

| Metric                                | Type      | Labels           | Description                                                                                                                                    |
| ------------------------------------- | --------- | ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `ledger_peer_send_queue_depth`        | Gauge     | `peer`           | Current outbound queue depth for the peer.                                                                                                     |
| `ledger_peer_send_drops_total`        | Counter   | `peer`, `reason` | Messages dropped from the queue. `reason` ∈ `queue_full` (capacity overflow), `task_shutdown` (peer removed or replaced).                      |
| `ledger_peer_send_latency_seconds`    | Histogram | `peer`           | Per-message send latency (enqueue-to-wire, dominated by gRPC call time).                                                                       |
| `ledger_peer_stream_reconnects_total` | Counter   | `peer`           | Bidi `Replicate` reconnect attempts (Phase 4). Non-zero during peer churn or network hiccups; exponential backoff caps at 5s between attempts. |

**Labels:**

- `peer`: Raw node ID (numeric string); cardinality bounded by cluster size
- `reason`: `queue_full` or `task_shutdown`

See the [consensus transport backpressure runbook](runbooks/consensus-transport-backpressure.md)
for symptom-to-cause mapping.

## Node Connection Registry

Metrics from the node-level `NodeConnectionRegistry`, which owns one gRPC
channel per peer node shared across all subsystems (consensus, forwarding,
discovery, admin). Replaces the previous per-region, per-subsystem channel
duplication.

| Metric                                | Type    | Labels          | Description                                                                                                                   |
| ------------------------------------- | ------- | --------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `ledger_node_connections_active`      | Gauge   | none            | Total peer connections currently in the registry.                                                                             |
| `ledger_node_connection_events_total` | Counter | `peer`, `event` | Lifecycle events. `event` ∈ `registered` (new entry), `unregistered` (explicit remove), `pruned` (membership change removed). |

**Labels:**

- `peer`: Raw node ID (numeric string); cardinality bounded by cluster size
- `event`: `registered`, `unregistered`, or `pruned`

See the [node connection registry runbook](runbooks/node-connection-registry.md)
for symptom-to-cause mapping.

## SDK Region Leader Cache

Metrics emitted by the SDK's regional leader cache, which routes requests
directly to the regional Raft leader to avoid gateway-side forwarding. The
cache implements stale-while-revalidate with soft/hard TTLs (30s/120s by
default) and single-flight coalescing for concurrent miss resolutions.

| Metric                                                   | Type    | Labels             | Description                                                                                                                                                                                                                                                                         |
| -------------------------------------------------------- | ------- | ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ledger_sdk_leader_cache_hits_total`                     | Counter | `region`           | Region-leader lookups that returned a fresh or stale-but-usable cached entry without triggering a resolve.                                                                                                                                                                          |
| `ledger_sdk_leader_cache_misses_total`                   | Counter | `region`           | Lookups where the cache was empty or past `hard_ttl`, forcing a `ResolveRegionLeader` RPC before the request could proceed.                                                                                                                                                         |
| `ledger_sdk_leader_cache_flaps_total`                    | Counter | `region`           | Resolves that returned a different leader endpoint than the previously cached one — indicates a leader change.                                                                                                                                                                      |
| `ledger_sdk_region_resolve_singleflight_coalesced_total` | Counter | `region`           | Concurrent resolvers that joined an in-flight resolve instead of launching their own RPC (thundering-herd defense).                                                                                                                                                                 |
| `ledger_sdk_region_resolve_stale_served_total`           | Counter | `region`           | Stale-but-usable entries served to a caller while a background refresh was kicked off.                                                                                                                                                                                              |
| `ledger_sdk_leader_watch_updates_total`                  | Counter | `region`           | Leader updates received over the `WatchLeader` stream (includes the initial state pushed on stream open).                                                                                                                                                                           |
| `ledger_sdk_leader_watch_reconnects_total`               | Counter | `region`           | `WatchLeader` stream reconnect attempts after error or server-initiated close. Exponential backoff caps at 30s between attempts.                                                                                                                                                    |
| `ledger_sdk_leader_stale_term_rejected_total`            | Counter | `region`, `source` | Cache writes rejected because the incoming Raft term is older than the currently-cached term. `source` ∈ `hint`, `watch`. Non-zero under message reordering; term-gating preserves monotonicity.                                                                                    |
| `ledger_sdk_redirect_retries_total`                      | Counter | `region`           | Number of retries triggered by a leader-redirect hint. Expected to be non-zero on cold start for cross-region writes; should trend toward zero on warm paths with `preferred_region` configured. See: [architecture-redirect-routing.md](runbooks/architecture-redirect-routing.md) |

**Labels:**

- `region`: Region identifier the cache entry belongs to

### Key Indicators

```promql
# Cache hit ratio (target: > 0.95 in steady state)
rate(ledger_sdk_leader_cache_hits_total[5m]) /
(rate(ledger_sdk_leader_cache_hits_total[5m]) + rate(ledger_sdk_leader_cache_misses_total[5m]))

# Leader flap rate (any sustained non-zero rate is actionable)
rate(ledger_sdk_leader_cache_flaps_total[5m])
```

See the [leader cache diagnosis runbook](runbooks/leader-cache-diagnosis.md)
for symptom-to-cause mapping.

## Alert Recommendations

### Critical (Page)

```yaml
- alert: LedgerDeterminismBug
  expr: ledger_determinism_bug_total > 0
  labels:
    severity: critical
  annotations:
    summary: "Determinism bug detected in vault"
    description: "Vault {{ $labels.vault_id }} has state divergence"

- alert: LedgerNoLeader
  expr: sum(inferadb_ledger_raft_is_leader) == 0
  for: 30s
  labels:
    severity: critical
  annotations:
    summary: "No Raft leader elected"
```

### Warning (Ticket)

```yaml
- alert: LedgerHighProposalBacklog
  expr: inferadb_ledger_raft_proposals_pending > 50
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "High Raft proposal backlog"

- alert: LedgerHighWriteLatency
  expr: |
    histogram_quantile(0.99,
      rate(ledger_grpc_request_latency_seconds_bucket{service="WriteService"}[5m])
    ) > 0.1
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Write latency p99 > 100ms"
```

## Cardinality Guard

A `CardinalityTracker` sits in front of every Prometheus write. It enforces per-metric time-series budgets defined in `CardinalityConfig`. When a new label set would exceed the budget for a metric, the write is dropped and `ledger_metrics_cardinality_overflow_total` is incremented instead.

| Metric                                      | Type    | Labels        | Description                                                 |
| ------------------------------------------- | ------- | ------------- | ----------------------------------------------------------- |
| `ledger_metrics_cardinality_overflow_total` | Counter | `metric_name` | Label sets dropped because the cardinality cap was exceeded |

Any non-zero rate on this counter means a high-cardinality label is being passed to a metric — this is a code defect, not a capacity issue.

```promql
# Any non-zero rate = someone added a high-cardinality label (fix immediately)
rate(ledger_metrics_cardinality_overflow_total[5m]) > 0
```

The current status of all tracked metrics (series count, cap, overflow count) is also available via `HealthService/Check` in the `details` map under the `cardinality:*` keys.

## Prometheus Configuration

### Basic Scrape Config

```yaml
scrape_configs:
  - job_name: "ledger"
    static_configs:
      - targets:
          - "ledger-0:9090"
          - "ledger-1:9090"
          - "ledger-2:9090"
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance
        regex: '([^:]+):\d+'
        replacement: "${1}"
```

### Kubernetes Service Discovery

```yaml
scrape_configs:
  - job_name: "ledger"
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names:
            - inferadb
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app]
        action: keep
        regex: ledger
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: instance
      - source_labels: [__address__]
        action: replace
        regex: '([^:]+):\d+'
        replacement: "${1}:9090"
        target_label: __address__
```

### Prometheus Operator ServiceMonitor

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: ledger
  namespace: inferadb
spec:
  selector:
    matchLabels:
      app: ledger
  endpoints:
    - port: metrics
      interval: 15s
      path: /metrics
  namespaceSelector:
    matchNames:
      - inferadb
```

## Grafana Dashboard

See the [dashboards directory](../dashboards/) for pre-built Grafana dashboards:

- `api-performance.json` — API latency, throughput, and error rates
- `raft-health.json` — Raft consensus health and replication metrics
- `resource-saturation.json` — Resource utilization and saturation indicators
- `storage-engine.json` — B+ tree and storage engine metrics
