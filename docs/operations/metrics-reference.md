# Metrics Reference

Complete reference for Prometheus metrics exposed by Ledger.

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

| Suffix     | Type              | Example                              |
| ---------- | ----------------- | ------------------------------------ |
| `_total`   | Counter           | `ledger_writes_total`                |
| `_seconds` | Histogram         | `ledger_write_latency_seconds`       |
| `_bytes`   | Histogram/Counter | `ledger_storage_bytes_written_total` |
| (none)     | Gauge             | `ledger_active_connections`          |

## Write Service

| Metric                             | Type      | Labels            | Description                  |
| ---------------------------------- | --------- | ----------------- | ---------------------------- |
| `ledger_writes_total`              | Counter   | `status`          | Total write operations       |
| `ledger_write_latency_seconds`     | Histogram | `status`          | Write operation latency      |
| `ledger_batch_writes_total`        | Counter   | `status`          | Total batch write operations |
| `ledger_batch_size`                | Histogram | -                 | Operations per batch         |
| `ledger_rate_limit_exceeded_total` | Counter   | `organization_id` | Rate limit violations        |

**Labels:**

- `status`: `success` or `error`
- `organization_id`: Internal organization identifier

## Read Service

| Metric                        | Type      | Labels               | Description                   |
| ----------------------------- | --------- | -------------------- | ----------------------------- |
| `ledger_reads_total`          | Counter   | `status`             | Total read operations         |
| `ledger_read_latency_seconds` | Histogram | `status`, `verified` | Read operation latency        |
| `ledger_verified_reads_total` | Counter   | `status`             | Reads with proof verification |

## Raft Consensus

| Metric                                       | Type      | Labels | Description                    |
| -------------------------------------------- | --------- | ------ | ------------------------------ |
| `inferadb_ledger_raft_proposals_total`       | Counter   | -      | Total Raft proposals submitted |
| `inferadb_ledger_raft_proposals_pending`     | Gauge     | -      | Proposals awaiting commit      |
| `inferadb_ledger_raft_apply_latency_seconds` | Histogram | -      | Log entry apply latency        |
| `inferadb_ledger_raft_commit_index`          | Gauge     | -      | Current commit index           |
| `inferadb_ledger_raft_term`                  | Gauge     | -      | Current Raft term              |
| `inferadb_ledger_raft_is_leader`             | Gauge     | -      | 1 if leader, 0 otherwise       |

### Key Indicators

```promql
# Leader status (should be exactly 1 across cluster)
sum(inferadb_ledger_raft_is_leader)

# Proposal backlog (high = disk/network bottleneck)
inferadb_ledger_raft_proposals_pending > 50

# Term changes (high = election instability)
rate(inferadb_ledger_raft_term[5m])
```

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

| Metric                                     | Type    | Labels | Description                     |
| ------------------------------------------ | ------- | ------ | ------------------------------- |
| `ledger_idempotency_cache_hits_total`      | Counter | -      | Cache hits (duplicate requests) |
| `ledger_idempotency_cache_misses_total`    | Counter | -      | Cache misses (new requests)     |
| `ledger_idempotency_cache_size`            | Gauge   | -      | Current cache entries           |
| `ledger_idempotency_cache_evictions_total` | Counter | -      | Evicted entries                 |

### Key Indicators

```promql
# Cache hit ratio
rate(ledger_idempotency_cache_hits_total[5m]) /
(rate(ledger_idempotency_cache_hits_total[5m]) + rate(ledger_idempotency_cache_misses_total[5m]))
```

## Connections

| Metric                                | Type      | Labels                        | Description               |
| ------------------------------------- | --------- | ----------------------------- | ------------------------- |
| `ledger_active_connections`           | Gauge     | -                             | Active gRPC connections   |
| `ledger_grpc_requests_total`          | Counter   | `service`, `method`, `status` | gRPC requests by endpoint |
| `ledger_grpc_request_latency_seconds` | Histogram | `service`, `method`           | gRPC request latency      |

**Labels:**

- `service`: `ReadService`, `WriteService`, `AdminService`, `OrganizationService`, `VaultService`, `UserService`, `AppService`, `TokenService`, `EventsService`, `HealthService`, `DiscoveryService`, `RaftService`
- `method`: RPC method name
- `status`: gRPC status code

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

| Metric                          | Type    | Labels                                  | Description                             |
| ------------------------------- | ------- | --------------------------------------- | --------------------------------------- |
| `ledger_recovery_success_total` | Counter | `organization_id`, `vault_id`           | Successful vault recoveries             |
| `ledger_recovery_failure_total` | Counter | `organization_id`, `vault_id`, `reason` | Failed recovery attempts                |
| `ledger_determinism_bug_total`  | Counter | `organization_id`, `vault_id`           | **CRITICAL**: Determinism bugs detected |

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

## Serialization

| Metric                                         | Type      | Labels                    | Description              |
| ---------------------------------------------- | --------- | ------------------------- | ------------------------ |
| `ledger_serialization_proto_decode_seconds`    | Histogram | `operation`               | Protobuf decoding time   |
| `ledger_serialization_postcard_encode_seconds` | Histogram | `entry_type`              | Postcard encoding time   |
| `ledger_serialization_postcard_decode_seconds` | Histogram | `entry_type`              | Postcard decoding time   |
| `ledger_serialization_bytes`                   | Histogram | `direction`, `entry_type` | Serialized payload sizes |

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

## Onboarding and TOTP

| Metric                                          | Type    | Labels   | Description                               |
| ----------------------------------------------- | ------- | -------- | ----------------------------------------- |
| `ledger_onboarding_initiation_total`            | Counter | `status` | Email verification initiations            |
| `ledger_onboarding_verification_total`          | Counter | `status` | Email code verifications                  |
| `ledger_onboarding_registration_total`          | Counter | `status` | Registration completions                  |
| `ledger_onboarding_verification_codes_gc_total` | Counter | -        | Expired verification codes cleaned by GC  |
| `ledger_onboarding_accounts_gc_total`           | Counter | -        | Expired onboarding accounts cleaned by GC |
| `ledger_totp_challenges_gc_total`               | Counter | -        | Expired TOTP challenges cleaned by GC     |

**Labels:**

- `status`: `success`, `failure`, `rate_limited`, `expired`, `totp_required`

## Background Jobs

| Metric                                        | Type      | Labels          | Description                |
| --------------------------------------------- | --------- | --------------- | -------------------------- |
| `ledger_background_job_duration_seconds`      | Histogram | `job`           | Duration of each job cycle |
| `ledger_background_job_runs_total`            | Counter   | `job`, `result` | Total cycles executed      |
| `ledger_background_job_items_processed_total` | Counter   | `job`           | Work items processed       |

**Labels:**

- `job`: `gc`, `compaction`, `integrity_scrub`, `auto_recovery`, `backup`, `dek_rewrap`, `orphan_cleanup`, `saga_orchestrator`, `token_maintenance`, `ttl_gc`, `learner_refresh`, `organization_purge`, `user_retention`, `events_gc`, `resource_metrics`
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

## SLI Metrics

| Metric                            | Type      | Labels                                       | Description                      |
| --------------------------------- | --------- | -------------------------------------------- | -------------------------------- |
| `ledger_grpc_sli_latency_seconds` | Histogram | `service`, `method`, `status`, `error_class` | Per-RPC latency with SLI buckets |

**SLI histogram buckets**: 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 10.0 seconds.

## Hot Key Detection

| Metric                            | Type    | Labels     | Description                |
| --------------------------------- | ------- | ---------- | -------------------------- |
| `ledger_hot_key_detections_total` | Counter | `vault_id` | Hot key detection events   |
| `ledger_hot_key_current_count`    | Gauge   | -          | Currently tracked hot keys |

## Rate Limiting

| Metric                             | Type    | Labels            | Description                       |
| ---------------------------------- | ------- | ----------------- | --------------------------------- |
| `ledger_rate_limit_exceeded_total` | Counter | `organization_id` | Rate limit violations             |
| `ledger_rate_limit_queue_depth`    | Gauge   | -                 | Pending proposals in rate limiter |

## Quota

| Metric                        | Type    | Labels                        | Description      |
| ----------------------------- | ------- | ----------------------------- | ---------------- |
| `ledger_quota_exceeded_total` | Counter | `organization_id`, `resource` | Quota violations |

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
  expr: histogram_quantile(0.99, ledger_write_latency_seconds) > 0.1
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Write latency p99 > 100ms"
```

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
        organizations:
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
  organization: inferadb
spec:
  selector:
    matchLabels:
      app: ledger
  endpoints:
    - port: metrics
      interval: 15s
      path: /metrics
  organizationSelector:
    matchNames:
      - inferadb
```

## Grafana Dashboard

See [grafana/ledger-dashboard.json](grafana/ledger-dashboard.json) for a complete dashboard.
