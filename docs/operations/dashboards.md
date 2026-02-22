# Grafana Dashboard Templates

Pre-built Grafana dashboards for InferaDB Ledger operational monitoring. Four dashboards cover API performance, Raft consensus health, storage engine metrics, and resource saturation.

## Installation

Import each JSON file from `docs/dashboards/` into Grafana:

1. Open Grafana → Dashboards → Import.
2. Upload the JSON file or paste its contents.
3. Select your Prometheus datasource when prompted.
4. Save.

Dashboards use a `$datasource` variable so you can switch between Prometheus instances without editing queries.

## Dashboards

### API Performance (`api-performance.json`)

Covers the gRPC request lifecycle from ingestion through response.

| Panel                       | Metric(s)                                         | Purpose                      |
| --------------------------- | ------------------------------------------------- | ---------------------------- |
| Request Rate                | `ledger_grpc_requests_total`                      | Throughput by service/method |
| Error Rate                  | `ledger_grpc_requests_total{error_class!="none"}` | SLO: > 99.9% success         |
| Request Latency p50/p95/p99 | `ledger_grpc_request_latency_seconds`             | SLO tracking                 |
| Write Latency p50/p95/p99   | `ledger_write_latency_seconds`                    | SLO: p99 < 50ms              |
| Read Latency p50/p95/p99    | `ledger_read_latency_seconds`                     | SLO: p99 < 10ms              |
| Error Class Breakdown       | `ledger_grpc_requests_total` by `error_class`     | Root cause triage            |
| Rate Limit Rejections       | `ledger_rate_limit_rejected_total`                | Backpressure visibility      |
| Batch Write Performance     | `ledger_batch_flush_latency_seconds`              | SLO: p99 < 100ms             |
| Organization Operations        | `ledger_organization_operations_total`               | Per-tenant traffic           |
| Organization Latency           | `ledger_organization_latency_seconds`                | Per-tenant latency           |

**Variables:** `$datasource`, `$node_id`, `$organization_slug`, `$service`, `$method`

### Raft Consensus Health (`raft-health.json`)

Monitors consensus stability, leader elections, and proposal pipeline.

| Panel                    | Metric(s)                                                 | Purpose                     |
| ------------------------ | --------------------------------------------------------- | --------------------------- |
| Quorum Status            | `ledger_cluster_quorum_status`                            | SLO: 100% quorum            |
| Current Leader           | `inferadb_ledger_raft_is_leader`                          | Leader identification       |
| Current Term             | `inferadb_ledger_raft_term`                               | Election tracking           |
| Commit Index             | `inferadb_ledger_raft_commit_index`                       | Replication progress        |
| Leader Elections         | `ledger_leader_elections_total`                           | Alert: > 3 in 5 min         |
| Raft Apply Latency       | `inferadb_ledger_raft_apply_latency_seconds`              | SLO: p99 < 25ms             |
| Proposal Rate & Timeouts | `inferadb_ledger_raft_proposals_total`, `_timeouts_total` | Write pipeline health       |
| Pending Proposals        | `inferadb_ledger_raft_proposals_pending`                  | Saturation indicator        |
| Snapshot Operations      | `ledger_snapshot_create/restore_latency_seconds`          | Recovery performance        |
| Vault Health             | `ledger_vault_health`                                     | Divergence detection        |
| Recovery Attempts        | `ledger_divergence_recovery_attempts_total`               | Auto-recovery effectiveness |
| Idempotency Cache        | `ledger_idempotency_cache_{hits,misses,size}_total`       | Deduplication effectiveness |
| State Root Computation   | `inferadb_ledger_state_root_latency_seconds`              | Merkle computation overhead |

**Variables:** `$datasource`, `$node_id`, `$shard_id`

### Storage Engine (`storage-engine.json`)

Tracks disk usage, page cache efficiency, B-tree structure, and compaction progress.

| Panel               | Metric(s)                                       | Purpose                       |
| ------------------- | ----------------------------------------------- | ----------------------------- |
| Disk Usage          | `ledger_disk_bytes_{total,used,free}`           | Capacity planning             |
| Snapshot Disk Usage | `ledger_snapshot_disk_bytes`                    | Backup storage overhead       |
| Page Cache Hit Rate | `ledger_page_cache_{hits,misses}_total`         | Target: > 95%                 |
| Page Cache Size     | `ledger_page_cache_size`                        | Memory utilization            |
| B-tree Depth        | `ledger_btree_depth`                            | Query performance proxy       |
| B-tree Page Splits  | `ledger_btree_page_splits_total`                | Write amplification indicator |
| Compaction Progress | `ledger_btree_compaction_pages_{merged,freed}`  | Maintenance throughput        |
| Compaction Lag      | `ledger_compaction_lag_blocks`                  | Compaction backlog            |
| Storage I/O         | `ledger_storage_bytes_{written,read}_total`     | I/O bandwidth                 |
| Storage Operations  | `ledger_storage_operations_total`               | IOPS                          |
| Organization Storage   | `ledger_organization_storage_bytes`                | Per-tenant disk usage         |
| Integrity Scrubber  | `ledger_integrity_{pages_checked,errors}_total` | Corruption detection          |

**Variables:** `$datasource`, `$node_id`, `$organization_slug`, `$shard_id`

### Resource Saturation (`resource-saturation.json`)

Leading indicators for capacity planning: queue depths, connection counts, and background job health.

| Panel                     | Metric(s)                                     | Purpose                         |
| ------------------------- | --------------------------------------------- | ------------------------------- |
| Batch Queue Depth         | `ledger_batch_queue_depth`                    | Warning: > 500, Critical: > 900 |
| Rate Limit Queue Depth    | `ledger_rate_limit_queue_depth`               | Warning: > 40, Critical: > 80   |
| Active Connections        | `ledger_active_connections`                   | Connection pool utilization     |
| Pending Raft Proposals    | `inferadb_ledger_raft_proposals_pending`      | Write pipeline saturation       |
| Background Job Health     | `ledger_background_job_runs_total`            | Success rate per job            |
| Background Job Duration   | `ledger_background_job_duration_seconds`      | Job cycle p99 latency           |
| Background Job Throughput | `ledger_background_job_items_processed_total` | Work items per second           |
| Background Job Runs       | `ledger_background_job_runs_total` by result  | Success vs failure counts       |
| Eager vs Timeout Commits  | `ledger_batch_{eager,timeout}_commits_total`  | Batch writer efficiency         |
| Batch Coalesce Size       | `ledger_batch_coalesce_size`                  | Batching effectiveness          |
| Rate Limit Events         | `ledger_rate_limit_{rejected,exceeded}_total` | Throttling visibility           |
| Hot Key Detections        | `ledger_hot_key_detected_total`               | Access pattern anomalies        |
| Audit Events              | `ledger_audit_events_total`                   | Audit trail activity            |

**Variables:** `$datasource`, `$node_id`, `$shard_id`

## Alert Rule Templates

Each dashboard encodes SLO thresholds from [`slo.md`](slo.md) as panel threshold annotations (green/yellow/red bands). To convert these into Grafana alerts:

1. Open the panel with threshold annotations.
2. Click "Alert" tab → "Create alert rule from this panel".
3. Set the evaluation interval and `for` duration per the examples in [`slo.md`](slo.md).

Key alert thresholds (see [`slo.md`](slo.md) for full definitions):

| Alert                    | Metric                             | Condition                       |
| ------------------------ | ---------------------------------- | ------------------------------- |
| High Error Rate          | `ledger_grpc_requests_total`       | Error rate > 0.1% for 2 min     |
| Write Latency SLO Breach | `ledger_write_latency_seconds`     | p99 > 50ms for 5 min            |
| Read Latency SLO Breach  | `ledger_read_latency_seconds`      | p99 > 10ms for 5 min            |
| Quorum Lost              | `ledger_cluster_quorum_status`     | Value == 0 for 30s              |
| Excessive Elections      | `ledger_leader_elections_total`    | > 3 in 5 minutes                |
| Batch Queue Saturation   | `ledger_batch_queue_depth`         | > 900 for 1 min                 |
| Background Job Failure   | `ledger_background_job_runs_total` | Failure rate > 10% for 1 hour   |
| Integrity Errors         | `ledger_integrity_errors_total`    | Any increase                    |
| Determinism Bug          | `ledger_determinism_bug_total`     | Any increase (page immediately) |

## Exemplar Configuration

To link histogram panels to distributed traces (Tempo/Jaeger):

1. Enable exemplar storage in Prometheus (`--enable-feature=exemplar-storage`).
2. Configure InferaDB's metrics exporter to attach `trace_id` exemplar labels on histogram observations.
3. In Grafana, add your trace datasource (Tempo/Jaeger).
4. On histogram panels, enable "Exemplars" in the query options and select the trace datasource.
5. Click exemplar dots on the histogram to jump directly to the associated trace.

InferaDB attaches `x-request-id` and `x-trace-id` to all gRPC responses via `status_with_correlation()`. The trace ID from `x-trace-id` can serve as the exemplar label value when the metrics exporter is configured to include it.

## Validation

To verify dashboards import correctly:

```bash
# Start a local Grafana instance
docker run -d -p 3000:3000 --name grafana grafana/grafana-oss:latest

# Import via API
for f in docs/dashboards/*.json; do
  curl -X POST http://admin:admin@localhost:3000/api/dashboards/db \
    -H "Content-Type: application/json" \
    -d "{\"dashboard\": $(cat "$f"), \"overwrite\": true}"
done

# Verify all four dashboards appear
curl -s http://admin:admin@localhost:3000/api/search?tag=inferadb | jq '.[].title'
```

Expected output:

```
"InferaDB Ledger — API Performance"
"InferaDB Ledger — Raft Consensus Health"
"InferaDB Ledger — Storage Engine"
"InferaDB Ledger — Resource Saturation"
```

## Related Documentation

- [SLI/SLO Reference](slo.md) — threshold definitions and alert examples
- [Background Job Observability](background-jobs.md) — per-job metric details and PromQL queries
- [Error Code Catalog](../errors.md) — machine-readable error codes for error class drill-down
