# SLI/SLO Reference

Recommended Service Level Indicators and Objectives for InferaDB Ledger in production.

## Histogram Bucket Configuration

All latency histograms use SLI-aligned bucket boundaries (seconds):

```
0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0
```

These boundaries enable accurate p50/p95/p99 calculation at the latency ranges that matter for SLO alerting.

## Recommended SLO Thresholds

### Latency

| Operation             | p50 Target | p95 Target | p99 Target | Metric                                       |
| --------------------- | ---------- | ---------- | ---------- | -------------------------------------------- |
| Read (single entity)  | < 1ms      | < 5ms      | < 10ms     | `ledger_read_latency_seconds`                |
| Write (single entity) | < 5ms      | < 25ms     | < 50ms     | `ledger_write_latency_seconds`               |
| Batch write           | < 10ms     | < 50ms     | < 100ms    | `ledger_write_latency_seconds`               |
| Raft apply            | < 1ms      | < 5ms      | < 25ms     | `inferadb_ledger_raft_apply_latency_seconds` |
| Batch flush           | < 5ms      | < 25ms     | < 100ms    | `ledger_batch_flush_latency_seconds`         |

### Availability

| SLI                  | Target  | Metric / Query                                                                                           |
| -------------------- | ------- | -------------------------------------------------------------------------------------------------------- |
| Request success rate | > 99.9% | `1 - (rate(ledger_grpc_requests_total{error_class!="none"}[5m]) / rate(ledger_grpc_requests_total[5m]))` |
| Cluster quorum       | 100%    | `ledger_cluster_quorum_status == 1`                                                                      |
| Node readiness       | > 99.9% | Health probe success rate from Kubernetes                                                                |

### Error Budget

| Error Class    | Budget (30-day)           | Alert Threshold                |
| -------------- | ------------------------- | ------------------------------ |
| `timeout`      | < 0.1% of requests        | Burn rate > 14.4x over 1h      |
| `unavailable`  | < 0.1% of requests        | Burn rate > 14.4x over 1h      |
| `internal`     | < 0.01% of requests       | Any occurrence triggers page   |
| `rate_limited` | Informational             | Spike > 10x baseline over 5m   |
| `validation`   | No budget (client errors) | Monitor for SDK version issues |

## Key Metrics for SLO Monitoring

### Leading Indicators (Saturation)

| Metric                                   | Warning | Critical | Description                           |
| ---------------------------------------- | ------- | -------- | ------------------------------------- |
| `ledger_batch_queue_depth`               | > 500   | > 900    | Pending writes in batch writer        |
| `ledger_rate_limit_queue_depth`          | > 40    | > 80     | Pending Raft proposals (backpressure) |
| `inferadb_ledger_raft_proposals_pending` | > 100   | > 500    | Uncommitted Raft proposals            |

### Cluster Health

| Metric                           | Expected  | Alert Condition            |
| -------------------------------- | --------- | -------------------------- |
| `ledger_cluster_quorum_status`   | 1         | Value drops to 0           |
| `ledger_leader_elections_total`  | Low rate  | > 3 elections in 5 minutes |
| `inferadb_ledger_raft_is_leader` | Stable    | Flapping (> 2 changes/min) |
| `inferadb_ledger_raft_term`      | Monotonic | Rapid increases            |

### Error Classification

The `error_class` label on `ledger_grpc_requests_total` groups errors by cause:

| Class               | gRPC Codes                                                                     | Operator Action                           |
| ------------------- | ------------------------------------------------------------------------------ | ----------------------------------------- |
| `none`              | OK                                                                             | Normal operation                          |
| `timeout`           | DEADLINE_EXCEEDED, CANCELLED                                                   | Check Raft latency, batch queue depth     |
| `unavailable`       | UNAVAILABLE                                                                    | Check leader status, network connectivity |
| `permission_denied` | PERMISSION_DENIED, UNAUTHENTICATED                                             | Check upstream auth service               |
| `validation`        | INVALID_ARGUMENT, NOT_FOUND, ALREADY_EXISTS, FAILED_PRECONDITION, OUT_OF_RANGE | SDK version mismatch or client bugs       |
| `rate_limited`      | RESOURCE_EXHAUSTED                                                             | Scale cluster or adjust rate limits       |
| `internal`          | INTERNAL, DATA_LOSS, UNKNOWN, UNIMPLEMENTED                                    | Investigate logs immediately              |

## Example Prometheus Alerts

### High Error Rate (Multi-Window Multi-Burn-Rate)

```yaml
# Fast burn: 14.4x error budget consumption over 1 hour
- alert: LedgerHighErrorRate_1h
  expr: |
    (
      rate(ledger_grpc_requests_total{error_class=~"timeout|unavailable|internal"}[1h])
      / rate(ledger_grpc_requests_total[1h])
    ) > 14.4 * 0.001
  for: 2m
  labels:
    severity: critical
```

### Write Latency SLO Breach

```yaml
- alert: LedgerWriteLatencyP99High
  expr: |
    histogram_quantile(0.99, rate(ledger_write_latency_seconds_bucket[5m])) > 0.05
  for: 5m
  labels:
    severity: warning
```

### Quorum Lost

```yaml
- alert: LedgerQuorumLost
  expr: ledger_cluster_quorum_status == 0
  for: 30s
  labels:
    severity: critical
```

### Excessive Leader Elections

```yaml
- alert: LedgerExcessiveElections
  expr: rate(ledger_leader_elections_total[5m]) > 0.01
  for: 5m
  labels:
    severity: warning
```
