# Alerting Guide

Comprehensive alerting configuration for Ledger clusters.

## Alert Severity Levels

| Severity | Response Time | Examples                             |
| -------- | ------------- | ------------------------------------ |
| Critical | Immediate     | No leader, determinism bug, quorum   |
| Warning  | 1 hour        | High latency, proposal backlog       |
| Info     | Next business | Cache misses, approaching thresholds |

## Service Level Objectives

| SLO              | Target | Metric                                    |
| ---------------- | ------ | ----------------------------------------- |
| Availability     | 99.9%  | Health checks pass                        |
| Write latency    | <100ms | p99 `ledger_write_latency_seconds`        |
| Read latency     | <10ms  | p99 `ledger_read_latency_seconds`         |
| Leader election  | <5s    | Time without leader                       |
| Data consistency | 100%   | `ledger_determinism_bug_total` always = 0 |

## Critical Alerts (Page Immediately)

These require immediate attention and should page on-call.

### LedgerDeterminismBug

**Severity**: Critical

**Description**: A vault has detected state divergence between replicas. This indicates a determinism bug that could lead to data corruption.

```yaml
- alert: LedgerDeterminismBug
  expr: ledger_determinism_bug_total > 0
  labels:
    severity: critical
  annotations:
    summary: "CRITICAL: Determinism bug detected"
    description: "Vault {{ $labels.vault_id }} has state divergence. Immediate investigation required."
    runbook: "docs/operations/runbooks/disaster-recovery.md#data-corruption-recovery"
```

**Response**:

1. Check which vault is affected
2. Stop writes to the affected vault if possible
3. Follow [Data Corruption Recovery](runbooks/disaster-recovery.md#data-corruption-recovery)

### LedgerNoLeader

**Severity**: Critical

**Description**: No Raft leader is elected. The cluster cannot accept writes.

```yaml
- alert: LedgerNoLeader
  expr: sum(inferadb_ledger_raft_is_leader) == 0
  for: 30s
  labels:
    severity: critical
  annotations:
    summary: "No Raft leader elected"
    description: "Cluster has no leader for 30s. Writes are unavailable."
    runbook: "docs/operations/runbooks/disaster-recovery.md#quorum-recovery"
```

**Response**:

1. Check node health: `grpcurl -plaintext node:50051 ledger.v1.HealthService/Check`
2. Verify network connectivity between nodes
3. Check logs for election failures
4. If quorum lost, follow [Quorum Recovery](runbooks/disaster-recovery.md#quorum-recovery)

### LedgerMultipleLeaders

**Severity**: Critical

**Description**: Multiple nodes claim leadership. This indicates a split-brain scenario.

```yaml
- alert: LedgerMultipleLeaders
  expr: sum(inferadb_ledger_raft_is_leader) > 1
  labels:
    severity: critical
  annotations:
    summary: "Multiple Raft leaders detected"
    description: "{{ $value }} nodes claim leadership. Split-brain scenario."
```

**Response**:

1. Identify which nodes claim leadership
2. Check for network partitions
3. Restart the node with higher term number

### LedgerQuorumLost

**Severity**: Critical

**Description**: Less than majority of nodes are healthy.

```yaml
- alert: LedgerQuorumLost
  expr: sum(up{job="ledger"}) < ceil(count(up{job="ledger"}) / 2) + 1
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "Cluster quorum lost"
    description: "Only {{ $value }} nodes are up. Minimum {{ $threshold }} required for quorum."
```

### LedgerNodeDown

**Severity**: Critical

**Description**: A node has been unreachable for over 5 minutes.

```yaml
- alert: LedgerNodeDown
  expr: up{job="ledger"} == 0
  for: 5m
  labels:
    severity: critical
  annotations:
    summary: "Ledger node unreachable"
    description: "Node {{ $labels.instance }} has been down for 5 minutes."
```

## Warning Alerts (Ticket/Slack)

These require attention but not immediate paging.

### LedgerHighProposalBacklog

**Description**: High number of pending Raft proposals indicates slow disk or network.

```yaml
- alert: LedgerHighProposalBacklog
  expr: inferadb_ledger_raft_proposals_pending > 50
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "High Raft proposal backlog"
    description: "{{ $value }} proposals pending. Check disk I/O and network latency."
```

**Response**:

1. Check disk I/O: `iostat -x 1`
2. Check network latency between nodes
3. Review batch settings if consistently high

### LedgerHighWriteLatency

**Description**: Write latency exceeds SLO threshold.

```yaml
- alert: LedgerHighWriteLatency
  expr: histogram_quantile(0.99, rate(ledger_write_latency_seconds_bucket[5m])) > 0.1
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Write latency p99 > 100ms"
    description: "Current p99: {{ $value | humanizeDuration }}"
```

### LedgerHighReadLatency

**Description**: Read latency exceeds SLO threshold.

```yaml
- alert: LedgerHighReadLatency
  expr: histogram_quantile(0.99, rate(ledger_read_latency_seconds_bucket[5m])) > 0.01
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Read latency p99 > 10ms"
    description: "Current p99: {{ $value | humanizeDuration }}"
```

### LedgerHighErrorRate

**Description**: Error rate exceeds 1%.

```yaml
- alert: LedgerHighErrorRate
  expr: |
    sum(rate(ledger_writes_total{status="error"}[5m])) /
    sum(rate(ledger_writes_total[5m])) > 0.01
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Write error rate > 1%"
    description: "Current error rate: {{ $value | humanizePercentage }}"
```

### LedgerLeaderFlapping

**Description**: Frequent leader elections indicate cluster instability.

```yaml
- alert: LedgerLeaderFlapping
  expr: changes(inferadb_ledger_raft_term[10m]) > 5
  labels:
    severity: warning
  annotations:
    summary: "Frequent leader elections"
    description: "{{ $value }} elections in 10 minutes. Check network stability."
```

### LedgerDiskSpaceLow

**Description**: Data directory running low on disk space.

```yaml
- alert: LedgerDiskSpaceLow
  expr: |
    (node_filesystem_avail_bytes{mountpoint="/data"} /
     node_filesystem_size_bytes{mountpoint="/data"}) < 0.15
  labels:
    severity: warning
  annotations:
    summary: "Disk space below 15%"
    description: "{{ $labels.instance }} has {{ $value | humanizePercentage }} disk space remaining."
```

### LedgerSnapshotFailed

**Description**: Snapshot creation failed.

```yaml
- alert: LedgerSnapshotFailed
  expr: increase(ledger_snapshots_created_total[1h]) == 0 and ledger_snapshots_created_total > 0
  labels:
    severity: warning
  annotations:
    summary: "No snapshots created in 1 hour"
    description: "Check snapshot configuration and disk space."
```

## Info Alerts

Non-urgent notifications for awareness.

### LedgerHighCacheMissRate

```yaml
- alert: LedgerHighCacheMissRate
  expr: |
    rate(ledger_idempotency_cache_misses_total[5m]) /
    (rate(ledger_idempotency_cache_hits_total[5m]) + rate(ledger_idempotency_cache_misses_total[5m])) > 0.5
  for: 15m
  labels:
    severity: info
  annotations:
    summary: "High idempotency cache miss rate"
    description: "Consider increasing cache size if retries are common."
```

### LedgerApproachingRateLimit

```yaml
- alert: LedgerApproachingRateLimit
  expr: rate(ledger_rate_limit_exceeded_total[5m]) > 0
  for: 5m
  labels:
    severity: info
  annotations:
    summary: "Rate limits being hit"
    description: "Namespace {{ $labels.namespace_id }} hitting rate limits."
```

## Complete Alerting Rules File

```yaml
# ledger-alerts.yaml
groups:
  - name: ledger.critical
    rules:
      - alert: LedgerDeterminismBug
        expr: ledger_determinism_bug_total > 0
        labels:
          severity: critical
        annotations:
          summary: "CRITICAL: Determinism bug detected"
          description: "Vault {{ $labels.vault_id }} has state divergence."

      - alert: LedgerNoLeader
        expr: sum(inferadb_ledger_raft_is_leader) == 0
        for: 30s
        labels:
          severity: critical
        annotations:
          summary: "No Raft leader elected"

      - alert: LedgerMultipleLeaders
        expr: sum(inferadb_ledger_raft_is_leader) > 1
        labels:
          severity: critical
        annotations:
          summary: "Multiple Raft leaders detected"

      - alert: LedgerNodeDown
        expr: up{job="ledger"} == 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Ledger node {{ $labels.instance }} unreachable"

  - name: ledger.warning
    rules:
      - alert: LedgerHighProposalBacklog
        expr: inferadb_ledger_raft_proposals_pending > 50
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High Raft proposal backlog: {{ $value }}"

      - alert: LedgerHighWriteLatency
        expr: histogram_quantile(0.99, rate(ledger_write_latency_seconds_bucket[5m])) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Write latency p99 > 100ms"

      - alert: LedgerHighReadLatency
        expr: histogram_quantile(0.99, rate(ledger_read_latency_seconds_bucket[5m])) > 0.01
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Read latency p99 > 10ms"

      - alert: LedgerLeaderFlapping
        expr: changes(inferadb_ledger_raft_term[10m]) > 5
        labels:
          severity: warning
        annotations:
          summary: "Frequent leader elections"

      - alert: LedgerDiskSpaceLow
        expr: |
          (node_filesystem_avail_bytes{mountpoint="/data"} /
           node_filesystem_size_bytes{mountpoint="/data"}) < 0.15
        labels:
          severity: warning
        annotations:
          summary: "Disk space below 15%"

  - name: ledger.info
    rules:
      - alert: LedgerHighCacheMissRate
        expr: |
          rate(ledger_idempotency_cache_misses_total[5m]) /
          (rate(ledger_idempotency_cache_hits_total[5m]) + rate(ledger_idempotency_cache_misses_total[5m])) > 0.5
        for: 15m
        labels:
          severity: info
        annotations:
          summary: "High idempotency cache miss rate"

      - alert: LedgerApproachingRateLimit
        expr: rate(ledger_rate_limit_exceeded_total[5m]) > 0
        for: 5m
        labels:
          severity: info
        annotations:
          summary: "Rate limits being hit"
```

## Alertmanager Configuration

### PagerDuty Integration

```yaml
# alertmanager.yml
global:
  pagerduty_url: "https://events.pagerduty.com/v2/enqueue"

route:
  receiver: "default"
  group_by: ["alertname", "severity"]
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  routes:
    - match:
        severity: critical
      receiver: "pagerduty-critical"
      continue: true
    - match:
        severity: warning
      receiver: "slack-warnings"

receivers:
  - name: "default"
    slack_configs:
      - channel: "#ledger-alerts"

  - name: "pagerduty-critical"
    pagerduty_configs:
      - service_key: "<your-pagerduty-key>"
        severity: critical
        description: "{{ .CommonAnnotations.summary }}"
        details:
          firing: "{{ .Alerts.Firing | len }}"
          description: "{{ .CommonAnnotations.description }}"
          runbook: "{{ .CommonAnnotations.runbook }}"

  - name: "slack-warnings"
    slack_configs:
      - channel: "#ledger-alerts"
        send_resolved: true
        title: "{{ .Status | toUpper }}: {{ .CommonAnnotations.summary }}"
        text: "{{ .CommonAnnotations.description }}"
```

### Slack Integration

```yaml
receivers:
  - name: "slack-critical"
    slack_configs:
      - api_url: "https://hooks.slack.com/services/xxx/yyy/zzz"
        channel: "#ledger-critical"
        color: '{{ if eq .Status "firing" }}danger{{ else }}good{{ end }}'
        title: "{{ .CommonAnnotations.summary }}"
        text: |
          *Description:* {{ .CommonAnnotations.description }}
          *Runbook:* {{ .CommonAnnotations.runbook }}
          *Alerts:* {{ .Alerts | len }}
```

## On-Call Checklist

When paged for a critical alert:

1. **Acknowledge** the alert in PagerDuty
2. **Assess** severity and impact
3. **Verify** alert is not a false positive
4. **Execute** the runbook linked in the alert
5. **Communicate** status to stakeholders
6. **Document** in incident tracker
7. **Resolve** and write post-mortem if needed

## See Also

- [Metrics Reference](metrics-reference.md) - All available metrics
- [Disaster Recovery](runbooks/disaster-recovery.md) - Recovery procedures
- [Troubleshooting](../troubleshooting.md) - Common issues
