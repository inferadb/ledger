# Consensus Transport Backpressure Runbook

Procedure for diagnosing inter-node consensus message backpressure using the
`ledger_peer_send_*` Prometheus metrics. Use this runbook when peers report
elevated apply latency, Raft leader-election churn, or alerts fire on peer
send queue depth.

## Purpose

Each node maintains a bounded outbound queue (capacity 1024) per registered
peer. A per-peer drain task dequeues messages and invokes the gRPC transport.
On queue overflow, the oldest message is dropped; Raft retransmits on the
next heartbeat, so dropped heartbeats are self-healing, but sustained drops
indicate the transport cannot keep up with the proposal rate.

The three metrics in this runbook measure queue saturation, drop pressure,
and send latency per peer.

## Healthy Steady State

Under normal operation:

- `ledger_peer_send_queue_depth{peer=*}` hovers at 0 — the drain task keeps up.
- `ledger_peer_send_drops_total` is zero or grows only during membership changes
  (`reason="task_shutdown"` accounts for those).
- `ledger_peer_send_latency_seconds` p99 stays below 50ms within a datacenter
  and around 100ms cross-region.

## Symptom → Cause → Action

| Symptom                                                 | Likely cause                                                                                | Action                                                                                                                   |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Sustained `drops_total{reason="queue_full"}` > 0        | Peer is persistently slow or unreachable; queue fills faster than drain                     | Check peer health and reachability; if down, expect membership change to follow and convert drops to `task_shutdown`     |
| `queue_depth` climbing steadily                         | Transient network congestion or GC pause on target peer                                     | Short-lived: observe and confirm recovery; persistent: investigate peer CPU, GC, and network telemetry                   |
| `queue_depth` pinned at 1024                            | Peer unrecoverable; drops accumulating at the push rate                                     | Peer is effectively down; cross-check `inferadb_ledger_raft_is_leader` and peer-side liveness before removing            |
| `latency_seconds` p99 climbing                          | Network RTT increase, TLS handshake churn, or peer-side scheduling delays                   | Check network telemetry (TCP RTT), recent certificate or config changes, and peer CPU saturation                         |
| `drops_total{reason="task_shutdown"}` spike             | Membership change (voter removed, learner demoted, region rebalance)                        | Expected; cross-check with audit logs or `ledger_background_job_runs_total{job="learner_refresh"}`                       |
| All peers showing `queue_depth` > 0 simultaneously      | This node's tokio runtime is overloaded — not a peer-specific issue                         | Check local CPU and runtime; investigate blocking calls; consider raising `worker_threads`                               |

## Dashboard Recommendations

**Panel 1 — Send queue depth per peer** (line chart):

```promql
ledger_peer_send_queue_depth
```

One series per `peer`. Healthy shape: all series near zero. Any persistent
elevation is actionable.

**Panel 2 — Drop rate per peer per reason** (stacked line):

```promql
rate(ledger_peer_send_drops_total[5m])
```

Group by `peer,reason`. `queue_full` is actionable; `task_shutdown` is
expected during membership changes.

**Panel 3 — Send latency p99** (line, per peer):

```promql
histogram_quantile(0.99, sum by (peer, le) (rate(ledger_peer_send_latency_seconds_bucket[5m])))
```

Target: < 50ms intra-DC, < 100ms cross-region. Sustained breach indicates
network or peer-side degradation.

## Alert Recommendations

```yaml
- alert: LedgerPeerSendBacklog
  expr: rate(ledger_peer_send_drops_total{reason="queue_full"}[5m]) > 0
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "Peer send queue overflowing for peer {{ $labels.peer }}"
    description: "Sustained queue_full drops for 10m — peer is slower than push rate"

- alert: LedgerPeerSendLatencyHigh
  expr: histogram_quantile(0.99, sum by (peer, le) (rate(ledger_peer_send_latency_seconds_bucket[5m]))) > 0.5
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "Peer {{ $labels.peer }} send latency p99 > 500ms"
    description: "Investigate network RTT, peer CPU, and TLS handshake churn"
```

`task_shutdown` drops are NOT alertable — they are a byproduct of membership
changes.

## Caveats

- `drops_total{reason="task_shutdown"}` is an **upper bound**. The drain task
  may successfully send some messages that were accounted as shutdown drops.
  The over-count is bounded by `PEER_QUEUE_CAPACITY` (1024) per peer-removal
  event. This affects post-mortem accounting, not alerting thresholds.
- The `peer` label uses raw node IDs (numeric strings). Cardinality is
  bounded by cluster size (< 100 in typical deployments).

## Related

- [Node Connection Registry spec](../../superpowers/specs/2026-04-15-node-connection-registry.md) (Phase 2 backpressure)
- [Metrics Reference: Consensus Transport](../metrics-reference.md#consensus-transport)
- [Leader Cache Diagnosis Runbook](leader-cache-diagnosis.md) — SDK-side companion for leader routing
