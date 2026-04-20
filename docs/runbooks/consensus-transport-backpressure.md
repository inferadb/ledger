# Consensus Transport Backpressure

On-call runbook for inter-node consensus backpressure — the outbound queue feeding peer messages is saturating faster than the drain task can flush it.

## Symptom

- Apply latency rising on multiple regions simultaneously or leader-election churn becoming visible on dashboards.
- `ledger_peer_send_queue_depth{peer=*}` shows non-zero values (healthy baseline is ~0).
- Non-zero `rate(ledger_peer_send_drops_total{reason="queue_full"}[5m])`.

## Alert / Trigger

- `LedgerPeerSendBacklog` — `rate(ledger_peer_send_drops_total{reason="queue_full"}[5m]) > 0` sustained for 10 minutes.
- Supporting alerts: elevated `inferadb_ledger_raft_apply_latency_seconds`, drops in `ledger_cluster_quorum_status`.

## Blast radius

- **Scope**: the affected peer-to-peer channel. If a single `peer=X` is saturating, only traffic to peer X is affected. If *all* peers show saturation simultaneously, the **local** node's tokio runtime is the bottleneck (not a peer problem).
- **Downstream impact**: Raft commits stall; writes that require quorum take longer or time out. Other Raft groups on the same process may also slow if the runtime is the bottleneck.

## Preconditions

- Read access to the cluster's Prometheus metrics endpoint.
- Ability to read the affected peer's CPU, network, and GC telemetry.
- If the fix involves removing a node: authority to call `AdminService.LeaveCluster` for that peer.

## Steps

1. **Triage local vs peer**: check the dashboard for all `peer=*` series. If **all** peers saturating → local runtime bottleneck; investigate this node's CPU, GC, and `worker_threads` config. If **one** peer → peer-specific problem.
2. **Classify the drop reason** — see the Symptom → Cause → Action table in [Deep reference](#symptom--cause--action). `queue_full` is actionable; `task_shutdown` is expected during membership changes.
3. **Execute the remediation** per the table: check peer health, network telemetry, or initiate membership change for a persistently unreachable peer.
4. **If all peers are saturating**, raise `tokio::runtime` worker thread count via `INFERADB__LEDGER__MAX_CONCURRENT` / `--concurrent` tuning OR shed load at the SDK layer until the saturation drains.

## Verification

- `ledger_peer_send_queue_depth{peer=*}` returns to ~0 baseline.
- `rate(ledger_peer_send_drops_total{reason="queue_full"}[5m])` returns to 0.
- Apply latency (`inferadb_ledger_raft_apply_latency_seconds`) returns to normal range.
- Cluster quorum (`ledger_cluster_quorum_status`) is 1.

## Rollback

No destructive actions are taken by this runbook. Membership changes triggered while diagnosing are themselves reversible via `AdminService.JoinCluster`. If worker-thread count was increased and the cluster becomes unstable, revert to the prior value and restart the affected node.

## Escalation

- `queue_full` drops persist after peer remediation: page the consensus-transport owner.
- Apply latency elevated cluster-wide despite local runtime headroom: escalate to disaster-recovery posture — see [disaster-recovery.md](disaster-recovery.md).

## Deep reference

### Purpose

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

| Symptom                                            | Likely cause                                                              | Action                                                                                                               |
| -------------------------------------------------- | ------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| Sustained `drops_total{reason="queue_full"}` > 0   | Peer is persistently slow or unreachable; queue fills faster than drain   | Check peer health and reachability; if down, expect membership change to follow and convert drops to `task_shutdown` |
| `queue_depth` climbing steadily                    | Transient network congestion or GC pause on target peer                   | Short-lived: observe and confirm recovery; persistent: investigate peer CPU, GC, and network telemetry               |
| `queue_depth` pinned at 1024                       | Peer unrecoverable; drops accumulating at the push rate                   | Peer is effectively down; cross-check `inferadb_ledger_raft_is_leader` and peer-side liveness before removing        |
| `latency_seconds` p99 climbing                     | Network RTT increase, TLS handshake churn, or peer-side scheduling delays | Check network telemetry (TCP RTT), recent certificate or config changes, and peer CPU saturation                     |
| `drops_total{reason="task_shutdown"}` spike        | Membership change (voter removed, learner demoted, region rebalance)      | Expected; cross-check with audit logs or `ledger_background_job_runs_total{job="learner_refresh"}`                   |
| All peers showing `queue_depth` > 0 simultaneously | This node's tokio runtime is overloaded — not a peer-specific issue       | Check local CPU and runtime; investigate blocking calls; consider raising `worker_threads`                           |

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
- [Metrics Reference: Consensus Transport](../reference/metrics.md#consensus-transport)
- [Leader Cache Diagnosis Runbook](leader-cache-diagnosis.md) — SDK-side companion for leader routing
