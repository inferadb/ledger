# Node Connection Registry Runbook

Procedure for diagnosing issues with the node-level `NodeConnectionRegistry`
using the `ledger_node_connections_*` Prometheus metrics. Use this runbook
when peer connectivity appears inconsistent, active connection counts diverge
from expected cluster topology, or membership changes fail to prune stale
peers.

## Purpose

Each node owns a single `NodeConnectionRegistry` that holds one HTTP/2 gRPC
`Channel` per peer. Consensus (AppendEntries), cross-region forwarding,
system discovery, and admin RPCs all borrow from this registry so one TCP
connection serves all outbound traffic to a given peer. The registry replaces
the previous per-region, per-subsystem channel duplication.

The two metrics in this runbook track how many connections the registry is
holding and lifecycle events against that set.

## Related

- [Node Connection Registry spec](../../superpowers/specs/2026-04-15-node-connection-registry.md)
- [Node Connection Registry plan](../../superpowers/plans/2026-04-15-phase-3-node-connection-registry.md)
- [Metrics Reference: Node Connection Registry](../metrics-reference.md#node-connection-registry)
- [Consensus Transport Backpressure Runbook](consensus-transport-backpressure.md) — per-peer send queue (consumes the registry's channels)
- [Leader Cache Diagnosis Runbook](leader-cache-diagnosis.md) — SDK-side leader routing

## Healthy Steady State

Under normal operation:

- `ledger_node_connections_active` equals `cluster_size - 1` (the local node
  is not registered against itself).
- `ledger_node_connection_events_total{event="registered"}` grows only during
  node joins and at startup as initial members register.
- `ledger_node_connection_events_total{event="pruned"}` grows only during
  membership changes (voter removal, region rebalance).
- `ledger_node_connection_events_total{event="unregistered"}` grows on
  explicit peer removal (graceful shutdown or admin API).

## Symptom → Cause → Action

| Symptom                                                        | Likely cause                                                                     | Action                                                                                                                            |
| -------------------------------------------------------------- | -------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `connections_active` climbing past expected cluster size       | Stale entries accumulating — membership pruning not wired for a subsystem        | Audit `on_membership_changed` wiring in `GrpcConsensusTransport`; verify `retain_peers` runs on membership callbacks              |
| `connections_active` below expected                            | Peers not being registered — bootstrap or join path skipped the registry         | Check `RaftManager::start_region` and `AdminService::join_cluster` paths; confirm `set_peer_via_registry` is called on every join |
| `events_total{event="pruned"}` spike                           | Legitimate membership change (voter removal, region rebalance, learner demotion) | Correlate with audit logs and `ledger_background_job_runs_total{job="learner_refresh"}`; no action needed if expected             |
| `events_total{event="registered"}` high rate                   | Peer churn — nodes being added/removed rapidly                                   | Investigate cluster stability; check for flapping nodes or unstable network partitions; review recent operator actions            |
| `connections_active` oscillating                               | Registry entries being created and dropped in a loop                             | Check for redundant `set_peer_via_registry` + `remove_peer` sequences in the join flow or in `retain_peers` callers               |
| `events_total{event="unregistered"}` without membership change | Graceful shutdown or admin-initiated peer removal                                | Cross-check with operator audit log; if unexpected, investigate `AdminService` call history                                       |

## Dashboard Recommendations

**Panel 1 — Active peer connections** (gauge):

```promql
ledger_node_connections_active
```

Single series per node. Healthy shape: flat line at `cluster_size - 1`. Any
persistent divergence from expected topology is actionable.

**Panel 2 — Lifecycle events by type** (stacked counter):

```promql
rate(ledger_node_connection_events_total[5m])
```

Group by `event`. `registered` and `pruned` should move in lockstep with
membership changes. `unregistered` is rare outside graceful shutdown.

## Alert Recommendations

```yaml
- alert: LedgerNodeRegistryTopologyDrift
  expr: |
    abs(ledger_node_connections_active - (count by (instance) (up{job="ledger"}) - 1))
    > 0
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Node {{ $labels.instance }} registry size diverges from cluster topology"
    description: "Active connections != cluster_size - 1 for 5m — investigate stale or missing peer entries"

- alert: LedgerNodeRegistryChurn
  expr: rate(ledger_node_connection_events_total{event="registered"}[5m]) > 0.5
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "Sustained peer registration churn on {{ $labels.instance }}"
    description: "Peer registration rate > 0.5/s for 10m — investigate node flapping or network instability"
```

`pruned` events are NOT alertable in isolation — they are a byproduct of
legitimate membership changes.

## Caveats

- The local node is never registered against itself, so the expected active
  count is `cluster_size - 1`, not `cluster_size`. Topology alerts must
  account for this offset.
- Cross-region forwarding uses string-valued node IDs (hashed into the
  registry's `u64` key space). Collisions are tolerated because the registry
  is a channel cache, not an identity store — a collision means two distinct
  cross-region nodes share a channel slot, which is acceptable at the HTTP/2
  layer.
- The `peer` label uses raw node IDs (numeric strings). Cardinality is
  bounded by cluster size (< 100 in typical deployments).
