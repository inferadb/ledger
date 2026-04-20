# Node Connection Registry

On-call runbook for the node-level `NodeConnectionRegistry` — the per-node HTTP/2 gRPC channel pool that backs all inter-node traffic (consensus, cross-region forwarding, system discovery, admin).

## Symptom

- `ledger_node_connections_active` diverges from the expected `cluster_size - 1` — too high (stale entries accumulating) or too low (peers not registering).
- `rate(ledger_node_connection_events_total{event="registered"}[5m])` sustained high, indicating peer churn.
- `ledger_node_connections_active` oscillating rather than stable.

## Alert / Trigger

- `LedgerNodeRegistryTopologyDrift` — `|active - (cluster_size - 1)| > 0` sustained for 5 minutes.
- `LedgerNodeRegistryChurn` — `rate(registered_events[5m]) > 0.5` sustained for 10 minutes.

## Blast radius

- **Scope**: local node's outbound peer connectivity. Stale entries waste channel slots; missing entries cause inter-node traffic to that peer to fail.
- **Downstream impact**: missing entries affect every subsystem borrowing the registry — consensus replication stalls, cross-region forwarding fails, admin RPCs to the affected peer time out.

## Preconditions

- Read access to node Prometheus metrics.
- If the fix involves membership changes: authority to call `AdminService.JoinCluster` or `LeaveCluster`.
- If the fix involves a code bug (missing `set_peer_via_registry` wiring): developer access to file the fix.

## Steps

1. **Measure the drift**: compare `ledger_node_connections_active` against `count by (instance) (up{job="ledger"}) - 1`. Determine whether the registry is too large (stale entries) or too small (missing entries).
2. **Classify**: use the symptom → cause table in [Deep reference § Symptom → Cause → Action](#symptom--cause--action). Stale entries point to broken pruning; missing entries point to broken registration; oscillation points to redundant register/remove sequences.
3. **If stale entries**: audit `on_membership_changed` wiring in `GrpcConsensusTransport`; verify `retain_peers` runs on every membership callback. File a bug if wiring is missing for a subsystem.
4. **If missing entries**: check `RaftManager::start_region` and `AdminService::join_cluster` paths; confirm `set_peer_via_registry` is called on every join.
5. **If churn**: investigate cluster stability — flapping nodes, unstable network partitions, or recent operator actions that may be triggering repeated join/leave.

## Verification

- `ledger_node_connections_active` equals `cluster_size - 1` on every node.
- `rate(ledger_node_connection_events_total{event="registered"}[5m])` returns to 0 (outside of known membership-change windows).
- Subsystems that were failing (consensus replication to affected peer, admin RPCs) succeed.

## Rollback

Registry entries are in-memory state rebuilt from GLOBAL directory on restart. If a manual intervention corrupts the registry, restarting the node reconciles it from authoritative cluster state. No destructive on-disk action is taken by this runbook.

## Escalation

- Persistent drift after verifying membership and pruning wiring: page the consensus / transport owner.
- Cluster-wide registry churn: coordinate with the [consensus-transport-backpressure runbook](consensus-transport-backpressure.md) — connection churn often correlates with backpressure events.

## Deep reference

### Purpose

Each node owns a single `NodeConnectionRegistry` that holds one HTTP/2 gRPC
`Channel` per peer. Consensus (AppendEntries), cross-region forwarding,
system discovery, and admin RPCs all borrow from this registry so one TCP
connection serves all outbound traffic to a given peer. The registry replaces
the previous per-region, per-subsystem channel duplication.

The two metrics in this runbook track how many connections the registry is
holding and lifecycle events against that set.

## Related

- [Metrics Reference: Node Connection Registry](../reference/metrics.md#node-connection-registry)
- [Consensus Transport Backpressure](consensus-transport-backpressure.md) — per-peer send queue (consumes the registry's channels)
- [Leader Cache Diagnosis](leader-cache-diagnosis.md) — SDK-side leader routing

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
