# Leader Cache Diagnosis Runbook

Procedure for diagnosing SDK region-leader cache health using Prometheus
metrics. Use this runbook when clients report elevated tail latency,
unexpected `UNAVAILABLE` errors on write paths, or when alerts fire on
`ledger_sdk_leader_cache_flaps_total`.

## Purpose

The SDK's regional leader cache routes write requests directly to the Raft
leader of the target region, bypassing gateway-side forwarding. It maintains
per-region cached leader endpoints with two TTLs:

- **soft_ttl** (default 30s): beyond this, entries are served stale while a
  background resolve refreshes them.
- **hard_ttl** (default 120s): beyond this, entries are discarded and
  callers block on a foreground resolve.

Concurrent misses for the same region are coalesced via single-flight so
only one `ResolveRegionLeader` RPC is in flight at a time. Server-side
`NotLeader` hints are applied directly to the cache without a round-trip.

The five metrics in this runbook measure the cache's effectiveness at
avoiding unnecessary resolve RPCs and reacting to leader changes.

## Healthy Steady State

In a region with a stable leader and long-lived SDK clients:

- `hits_total` dominates; hit ratio typically > 0.95.
- `misses_total` occurs on client startup and after `hard_ttl` expiry
  without activity.
- `flaps_total` is near zero. Any sustained non-zero rate indicates
  leader elections.
- `singleflight_coalesced_total` is zero or bursts briefly during
  concurrent startup or after a leader change.
- `stale_served_total` accumulates slowly during soft-ttl refresh
  windows; bounded by request rate divided by `soft_ttl`.

## Symptom → Cause → Action

| Symptom                                                 | Likely cause                                                                                                   | Action                                                                                                                                                    |
| ------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Sustained `flaps_total` > 0/min                         | Genuine leader elections (network partitions, peer health, leadership transfers)                               | Investigate Raft election rate per region; correlate with `inferadb_ledger_raft_term` and `raft_elections_total`; check peer reachability and disk health |
| `misses_total` >> `hits_total`                          | TTL too short, cache not reused across clients, or clients creating fresh `ConnectionPool` per request         | Verify SDK clients are long-lived (one per process); review `region_leader_soft_ttl`/`hard_ttl` config; check for client instantiation in request handlers |
| `stale_served_total > 0` in steady state                | Expected during refresh windows after `soft_ttl` expiry                                                        | Normal behavior; monitor that the rate stays bounded and the background refresh completes                                                                 |
| `singleflight_coalesced_total` spikes on leader change  | Burst of concurrent requests hit the cache expiry simultaneously and coalesced into one resolve                | Expected; coalescing is working as designed. Verify the spike subsides once the new leader is cached                                                      |
| Non-zero `misses_total` despite servers emitting hints  | A server code path returns `NotLeader` without `status_with_not_leader_hint`, so the SDK has no hint to apply  | Grep server for bare `Status::unavailable` constructions with "leader" in the message; add hint attachment                                                |
| Gateway load spikes on every leader change              | Many clients' soft_ttl/hard_ttl expired simultaneously and all raced to resolve                                | Stagger `region_leader_soft_ttl` across fleet (jitter), or lean more on `NotLeader` hints so resolve RPCs are avoided entirely                            |
| `flaps_total` > 0 but `inferadb_ledger_raft_term` flat  | Cache is being populated from stale hints or misrouted responses                                                | Check for misconfigured region metadata on the server; verify `ResolveRegionLeader` returns the correct region's leader                                   |

## Dashboard Recommendations

**Panel 1 — Cache effectiveness** (per-region, stacked area):

```promql
rate(ledger_sdk_leader_cache_hits_total[5m])
rate(ledger_sdk_region_resolve_stale_served_total[5m])
rate(ledger_sdk_region_resolve_singleflight_coalesced_total[5m])
rate(ledger_sdk_leader_cache_misses_total[5m])
```

Healthy shape: hits dominate; misses form a thin band. Inversions indicate
client misuse or TTL misconfiguration.

**Panel 2 — Leader flap rate** (per-region, single line):

```promql
rate(ledger_sdk_leader_cache_flaps_total[5m])
```

Any sustained non-zero value is actionable. Correlate with server-side
`inferadb_ledger_raft_term` to confirm whether flaps map to real elections.

**Panel 3 — Hit ratio** (per-region, gauge):

```promql
rate(ledger_sdk_leader_cache_hits_total[5m]) /
(rate(ledger_sdk_leader_cache_hits_total[5m]) + rate(ledger_sdk_leader_cache_misses_total[5m]))
```

Target > 0.95 in steady state. Below 0.80 signals client lifecycle or
TTL issues.

## Alert Recommendations

```yaml
- alert: SdkLeaderCacheFlapping
  expr: rate(ledger_sdk_leader_cache_flaps_total[5m]) > 0.1
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "SDK leader cache flapping in region {{ $labels.region }}"
    description: "Leader changed more than once per 10s sustained for 10m — investigate Raft election stability"

- alert: SdkLeaderCacheMissRatioHigh
  expr: |
    rate(ledger_sdk_leader_cache_misses_total[5m]) /
    (rate(ledger_sdk_leader_cache_hits_total[5m]) + rate(ledger_sdk_leader_cache_misses_total[5m])) > 0.2
  for: 15m
  labels:
    severity: warning
  annotations:
    summary: "SDK leader cache miss ratio high in region {{ $labels.region }}"
    description: "More than 20% of region-leader lookups require a resolve RPC — check client lifecycle and TTL config"
```

## Related

- [Metrics Reference: SDK Region Leader Cache](../metrics-reference.md#sdk-region-leader-cache)
- [Region Management](../region-management.md)
- [Multi-Region Operations](../multi-region.md)
