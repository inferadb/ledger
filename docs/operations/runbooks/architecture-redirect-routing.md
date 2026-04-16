# Redirect-Based Client Routing Architecture

Reference document describing how the Ledger cluster routes client requests
to regional Raft leaders. Use this document to understand why clients need
direct connectivity to every node, how cold start and warm paths differ, and
how leader changes are propagated to SDK clients mid-session.

## Purpose

Explains the Phase 5 redirect-based routing model: the SDK — not the server —
is responsible for discovering and connecting directly to the regional
leader. Server-side forwarding of client requests has been removed; the only
remaining cross-node proposal path (`SubmitRegionalProposal`) is reserved
for saga orchestration and does not carry client traffic.

## Model

Clients never proxy through an intermediate node. Any cluster node can serve
as a discovery endpoint, and all data-plane traffic goes directly to the
regional leader.

```text
                    ┌─────────────────────────┐
                    │   SDK (LedgerClient)    │
                    └───────────┬─────────────┘
                                │
            Cold start:         │         Warm path:
            discovery request   │         direct to leader
                                │
        ┌───────────────────────┼───────────────────────┐
        │                       │                       │
        ▼                       ▼                       ▼
┌───────────────┐       ┌───────────────┐       ┌───────────────┐
│   Node A      │       │   Node B      │       │   Node C      │
│  (follower)   │       │   (LEADER     │       │  (follower)   │
│               │──────▶│   region R)   │◀──────│               │
│ NotLeader +   │       │               │       │ NotLeader +   │
│ LeaderHint    │       │  Data plane   │       │ LeaderHint    │
│   → Node B    │       │  accepts      │       │   → Node B    │
└───────────────┘       └───────────────┘       └───────────────┘
```

On cold start the SDK may hit any node; that node responds with a
`NotLeader` hint carrying the leader's endpoint. The SDK retries directly
against the leader. On the warm path, the SDK resolves the leader once at
pool construction via `ResolveRegionLeader` and every subsequent request
goes straight to the leader.

## Why Redirect, Not Proxy

The prior architecture had client requests traverse a gateway node, which
forwarded to the regional leader on the client's behalf. Phase 5 removed
that forwarding layer in favor of client-side redirects.

**One network hop per write instead of two.** Under the proxy model, a
write from a client far from the leader paid two network hops: client →
gateway node → leader node. The redirect model collapses this to a single
hop (client → leader) once the SDK has cached the leader endpoint. The
cold-start penalty is one additional hop, paid once per client lifetime.

**Server code is simpler.** The `ForwardClient`, `RegionForwardClient`,
`LeaderForwardClient`, and `RegionRouter` machinery have been deleted
wholesale. Non-leader nodes no longer need to hold client connections open,
stream request bodies, or re-encode responses. Failure modes that arose
from double-hop request cancellation and partial forwarding are gone.

**The SDK owns leader discovery.** `RegionLeaderCache` holds per-region
leader endpoints with soft/hard TTLs, populated via three sources:
(1) explicit `ResolveRegionLeader` RPCs at pool construction, (2) server
`NotLeader` hints applied via `apply_region_leader_hint_or_invalidate`,
and (3) server-initiated push updates over the `WatchLeader` bidirectional
stream. Each source is term-gated so stale hints cannot overwrite fresher
state.

**Saga orchestration is unchanged.** The `SubmitRegionalProposal` RPC
remains for server-to-server proposal forwarding during cross-region saga
steps (e.g., `MigrateOrganization`). This is a coordination primitive
between nodes, not a client-request proxy, and its semantics are out of
scope for this document.

## Cold Start Flow

An SDK client with an empty `RegionLeaderCache` writes to a region. The
configured endpoint happens to be a follower. Six steps:

1. SDK dispatches the Write RPC to the configured endpoint.
2. The receiving follower returns `Status::unavailable` with an
   `ErrorDetails` payload carrying `leader_id`, `leader_endpoint`, and
   `leader_term` in the context map.
3. The SDK's retry loop sees the hint and calls
   `ConnectionPool::apply_region_leader_hint_or_invalidate`, which
   populates `RegionLeaderCache` with the hinted leader at `leader_term`.
4. The SDK reconnects to the leader's advertised endpoint (opening or
   reusing a `Channel` in the connection pool).
5. The SDK retries the Write directly against the leader.
6. Retry succeeds.

Each cold-start redirect increments `ledger_sdk_redirect_retries_total` by
one. This metric fires once per redirect; a steady non-zero rate in
production indicates either a misconfigured `preferred_region` or cluster
churn (leader changes) rather than broken routing.

## Warm Path

When the SDK's `ClientConfig` specifies `preferred_region`, the connection
pool resolves the regional leader at construction time by calling
`ResolveRegionLeader` against the configured endpoint. The returned leader
endpoint and term populate `RegionLeaderCache` before the first request is
dispatched.

Every subsequent request looks up the leader in the cache (a local map
read), opens a channel to that endpoint (pooled), and dispatches directly.
No `NotLeader` hint is returned, no retry is issued,
`ledger_sdk_redirect_retries_total` stays flat. Round-trip cost matches
direct leader connection; there is no gateway-induced overhead.

## Leader Change During Session

When leadership changes after the cache is warm, the server proactively
pushes the new leader over the `WatchLeader` bidirectional stream. The SDK
applies updates via `apply_watch_update`, which term-gates every update:
an update with `leader_term` less than the cached term is rejected and
the rejection is counted in `ledger_sdk_leader_stale_term_rejected_total`.
Monotonic term ordering ensures the cache can never regress to an older
leader once a newer term has been observed, even if updates arrive out of
order (e.g., a late hint from a slow follower versus a fresh
`WatchLeader` push).

If a request in flight during the leader change lands on the old leader,
the old leader returns `NotLeader` with the new leader's hint, and the
same term-gated apply path kicks in. The `WatchLeader` push and the
`NotLeader` hint are two independent notification channels for the same
underlying state; term gating makes them commutative.

## Network Topology Implications

Under this model, **clients must have direct network connectivity to
every node that could hold regional Raft leadership**. This differs from
the proxy model, where clients only needed reachability to the gateway
subset.

For single-VPC or single-VNet deployments, this is typically already the
case — all nodes and clients share a flat L3 network. No configuration
change is needed.

For multi-VPC, cross-account, or on-prem-to-cloud deployments, firewall
rules and security groups must allow client traffic to reach every
cluster node on the configured gRPC port. A common failure mode during
Phase 5 rollout is clients that could reach the gateway node fine but
cannot reach follower-elected-leader nodes after a leadership change. Audit
rules before enabling redirect routing in these environments.

Kubernetes clusters using a headless Service for peer discovery expose
every pod IP to clients inside the cluster by default; no change needed.
Clients outside the cluster need a Service per pod, a LoadBalancer per
pod, or an ingress that preserves node identity — not a single VIP
fronting the whole StatefulSet.

## Known limitations

### `WatchBlocks` leadership race

`WatchBlocks` is a long-lived server-streaming RPC that subscribes to a
region's block-announcement broadcast. The leadership check fires once
at stream-establishment time. If the node loses leadership mid-stream
(leader flap, membership change), the stream does not terminate with
`NotLeader` — the subscription stays open but the broadcast on this
node goes silent because the new leader is producing blocks elsewhere.

Client-side recovery relies on gRPC keepalive / SDK stream-level
reconnect logic detecting the silence and reopening against the
current leader (which will either serve locally or respond with
`NotLeader` redirect at reconnect time). The SDK's reconnect timeout
bounds the detection latency.

A future enhancement would instrument the stream with a leadership
watcher that terminates with `NotLeader` + hint as soon as the node
loses leadership, giving the client an immediate redirect signal
rather than waiting on keepalive. This is deferred; the current
keepalive-based recovery is correct but introduces up to one
keepalive-interval of staleness per leader flap.

### Cross-region redirect requires full resolve

When a client sends a request to region A that is actually for
region B, the server returns `NotLeader` with no leader hint (the
server resolving the routing doesn't know the target region's current
leader). The SDK falls back to `ResolveRegionLeader` to learn the
target leader. This costs one additional RPC round-trip compared to
the in-region follower-to-leader redirect, which does include a leader
hint. The extra round-trip is amortized across subsequent requests
once the `RegionLeaderCache` is warm for the target region.

## Related

- [Node Connection Registry Runbook](node-connection-registry.md) —
  server-side peer channel pool shared across subsystems
- [Leader Cache Diagnosis Runbook](leader-cache-diagnosis.md) —
  diagnosing SDK `RegionLeaderCache` health via Prometheus
- [Multi-Region Operations](../multi-region.md) — multi-region deployment
  patterns and organization routing
