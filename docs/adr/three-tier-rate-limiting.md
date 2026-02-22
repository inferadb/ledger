# ADR: Three-Tier Rate Limiting

## Context

InferaDB Ledger runs multi-tenant shards where multiple organizations share a single Raft group. Without rate limiting, a single misconfigured client or a traffic spike in one organization can saturate the Raft proposal pipeline, degrading write latency for all tenants on the shard.

### The Problem

Rate limiting must address three distinct failure modes:

1. **Cluster overload** — the Raft consensus layer has too many pending proposals, regardless of source
2. **Noisy neighbor** — one organization generates disproportionate traffic, starving others
3. **Bad actor** — a single client within an organization sends excessive requests

A single-tier limit addresses only one of these. Two tiers leave one mode unprotected.

### Options Evaluated

| Option                 | Cluster Protection | Organization Isolation | Client Fairness |
| ---------------------- | ------------------ | ------------------- | --------------- |
| Global limit only      | Yes                | No                  | No              |
| Global + per-client    | Yes                | No                  | Yes             |
| Global + per-organization | Yes                | Yes                 | No              |
| Three-tier (chosen)    | Yes                | Yes                 | Yes             |

## Decision

**Implement three-tier admission control: global backpressure, per-client token bucket, per-organization token bucket** (`crates/raft/src/rate_limit.rs`).

### Tier 1: Global Backpressure

- **Mechanism:** Atomic comparison of pending Raft proposals against a threshold
- **Default threshold:** 100 pending proposals
- **Cost:** Single `AtomicU64` load — cheapest check, evaluated first
- **Rejection:** Returns `retry_after_ms = min((pending - threshold) × 10, 5000)`
- **Purpose:** Circuit-breaker for the entire shard when Raft is saturated. Prevents requests from queueing behind an already-deep proposal pipeline.

### Tier 2: Per-Client Token Bucket

- **Mechanism:** Token bucket per `client_id` string
- **Default capacity:** 100 tokens (burst allowance)
- **Default refill rate:** 50 tokens/sec (sustained throughput)
- **Precision:** Millisecond-granularity tokens (`u64 × 1000`) for sub-token accuracy
- **Purpose:** Prevents a single client from monopolizing resources. A misconfigured retry loop or a runaway batch job is contained to its client quota.

### Tier 3: Per-Organization Token Bucket

- **Mechanism:** Token bucket per `OrganizationSlug`
- **Default capacity:** 1,000 tokens (10× client burst)
- **Default refill rate:** 500 tokens/sec (10× client rate)
- **Purpose:** Ensures fair sharing across tenants. Even if an organization spawns many clients that individually stay under their limits, the aggregate is capped.

### Check Order

```
Request → Backpressure check → Per-client check → Per-organization check → Accept
              (cheapest)          (more specific)     (shared resource)
```

1. **Backpressure first** — single atomic load, no lock, no token consumption. If the cluster is overloaded, reject immediately without touching per-client/organization state.
2. **Per-client second** — checked before organization to avoid wasting shared organization tokens on requests that would be client-rejected anyway.
3. **Per-organization last** — shared resource consumed only after the client passes its individual check.

### Why Three Tiers Instead of Two?

**Without per-organization limits (global + per-client only):**

An organization spawns 1,000 unique `client_id` values, each staying under its 50 req/sec limit. Combined: 50,000 req/sec — far exceeding the organization's fair share of the shard. Other organizations on the same shard are starved.

**Without per-client limits (global + per-organization only):**

A single misbehaving client within an organization consumes the entire organization quota. Other legitimate clients in the same organization receive `RESOURCE_EXHAUSTED` errors for traffic they didn't cause. Per-client limits provide fairness _within_ an organization.

### Runtime Reconfiguration

All threshold values use `AtomicU64` for lock-free updates (floats stored via `f64::to_bits()/from_bits()`):

- `update_config()` atomically updates all thresholds
- Existing token buckets retain current token counts (gradual convergence to new rates)
- Integrated with `UpdateConfig` RPC and SIGHUP config reload

### Memory Management

Per-client and per-organization buckets are stored in `HashMap` protected by `parking_lot::Mutex`. Stale entries (clients that haven't sent traffic within `max_idle`) are cleaned up via `cleanup_stale()` to prevent unbounded growth.

## Consequences

### Positive

- **Defense in depth** — three independent failure modes covered by three independent mechanisms
- **Fair multi-tenancy** — organization limits prevent noisy-neighbor effects across tenants
- **Graceful degradation** — backpressure check is cheapest and fires first, shedding load before consuming tokens
- **Informative rejections** — `RESOURCE_EXHAUSTED` with `retry-after-ms` metadata enables intelligent client backoff
- **Runtime tunable** — operators can adjust limits without restart via `UpdateConfig` RPC or SIGHUP

### Negative

- **Three configuration surfaces** — operators must understand and tune three sets of limits (mitigated by sensible defaults)
- **Per-client HashMap growth** — under many unique `client_id` values, memory grows linearly (mitigated by stale entry cleanup)
- **Mutex on hot path** — per-client and per-organization checks acquire `parking_lot::Mutex` (uncontended: ~10–50ns, acceptable given Raft latency of 1–10ms)

### Neutral

- Token bucket refill is lazy — computed on check, not on a timer. No background threads needed.
- `retry_after_ms` is a hint, not a guarantee. Clients may retry earlier; the token bucket will reject again.
- Backpressure threshold tracks pending proposals via `set_pending_proposals()` — updated externally by the batch writer metrics observer.

## References

- `crates/raft/src/rate_limit.rs` — `RateLimiter`, `TokenBucket`, three-tier check logic
- `crates/types/src/config.rs` — `RateLimitConfig` with defaults and validation
- `crates/raft/src/services/write.rs` — `check_rate_limit()` integration, `RESOURCE_EXHAUSTED` mapping
- `crates/raft/src/runtime_config.rs` — `AtomicU64`-based runtime reconfiguration
- `DESIGN.md` §Write Path — "Rate limit check (3-tier: backpressure, organization, client)"
