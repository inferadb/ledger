# ADR: Moka TinyLFU for Idempotency Cache

## Context

InferaDB Ledger uses server-assigned sequences for write operations. Clients generate a UUID v4 idempotency key per write request. The server caches the result of each write so that retries (same key + same payload) return the cached result without re-executing the operation.

### Requirements

1. **Sub-microsecond lookups** — idempotency check is on the write hot path, before Raft proposal
2. **Bounded memory** — cache must not grow without limit under high write throughput
3. **TTL expiration** — entries should expire after a configurable period (client retry window)
4. **Concurrent access** — multiple gRPC handler threads check the cache simultaneously
5. **Payload integrity** — detect idempotency key reuse with different operation payloads

### Options Evaluated

| Option             | Read Latency | Memory Bound         | TTL Support | Operational Complexity       |
| ------------------ | ------------ | -------------------- | ----------- | ---------------------------- |
| Moka (TinyLFU)     | <100ns       | Yes (capacity + TTL) | Built-in    | None (in-process)            |
| Custom LRU         | <100ns       | Yes (capacity)       | Manual      | None, but maintenance burden |
| Redis              | 0.5–2ms      | Yes                  | Built-in    | High (separate cluster)      |
| Persistent storage | 1–10ms       | Disk-bound           | Manual      | Medium (compaction, GC)      |

## Decision

**Use moka with TinyLFU eviction for in-memory idempotency caching** (`crates/raft/src/idempotency.rs`).

### Configuration

- **Library:** `moka = { version = "0.12", features = ["sync"] }`
- **TTL:** 24 hours — matches typical client retry windows with 10–100× safety margin
- **Max capacity:** 100,000 entries
- **Eviction policy:** TinyLFU admission + LRU eviction (moka's default)

### Cache Key

```rust
pub struct IdempotencyKey {
    pub organization_slug: i64,
    pub vault_id: i64,
    pub client_id: String,
    pub idempotency_key: [u8; 16], // UUID v4
}
```

The 4-tuple provides isolation: the same UUID from different clients or organizations maps to different cache entries.

### Payload Integrity

Each cached entry stores a `request_hash: u64` computed via seahash over the concatenated operation fields. On cache hit:

- **Same key + same hash → `Duplicate`** — return cached `WriteSuccess`
- **Same key + different hash → `KeyReused`** — return error (client bug: reusing a UUID with different operations)

### Why TinyLFU Over LRU?

TinyLFU achieves ~85% hit rate versus ~60% for LRU on frequency-biased workloads. Idempotency workloads are inherently frequency-biased: retries concentrate on recently failed writes, while successfully acknowledged writes are rarely seen again.

TinyLFU's admission policy evaluates new entries against their estimated frequency before accepting them into the cache. This prevents a burst of unique one-time keys (e.g., successful writes that will never be retried) from evicting frequently-retried keys — a problem known as "cache pollution" in LRU.

Moka implements TinyLFU with a modified Count-Min Sketch for frequency estimation, requiring minimal additional memory beyond the cache entries themselves.

### Why Not Redis?

- **Latency.** Redis adds 0.5–2ms network RTT per check. At 100k writes/sec, this would require ~100 CPU cores just waiting on cache checks. In-memory moka: <100ns.
- **Operational complexity.** Redis requires a separate HA cluster with monitoring, backups, and failover configuration — significant operational burden for a cache with 24-hour TTL.
- **Single-leader architecture.** All writes flow through a single Raft leader. The leader's local cache handles all idempotency checks — no cross-node coordination needed.
- **No durability requirement.** The cache is a performance optimization, not a correctness mechanism (see "Failover Behavior" below).

### Why Not Persistent Storage?

- **Disk I/O on every write.** Checking a persistent store adds 1–10ms to the write path — unacceptable for sub-50ms p99 write latency target.
- **Compaction complexity.** TTL-based entries in persistent storage require background compaction or garbage collection.
- **No correctness benefit.** Idempotency is best-effort with TTL. After TTL expiry, a duplicate write succeeds as a new write regardless of storage durability.

### Two-Tier Lookup

The moka cache is the **fast-path** (Tier 1) in a two-tier idempotency system:

1. **Tier 1 — Moka TinyLFU cache** (in-memory, sub-microsecond): Holds full `WriteSuccess` results for the hottest entries. This is the primary deduplication path during normal operation.
2. **Tier 2 — `ClientSequences` B+ tree table** (replicated, persistent): Stores `ClientSequenceEntry` records (sequence number, last seen timestamp, last idempotency key hash, last request hash) in the replicated state. Survives leader failover within the TTL window (default 24 hours).

**Lookup order:**

1. Check moka cache — hit returns cached `WriteSuccess` immediately (fast path)
2. On moka miss, check `ClientSequences` table — hit returns `ALREADY_COMMITTED` (no full `WriteSuccess` available, but confirms the write was committed)
3. Both miss — proceed with new Raft proposal

**TTL eviction** of `ClientSequenceEntry` records is deterministic: triggered when `last_applied.index % eviction_interval == 0`, ensuring all replicas evict identical entries at the same log index. See [Configuration](../operations/configuration.md#client-sequence-eviction) for tuning parameters.

### Failover Behavior

The moka cache is **in-memory only** and does not survive leader failover. However, the persistent `ClientSequences` table provides cross-failover deduplication:

- On leader failover, the new leader starts with an empty moka cache but has access to all `ClientSequenceEntry` records in the replicated `ClientSequences` table
- A client retrying a pre-failover write will be deduplicated via Tier 2 (returns `ALREADY_COMMITTED` within the 24-hour TTL window)
- After TTL expiry, a duplicate write succeeds as a new write regardless — this is acceptable because the retry window is orders of magnitude shorter than TTL

Raft log replay does not repopulate the moka cache — the persistent table provides sufficient cross-failover guarantees without adding disk I/O to every write's hot path.

### Memory Footprint

```
Per entry:
  IdempotencyKey:  ~56 bytes (organization_slug + vault_id + client_id String + UUID)
  CachedResult:    ~64 bytes (request_hash + WriteSuccess fields)
  Moka overhead:   ~28 bytes (LRU pointers, hash table entry, TinyLFU sketch)
  Total:           ~148 bytes

At 100k entries:   ~14.8 MB
TinyLFU sketch:    ~1-2 MB (shared)
Grand total:       ~16-17 MB
```

This is negligible compared to typical Raft node memory (multi-GB for page cache and Raft log buffers).

## Consequences

### Positive

- **Sub-microsecond lookups** — lock-free concurrent hash table with TinyLFU admission
- **Bounded memory** — max 100k entries + 24-hour TTL ensures predictable resource usage
- **Zero operational overhead** — in-process, no external dependencies, no configuration beyond TTL and capacity
- **Cache pollution resistant** — TinyLFU admission prevents one-time keys from evicting retried keys
- **Payload integrity** — seahash detects idempotency key reuse with different operations

### Negative

- **Moka cache does not survive leader failover** — new leader starts with empty moka cache (Tier 2 persistent table provides cross-failover deduplication within TTL window)
- **Single-node scope** — moka cache is local to the leader; followers don't participate (acceptable because followers don't process writes, and the persistent `ClientSequences` table is replicated)
- **UUID collision assumption** — relies on UUID v4's 122 bits of entropy; at 100k writes/sec × 24h ≈ 8.64B entries, collision probability is ~10⁻²⁰

### Neutral

- TTL expiration is lazy (triggered by cache operations, not a background timer). Entries may persist slightly beyond 24 hours if the cache is idle.
- Cache size is reported via `entry_count()` for monitoring; no built-in Prometheus integration (metrics emitted by the service layer).

## References

- `crates/raft/src/idempotency.rs` — `IdempotencyCache`, `IdempotencyKey`, `CachedResult`
- `crates/raft/src/services/write.rs` — `hash_operations()`, idempotency check before Raft proposal
- `docs/adr/server-assigned-sequences.md` — Context for server-assigned idempotency model
- `Cargo.toml` — `moka = { version = "0.12", features = ["sync"] }`
- O'Neil, O'Neil, Gerber, "The LRU-K Page Replacement Algorithm For Database Disk Buffering" (1993) — LRU limitations
- Einziger, Friedman, Manes, "TinyLFU: A Highly Efficient Cache Admission Policy" (2017) — TinyLFU design
