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
    pub namespace_id: i64,
    pub vault_id: i64,
    pub client_id: String,
    pub idempotency_key: [u8; 16], // UUID v4
}
```

The 4-tuple provides isolation: the same UUID from different clients or namespaces maps to different cache entries.

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

### Failover Behavior

The idempotency cache is **in-memory only** and does not survive leader failover. This is a documented and tested trade-off:

- On leader failover, the new leader starts with an empty cache
- A client retrying a pre-failover write will execute it as a new write
- This is acceptable because: (a) failover is rare, (b) the client's retry window is short relative to TTL, (c) application-level idempotency (via conditional writes with `SetCondition::IfNotExists`) provides stronger guarantees when needed

Raft log replay does not repopulate the cache — doing so would require persisting per-entry hashes and results, adding disk I/O to every write for a rare edge case.

### Memory Footprint

```
Per entry:
  IdempotencyKey:  ~56 bytes (namespace_id + vault_id + client_id String + UUID)
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

- **Does not survive leader failover** — new leader starts with empty cache; pre-failover retries may execute as new writes
- **Single-node scope** — cache is local to the leader; followers don't participate (acceptable because followers don't process writes)
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
