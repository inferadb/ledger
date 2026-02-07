# ADR: Count-Min Sketch for Hot Key Detection

## Context

InferaDB Ledger operates in multi-tenant environments where a single hot key (frequently written entity or relationship) can saturate a shard's Raft proposal pipeline, degrading latency for all tenants. Detecting hot keys at the write path requires tracking access frequency across potentially millions of unique keys.

### The Problem

Given N unique entity keys written per window, detect which keys exceed a configurable operations-per-second threshold. The detector must:

1. Run on every write operation (hot path — Raft proposal latency is 1–10ms)
2. Use bounded memory regardless of key cardinality (protects against adversarial workloads)
3. Never miss a truly hot key (false negatives unacceptable for operational monitoring)
4. Accept occasional false positives (over-counting is safe — triggers investigation, not data loss)

### Options Evaluated

| Option                   | Memory                 | Per-Operation Cost | False Negatives                                  | False Positives    |
| ------------------------ | ---------------------- | ------------------ | ------------------------------------------------ | ------------------ |
| Exact HashMap            | O(n) keys              | O(1) amortized     | None                                             | None               |
| Count-Min Sketch + Top-K | O(width × depth) fixed | O(depth)           | None                                             | ~0.26% at defaults |
| Sampling (1% of keys)    | O(sample)              | O(1)               | Up to 99%                                        | None               |
| Top-K heap alone         | O(k)                   | O(log k)           | Cannot determine entry without counting all keys | N/A                |

## Decision

**Use Count-Min Sketch with a rotating window pair and Top-K heap** (`crates/raft/src/hot_key_detector.rs`).

### Algorithm

1. **CMS Matrix:** `depth` rows × `width` columns of `u64` counters (flat `Vec<u64>` for cache locality). Default: 4 × 1024 = 4,096 counters.
2. **Hash Functions:** `depth` independent hash functions using deterministic seeds derived from row index: `0x517cc1b727220a95.wrapping_mul(i + 1)`.
3. **Record Access:** For each key, increment `depth` counters (one per row). Point query returns the minimum across all rows — this never underestimates.
4. **Rotating Windows:** Two CMS instances (`current` and `previous`). On window expiry (default 60s), swap and clear — O(1) rotation, fixed memory.
5. **Top-K Tracking:** HashMap for O(1) lookup + BinaryHeap (min-heap) for eviction of coldest entries. Default k = 10.
6. **Threshold Check:** If `min_count / window_seconds > threshold` (default 100 ops/sec), key is flagged as hot.

### Memory Footprint

```
Per CMS:    width × depth × 8 bytes = 1024 × 4 × 8 = 32 KB
Two windows: 2 × 32 KB = 64 KB
Top-K state: ~480 bytes (negligible)
Total:       ~64 KB (independent of key cardinality)
```

Compare: exact HashMap with 1M unique keys × 32-byte average key ≈ 50 MB minimum.

### Why Not Exact HashMap?

- **Unbounded memory growth.** Under adversarial workloads (key cardinality attack), a HashMap grows without limit. An attacker generating unique keys at 100k/sec would consume ~3.2 MB/sec in HashMap entries alone.
- **Garbage collection needed.** Stale entries must be evicted periodically, adding complexity. CMS window rotation handles this implicitly.
- **Acceptable accuracy trade-off.** Hot key detection is operational monitoring, not a correctness requirement. Over-counting a near-threshold key triggers investigation — a minor cost. Under-counting (which CMS guarantees never happens) would miss real problems.

### Why Not Top-K Heap Alone?

A top-k heap cannot determine if a _new_ key should enter the heap without knowing its count. Without CMS, you need either:

- A HashMap of all keys (same unbounded memory problem)
- Sampling (misses truly hot keys that aren't sampled)

CMS provides frequency estimation for _every_ key with bounded memory, enabling informed threshold decisions.

### Why Rotating Window Pair?

| Approach                    | Memory    | Rotation Cost                    | Semantics                       |
| --------------------------- | --------- | -------------------------------- | ------------------------------- |
| Single CMS with decay       | 1× sketch | O(width × depth) per decay cycle | Imprecise window boundaries     |
| Ring buffer of mini-windows | M× sketch | O(1) swap                        | O(M × depth) per query          |
| Rotating pair (chosen)      | 2× sketch | O(1) swap                        | Clear current/previous boundary |

The rotating pair provides fixed 2× memory, O(1) rotation via `swap()`, and clear "current window" semantics for rate calculation.

### Error Bounds

With default configuration (`width=1024`, `depth=4`):

- Error rate `ε = e / width ≈ 0.0026` (0.26%)
- Confidence: `1 - (1/e)^4 ≈ 98.2%`
- Collision probability across all rows: `(1/1024)^4 ≈ 10^-12` per key pair

In practice: keys near the threshold may occasionally be flagged. This is acceptable — false positives trigger a WARN log and Prometheus metric, not data loss or request rejection.

### Prometheus Label Strategy

Entity keys are arbitrary strings with potentially unbounded cardinality. Emitting raw keys as Prometheus labels would cause cardinality explosion. Instead:

```
key_hash = format!("{:016x}", seahash::hash(key.as_bytes()))
```

Seahash produces a 64-bit hash formatted as a fixed 16-character hex string. Operators correlate `key_hash` labels with WARN logs that include the full key string.

## Consequences

### Positive

- **Bounded memory** (64 KB default) regardless of key cardinality — safe under adversarial workloads
- **Never underestimates** — CMS guarantee ensures truly hot keys are always detected
- **Hot-path performance** — ~100–200ns per write operation (4 hashes + 4 counter increments), negligible vs Raft latency
- **Runtime reconfigurable** — `window_secs` and `threshold` use `AtomicU64` for lock-free updates via `UpdateConfig` RPC

### Negative

- **May over-count** near threshold due to hash collisions — triggers investigation for non-hot keys
- **CMS dimensions not runtime-reconfigurable** — changing `width` or `depth` requires restart (reallocates counter arrays)
- **Sliding window is approximate** — rotating pair means a key active only at window boundaries may be under-counted by up to 50%

### Neutral

- Top-K heap provides ranked reporting of hottest keys; CMS provides detection. Both are needed — neither alone is sufficient.
- `parking_lot::Mutex` protects the detector state; uncontended lock acquisition is ~10–50ns.

## References

- `crates/raft/src/hot_key_detector.rs` — CMS implementation, rotating windows, top-k tracking
- `crates/types/src/config.rs` — `HotKeyConfig` with defaults and validation
- `crates/raft/src/services/write.rs` — `record_hot_keys()` integration in write path
- `crates/raft/src/services/multi_shard_write.rs` — Multi-shard coordinator integration
- `crates/raft/src/metrics.rs` — `record_hot_key_detected()` with seahash label
- Cormode & Muthukrishnan, "An Improved Data Stream Summary: The Count-Min Sketch and its Applications" (2005)
