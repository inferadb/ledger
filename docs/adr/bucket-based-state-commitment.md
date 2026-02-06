# ADR: Bucket-Based State Commitment

## Context

Each vault in InferaDB maintains a cryptographic `state_root` — a SHA-256 hash that represents the entire vault's state. This state root changes on every write and must be recomputed efficiently.

### The Problem

A naive approach recomputes the state root by hashing all entities in the vault:

```
state_root = SHA-256(entity_1 || entity_2 || ... || entity_n)
```

For a vault with 1M entities (~512 bytes each), this requires reading ~500 MB and hashing it on every commit. At 10 writes/second, this produces 5 GB/s of hash computation — impractical.

### Options Evaluated

| Option         | Complexity          | Memory       | Description                          |
| -------------- | ------------------- | ------------ | ------------------------------------ |
| Full rehash    | O(n)                | O(1)         | Hash all entities on every commit    |
| Merkle tree    | O(log n) per update | O(n)         | Tree of hashes over sorted entities  |
| Bucket hashing | O(k) per commit     | O(256 × 32B) | Fixed 256 buckets, rehash dirty only |

## Decision

**Use 256-bucket hashing with dirty tracking** (`VaultCommitment` in `crates/state/src/bucket.rs`).

### Algorithm

1. **Bucket assignment:** Each entity key is assigned to bucket `seahash(key) % 256`.
2. **Bucket root:** `bucket_root[i] = SHA-256(entity_1 || entity_2 || ...)` for all entities in bucket i, sorted by key.
3. **State root:** `state_root = SHA-256(bucket_root[0] || bucket_root[1] || ... || bucket_root[255])`.
4. **Dirty tracking:** On each write, `mark_dirty_by_key(key)` adds the bucket to a dirty set.
5. **Incremental update:** At commit, only dirty buckets are rehashed. The state root concatenates all 256 bucket roots (dirty = fresh, clean = cached).

### Why 256 Buckets?

- **Granularity vs overhead:** With 1M entities, each bucket averages ~3,900 entries. A write touching one key rehashes ~3,900 entries instead of 1M — a 256x improvement.
- **Fixed memory:** 256 × 32-byte hashes = 8 KB per vault. This fits in L1 cache and is included in snapshot metadata without concern.
- **Power of 2:** Single-byte bucket ID (`u8`) provides clean key encoding: `vault_id (8B) || bucket_id (1B) || local_key`.

### Why seahash for Bucket Assignment?

Seahash is a fast, non-cryptographic hash used only for uniform distribution across buckets. Cryptographic strength (SHA-256) is reserved for the commitment hashes where tamper resistance matters. Using SHA-256 for bucket assignment would waste ~10x more CPU for zero security benefit.

## Consequences

### Positive

- O(k) state root updates where k = dirty buckets (typically 1-5 per batch).
- Fixed 8 KB per-vault memory overhead regardless of entity count.
- Incremental snapshot restore — can load individual bucket roots.
- Simple implementation with clear correctness arguments.

### Negative

- Bucket rehash is O(bucket_size), not O(1). A single bucket with many entities (pathological key distribution) still requires significant work.
- 256 is a fixed constant — cannot be tuned per vault based on size. In practice, the seahash distribution is uniform enough that this is not a problem.
- Cannot produce efficient proofs for individual entities (unlike Merkle trees). Entity proofs use a separate `MerkleProof` mechanism over transaction batches.

### Neutral

- The bucket count (256) is hardcoded and cannot be changed without a storage format migration. Changing it would require rehashing all entities.
- Bucket roots are stored in snapshots, so snapshot format is coupled to the bucket count.

## References

- `crates/state/src/bucket.rs` — `VaultCommitment`, `BucketRootBuilder`
- `crates/types/src/hash.rs` — `bucket_id()`, `sha256_concat()`
- `DESIGN.md` §6 — State commitment specification
- `docs/specs/invariants.md` — Invariant #2 (state determinism), #11 (deterministic state roots)
