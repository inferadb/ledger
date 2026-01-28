# Cryptographic Specifications

Normative, language-independent specifications for all cryptographic hashes. Multi-SDK verification requires identical computation across implementations.

## Hash Algorithm

**SHA-256** for all cryptographic commitments. Outputs are 32 bytes.

## Block Hash

Commits to header only (not transaction bodies), enabling header-only verification:

```
block_hash = SHA-256(
    height              || # u64, big-endian (8 bytes)
    namespace_id        || # i64, big-endian (8 bytes)
    vault_id            || # i64, big-endian (8 bytes)
    previous_hash       || # 32 bytes (zero-hash for genesis)
    tx_merkle_root      || # 32 bytes
    state_root          || # 32 bytes
    timestamp_secs      || # i64, big-endian (8 bytes)
    timestamp_nanos     || # u32, big-endian (4 bytes)
    term                || # u64, big-endian (8 bytes)
    committed_index        # u64, big-endian (8 bytes)
)
# Total: 148 bytes fixed-size input
```

**Genesis block**: `previous_hash` is 32 zero bytes.

## Transaction Hash

Canonical binary encoding independent of protobuf wire format:

```
tx_hash = SHA-256(
    tx_id               || # 16 bytes (UUID)
    client_id_len       || # u32, little-endian
    client_id           || # UTF-8 bytes
    sequence            || # u64, big-endian
    actor_len           || # u32, little-endian
    actor               || # UTF-8 bytes
    op_count            || # u32, little-endian
    operations          || # Inline operation encoding (type byte + fields)
    timestamp_secs      || # i64, big-endian
    timestamp_nanos        # u32, big-endian
)
```

Operations are encoded inline (not as separate hashes) with a type byte followed by type-specific fields.

## Operation Encoding

Operations are encoded inline within the transaction hash (not as separate hashes).

**op_type** (single byte):

| Type               | Value  |
| ------------------ | ------ |
| CreateRelationship | `0x01` |
| DeleteRelationship | `0x02` |
| SetEntity          | `0x03` |
| DeleteEntity       | `0x04` |
| ExpireEntity       | `0x05` |

**op_data by type**:

```
CreateRelationship:
    resource_len (u32 LE) || resource ||
    relation_len (u32 LE) || relation ||
    subject_len (u32 LE) || subject

DeleteRelationship:
    resource_len (u32 LE) || resource ||
    relation_len (u32 LE) || relation ||
    subject_len (u32 LE) || subject

SetEntity:
    key_len (u32 LE) || key || value_len (u32 LE) || value ||
    condition_type (u8) || condition_data ||
    expires_at (u64 BE, 0 = never)

DeleteEntity:
    key_len (u32 LE) || key

ExpireEntity:
    key_len (u32 LE) || key || expired_at (u64 BE)
```

Note: SetEntity encodes condition before expires_at.

**Condition types** for SetEntity:

| Type          | Value  | Data                  |
| ------------- | ------ | --------------------- |
| None          | `0x00` | (none)                |
| MustNotExist  | `0x01` | (none)                |
| MustExist     | `0x02` | (none)                |
| VersionEquals | `0x03` | u64 BE version        |
| ValueEquals   | `0x04` | u32 LE length + bytes |

## Transaction Merkle Tree

Standard binary Merkle tree over transaction hashes:

1. Leaves are transaction hashes in block order
2. Odd nodes: duplicate last node
3. Parent = SHA-256(left_child || right_child)
4. Continue until single root

```
Example: 3 transactions [A, B, C]

Level 0 (leaves):  A    B    C    C'   (C duplicated)
                    \  /      \  /
Level 1:           AB          CC
                     \        /
Level 2 (root):      tx_merkle_root
```

**Empty block**: `tx_merkle_root` = `SHA-256("")`

## State Tree

### Leaf Contribution

Each key-value pair contributes to its bucket:

```
leaf_contribution = (
    key_len (u32 LE)    ||  # 4 bytes
    key                 ||  # Variable
    value_len (u32 LE)  ||  # 4 bytes
    value               ||  # Variable
    expires_at (u64 BE) ||  # 8 bytes (0 = never)
    version (u64 BE)        # 8 bytes (block height of last modification)
)
```

### Bucket Root

Incremental hash of all leaf contributions in lexicographic key order:

```rust
fn compute_bucket_root(entries: &[(Key, Value, ExpiresAt, Version)]) -> Hash {
    let mut hasher = Sha256::new();
    for (key, value, expires_at, version) in entries.iter().sorted_by_key(|e| &e.0) {
        hasher.update(&(key.len() as u32).to_le_bytes());
        hasher.update(key);
        hasher.update(&(value.len() as u32).to_le_bytes());
        hasher.update(value);
        hasher.update(&expires_at.to_be_bytes());
        hasher.update(&version.to_be_bytes());
    }
    hasher.finalize().into()
}
```

**Empty bucket**: `SHA-256("")`

### State Root

SHA-256 of concatenated bucket roots (256 buckets × 32 bytes = 8192 bytes):

```
state_root = SHA-256(bucket_root[0] || bucket_root[1] || ... || bucket_root[255])
```

**Bucket assignment**: `bucket_id = seahash(key) % 256`

## Merkle Proof Format

```protobuf
enum Direction {
  DIRECTION_LEFT = 1;   // Sibling is on the LEFT
  DIRECTION_RIGHT = 2;  // Sibling is on the RIGHT
}

message MerkleProof {
  bytes leaf_hash = 1;
  repeated MerkleNode siblings = 2;
}

message MerkleNode {
  bytes hash = 1;
  Direction direction = 2;
}
```

### Verification Algorithm

```rust
fn verify_merkle_proof(leaf_hash: Hash, siblings: &[(Hash, Direction)], root: Hash) -> bool {
    let mut current = leaf_hash;
    for (sibling, direction) in siblings {
        current = match direction {
            Direction::Left  => sha256(sibling || current),  // Sibling LEFT of current
            Direction::Right => sha256(current || sibling),  // Sibling RIGHT of current
        };
    }
    current == root
}
```

`Direction::Left` = sibling concatenated BEFORE current hash.
`Direction::Right` = sibling concatenated AFTER current hash.

## State Proof Format

State proofs differ from transaction proofs—bucket-based hashing, not full Merkle tree.

```rust
struct StateProof {
    key: Vec<u8>,
    value: Vec<u8>,
    expires_at: u64,
    version: u64,
    bucket_id: u8,
    bucket_root: Hash,               // Computed from bucket contents
    other_bucket_roots: [Hash; 255], // All other bucket roots
}
```

### Verification

1. Verify `seahash(key) % 256 == bucket_id`
2. Verify `bucket_root` matches by hashing all bucket contents (requires full bucket)
3. Verify `state_root == SHA-256(bucket_roots[0..256])` with provided roots

### Trade-offs

State proofs require full bucket contents (not O(log n) like Merkle trees):

| Aspect             | Implication                                     |
| ------------------ | ----------------------------------------------- |
| Bucket size        | ~4K entries each for 1M total keys              |
| Proof frequency    | Rare (verification usually via trusted headers) |
| Transaction proofs | Remain O(log n)                                 |

## Verification Scope

| Operation               | Verifiable | Proof Type                |
| ----------------------- | ---------- | ------------------------- |
| Point read (single key) | Yes        | StateProof                |
| Historical point read   | Yes        | StateProof + ChainProof   |
| Transaction inclusion   | Yes        | MerkleProof               |
| Write committed         | Yes        | BlockHeader + TxProof     |
| List relationships      | No         | Trust server              |
| List entities           | No         | Trust server              |
| Pagination completeness | No         | Cannot prove no omissions |

For verifiable list operations, maintain your own state via `WatchBlocks` subscription.
