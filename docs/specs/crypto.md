# Cryptographic Specifications

Normative, language-independent specifications for all cryptographic hashes. Multi-SDK verification requires identical computation across implementations.

## Hash Algorithm

**SHA-256** for all cryptographic commitments. Outputs are 32 bytes.

## Block Hash

Commits to header only (not transaction bodies), enabling header-only verification:

```
block_hash = SHA-256(
    height              || # u64, big-endian (8 bytes)
    organization            || # i64, big-endian (8 bytes)
    vault               || # i64, big-endian (8 bytes)
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

| Condition      | Type Byte | Data                  |
| -------------- | --------- | --------------------- |
| (no condition) | `0x00`    | (none)                |
| MustNotExist   | `0x01`    | (none)                |
| MustExist      | `0x02`    | (none)                |
| VersionEquals  | `0x03`    | u64 BE version        |
| ValueEquals    | `0x04`    | u32 LE length + bytes |

Note: `0x00` represents `Option<SetCondition>::None`, not a SetCondition variant.

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

```
compute_bucket_root(entries):
    sort entries by key (lexicographic)
    hasher = SHA-256()
    for each (key, value, expires_at, version) in entries:
        hasher.update(length_of(key) as little-endian uint32)
        hasher.update(key)
        hasher.update(length_of(value) as little-endian uint32)
        hasher.update(value)
        hasher.update(expires_at as big-endian uint64)
        hasher.update(version as big-endian uint64)
    return hasher.finalize()  // 32 bytes
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
  DIRECTION_UNSPECIFIED = 0;
  DIRECTION_LEFT = 1;   // Sibling is on the LEFT
  DIRECTION_RIGHT = 2;  // Sibling is on the RIGHT
}

message MerkleProof {
  Hash leaf_hash = 1;
  repeated MerkleSibling siblings = 2;
}

message MerkleSibling {
  Hash hash = 1;
  Direction direction = 2;
}
```

### Verification Algorithm

```
verify_merkle_proof(leaf_hash, siblings, root):
    current = leaf_hash
    for each (sibling_hash, direction) in siblings:
        if direction == LEFT:
            current = SHA-256(sibling_hash || current)   // Sibling before current
        else:
            current = SHA-256(current || sibling_hash)   // Sibling after current
    return current == root
```

`LEFT` = sibling concatenated BEFORE current hash.
`RIGHT` = sibling concatenated AFTER current hash.

## State Proof Format

State proofs differ from transaction proofs—bucket-based hashing, not full Merkle tree.

```
BucketEntry:
    key:                bytes
    value:              bytes
    expires_at:         uint64
    version:            uint64

StateProof:
    target_key:         bytes            // The key being proved
    bucket_id:          uint8            // 0-255 (seahash(target_key) % 256)
    bucket_entries:     BucketEntry[]    // All entries in this bucket
    other_bucket_roots: Hash[255]        // All other bucket roots (32 bytes each)
```

The proof includes the full bucket contents so the verifier can recompute `bucket_root` independently.

### Verification

1. Verify `seahash(target_key) % 256 == bucket_id`
2. Verify `target_key` exists in `bucket_entries`
3. Recompute `bucket_root = compute_bucket_root(bucket_entries)`
4. Reconstruct all 256 bucket roots (computed root at `bucket_id`, provided roots elsewhere)
5. Verify `state_root == SHA-256(bucket_roots[0..256])`

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

## JWT Token Signing

Ledger signs JWT tokens using Ed25519 (EdDSA). This is separate from the data integrity hashing above — Ed25519 is used for token authentication, not for chain verification.

### Algorithm

**EdDSA with Ed25519** — the only supported algorithm. Enforced at two levels:

1. Raw JWT header pre-check: reject any `alg` value that is not exactly the string `"EdDSA"`
2. Library-level: configure the JWT validation library to accept only the EdDSA algorithm

### Key Identifier Format

Each signing key has a `kid` (Key ID) in UUID format. Non-UUID `kid` values are rejected before any state lookup to prevent cache pollution.

### Signing Key Envelope Encryption

Private key material is envelope-encrypted at rest using the Region Master Key (RMK):

```
┌─────────────────────────────────────────────────────────┐
│  SigningKeyEnvelope (100 bytes fixed)                    │
├─────────────────────────────────────────────────────────┤
│  wrapped_dek    : 40 bytes  (AES-KWP wrapped DEK)      │
│  nonce          : 12 bytes  (AES-256-GCM nonce)        │
│  ciphertext     : 32 bytes  (encrypted Ed25519 key)    │
│  auth_tag       : 16 bytes  (GCM authentication tag)   │
└─────────────────────────────────────────────────────────┘
```

**Encryption flow:**

1. Generate fresh DEK (32 bytes from CSPRNG)
2. Wrap DEK with RMK via AES-KWP
3. Generate nonce (12 bytes from CSPRNG) and encrypt Ed25519 private key (32 bytes) with DEK using AES-256-GCM
4. Use `kid` as AAD (additional authenticated data) to bind ciphertext to key identity

**Decryption flow:**

1. Unwrap DEK with RMK via `unwrap_dek(&wrapped_dek, rmk)`
2. Decrypt ciphertext with DEK using AES-256-GCM with `kid` as AAD
3. Return private key bytes in a zeroizing buffer (zeroized on deallocation/drop)

### Refresh Token Hash

Refresh tokens are hashed with SHA-256 for storage lookup:

```
token_string = "ilrt_" + base64url(32 CSPRNG bytes)     # 48 chars
token_hash   = SHA-256(token_string)                    # 32 bytes, stored in state
```

The `ilrt_` prefix (InferaDB Ledger Refresh Token) enables secret scanning tool detection.
