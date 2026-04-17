# Property Testing

Property tests use [proptest](https://proptest-rs.github.io/proptest/) to verify invariants across randomly generated inputs. Unlike fuzz tests (which target crash-inducing inputs), property tests assert logical properties that must hold for all valid inputs.

## Running Property Tests

```bash
# Run with default iterations (256 cases per test)
cargo +1.92 test --workspace --lib -- proptest

# Run with high iteration count (10k cases per test)
just test-proptest

# Custom iteration count
just test-proptest 50000
```

## Test Suites

| Suite                  | Crate       | Properties                                                                                                                                                            |
| ---------------------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Codec roundtrip        | `types`     | Encode/decode cycle preserves all serializable types (Operation, Entity, Relationship, Transaction, BlockHeader, VaultBlock, VaultEntry, ShardBlock, ChainCommitment) |
| Merkle proofs          | `types`     | Valid proofs always verify, tampered proofs never verify, wrong roots never verify, root computation is deterministic                                                 |
| B+ tree invariants     | `store`     | Inserted keys retrievable, iteration returns sorted keys, deletes remove only target keys, updates overwrite correctly, get/iteration consistency                     |
| Consensus determinism  | `consensus` | Same operations on two independent engines produce identical state, encrypted WAL roundtrips correctly, block hashes are deterministic across nodes                   |
| WAL integrity          | `consensus` | Frame write/read roundtrip, segment rotation preserves entries, CRC catches corruption, crash recovery replays correctly                                              |
| Split/merge roundtrip  | `consensus` | Split followed by merge preserves all data, router updates are consistent, organization boundaries are respected                                                      |

## Writing New Property Tests

1. Add strategies to `crates/test-utils/src/strategies.rs` for reusable input generators
2. Place property tests in a `proptest_*` inner module within the test module of the target crate
3. Name test functions with a `prop_` prefix so `cargo test -- proptest` filters correctly
4. Use `inferadb-ledger-test-utils` strategies where possible to avoid duplication

## CI

Property tests run at default iterations (256) on every PR via the standard CI workflow. A dedicated nightly workflow (`.github/workflows/proptest.yml`) runs 10,000 iterations per test to catch rare edge cases.
