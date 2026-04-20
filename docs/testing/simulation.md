# Simulation Testing

The consensus engine uses deterministic simulation testing. All nondeterminism (time, randomness, disk I/O) is abstracted behind injectable traits (`Clock`, `RngSource`, `WalBackend`), enabling reproducible multi-node tests in a single process.

## Running Simulation Tests

```bash
# Run every integration test in the consensus crate
cargo +1.92 test -p inferadb-ledger-consensus --tests

# Run a specific scenario by its integration-test binary name
cargo +1.92 test -p inferadb-ledger-consensus --test simulation_linearizability
cargo +1.92 test -p inferadb-ledger-consensus --test wal_durability

# Reproduce a failing seed (harness-provided scenarios accept `--seed`)
cargo +1.92 test -p inferadb-ledger-consensus --test simulation_linearizability -- --seed 42
```

## Simulation Scenarios

Integration tests live under `crates/consensus/tests/` (one file per `--test` target).

| Test target                  | File                                  | What it exercises                                                                       |
| ---------------------------- | ------------------------------------- | --------------------------------------------------------------------------------------- |
| `simulation_linearizability` | `tests/simulation_linearizability.rs` | Linearizable reads, stale reads, partition recovery, lease expiry under network faults. |
| `wal_durability`             | `tests/wal_durability.rs`             | WAL fsync semantics, corruption, and recovery paths.                                    |
| `wal_checkpoint`             | `tests/wal_checkpoint.rs`             | Checkpointer interaction with the WAL and recovery contract.                            |
| `election`                   | `tests/election.rs`                   | Leader election under partitions and clock skew.                                        |
| `replication`                | `tests/replication.rs`                | Log replication correctness and catch-up.                                               |
| `membership`                 | `tests/membership.rs`                 | Membership change safety (add/remove node).                                             |
| `engine`                     | `tests/engine.rs`                     | End-to-end engine state transitions.                                                    |
| `encryption_roundtrip`       | `tests/encryption_roundtrip.rs`       | Encrypted WAL round-trip.                                                               |

Each harness-driven test runs many seeded iterations with fault injection (partitions, crashes, clock skew, slow disk). A failing test prints the seed so you can reproduce the exact failure.
