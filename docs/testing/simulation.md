# Simulation Testing

The consensus engine uses deterministic simulation testing. All nondeterminism (time, randomness, disk I/O) is abstracted behind injectable traits (`Clock`, `RngSource`, `WalBackend`), enabling reproducible multi-node tests in a single process.

## Running Simulation Tests

```bash
# Run all simulation tests (default: 1000 seeds)
cargo +1.92 test -p inferadb-ledger-consensus --test simulation

# Run a specific simulation scenario
cargo +1.92 test -p inferadb-ledger-consensus --test simulation linearizability

# Reproduce a failing seed
cargo +1.92 test -p inferadb-ledger-consensus --test simulation -- --seed 42
```

## Simulation Scenarios

| Scenario          | What it tests                                                      |
| ----------------- | ------------------------------------------------------------------ |
| `linearizability` | Stale reads, partition recovery, lease expiry under network faults |
| `split_merge`     | Split/merge under crash, partition, clock skew                     |
| `durability`      | WAL corruption, sync failures, recovery paths                      |
| `closed_ts`       | Closed timestamp correctness under leader transitions              |

Each test runs thousands of seeded iterations with fault injection (partitions, crashes, clock skew, slow disk). A failing test produces a seed that reproduces the exact failure.
