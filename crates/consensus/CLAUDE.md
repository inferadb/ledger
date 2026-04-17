# consensus

Multi-shard Raft, event-driven reactor, segmented WAL, pipelined replication. The production system is **always** multi-Raft — never describe or design around a single-Raft assumption.

## Key types

| Type | File | Purpose |
| --- | --- | --- |
| `ConsensusEngine` | `lib.rs` | Multi-shard API: `propose`, `read_index`, membership |
| `Reactor` | `reactor.rs` | Single-task event loop; batches WAL writes and network sends |
| `Shard` | `shard.rs` | Single Raft instance; event-driven — returns `Action` values, performs no I/O |
| `WalBackend` trait | `wal_backend.rs` + `wal/{segmented,encrypted,memory,io_uring_backend}.rs` | Pluggable WAL |

## Invariants

- **Shards never perform I/O.** They return `Action` values; the Reactor executes them. Any code inside `Shard` that blocks, reads disk, or sends over the network is a bug.
- **WAL writes are batched with a single fsync per batch.** Never fsync per proposal — that kills throughput.
- **Production WAL** is per-vault AES-256-GCM segmented. Other backends (`memory`, `io_uring`) exist for tests and experiments.
- **Pipelined replication** overlaps proposal batches with follower ack — don't serialize accidentally.

## openraft notes

- openraft 0.9 has no `transfer_leader()`. For graceful shutdown: trigger a final snapshot and shut down — re-election happens automatically.
- `LogId::new(CommittedLeaderId::new(term, node_id), index)` — never `LogId::new(term, index)`. The wrong shape compiles but breaks log comparisons.

## Shard assignment

Orgs are grouped into shards by `ShardManager` (lives in `state`, not here). `ConsensusEngine` is given the shard set at startup and routes proposals to the right `Shard`.

## Related tooling

- Agent: `consensus-reviewer` (audits Raft/WAL invariants on every change to this crate)
