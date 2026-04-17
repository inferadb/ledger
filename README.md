<div align="center">
    <p><a href="https://inferadb.com"><img src=".github/inferadb.png" width="100" alt="InferaDB Logo" /></a></p>
    <h1>InferaDB Ledger</h1>
    <p>
        <a href="https://discord.gg/inferadb"><img src="https://img.shields.io/badge/Discord-Join%20us-5865F2?logo=discord&logoColor=white" alt="Discord" /></a>
        <a href="#license"><img src="https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg" alt="License" /></a>
        <a href="https://github.com/inferadb/ledger/actions"><img src="https://img.shields.io/github/actions/workflow/status/inferadb/ledger/ci.yml?branch=main" alt="CI" /></a>
    </p>
    <p><b>Blockchain storage for cryptographically verifiable authorization.</b></p>
</div>

> [!IMPORTANT]
> Under active development. Not production-ready.

**[InferaDB](https://inferadb.com) Ledger is a distributed database purpose-built for authorization.** Every permission change is committed to an append-only blockchain, replicated via a custom multi-shard Raft consensus engine, and independently verifiable by clients through Merkle proofs — giving you a tamper-proof record of who had access to what, when. Ledger is the storage layer behind [InferaDB Engine](https://github.com/inferadb/engine) and [InferaDB Control](https://github.com/inferadb/control).

- [Features](#features)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Development](#development)
- [Documentation](#documentation)
- [Community](#community)
- [License](#license)

## Features

- **Tamper-Proof Authorization History** — Every permission change is committed to a per-vault blockchain with consensus-verified block hashes. Not even database administrators can retroactively alter who had access to what, when.
- **Client-Side Proof Verification** — Clients receive Merkle proofs with every read and can verify authorization decisions independently, without trusting the server. Proofs are pre-computed during apply for near-instant verified reads.
- **Custom Consensus Engine** — Purpose-built multi-shard Raft engine with an event-driven reactor, pipelined replication, zero-copy rkyv serialization, and batched I/O across shards. Single WAL fsync on the consensus critical path.
- **Data Residency** — Pin authorization data to geographic regions. Nodes only join Raft groups for their assigned region, keeping data within jurisdictional boundaries. Automatic region shard creation and membership management.
- **Tenant Isolation** — Per-organization, per-vault security boundaries with per-vault WAL frame encryption (AES-256-GCM). Each vault maintains its own blockchain — one tenant's data can never leak into another's.
- **Immediate Consistency** — Raft consensus ensures permission changes are visible cluster-wide before the write returns. Closed timestamps enable zero-hop follower reads for bounded-staleness queries.
- **Sub-Millisecond Reads** — B+ tree indexes serve lookups without touching the Merkle layer. Leader lease reads at ~50ns, follower closed-timestamp reads at ~100ns.

## Quick Start

**Start a node:**

```bash
inferadb-ledger --listen 0.0.0.0:9090 --data /var/lib/ledger
```

**Bootstrap the cluster (once, from any machine):**

```bash
inferadb-ledger init --host node1:9090
```

**Add more nodes:**

```bash
inferadb-ledger \
  --listen 0.0.0.0:9090 \
  --data /var/lib/ledger \
  --join node1:9090
```

Nodes discover each other via `--join` seed addresses. The cluster manages membership automatically — new nodes are added as learners and promoted to voters once caught up. On restart, only `--data` is required; peer addresses are read from persisted Raft membership state.

**Data residency (regulated regions):**

```bash
inferadb-ledger \
  --listen 0.0.0.0:9090 \
  --data /var/lib/ledger \
  --join node1:9090 \
  --region ie-east-dublin
```

See the [deployment guide](docs/operations/deployment.md) for multi-node setup, Kubernetes, adding/removing nodes, backup, and recovery.

## Configuration

| CLI           | Purpose                                                                                                  | Default         |
| ------------- | -------------------------------------------------------------------------------------------------------- | --------------- |
| `--data`      | Persistent [storage](docs/internals/storage.md#directory-layout) (WAL, state, snapshots)                 | _(ephemeral)_   |
| `--listen`    | TCP address for gRPC API                                                                                 | _(none)_        |
| `--socket`    | Unix domain socket path for gRPC API                                                                     | _(none)_        |
| `--join`      | Seed addresses for [cluster discovery](docs/operations/deployment.md#adding-a-node) (comma-separated)    | _(none)_        |
| `--region`    | Geographic data residency [region](docs/operations/deployment.md)                                        | `global`        |
| `--advertise` | Address advertised to peers ([details](docs/operations/deployment.md#advertise-address))                 | _(auto-detect)_ |

At least one of `--listen` or `--socket` must be specified. On restart, only `--data` is required. All other flags are persisted on first boot and ignored on subsequent starts.

See [Configuration Reference](docs/operations/deployment.md#configuration-reference) for environment variables and all options including metrics, batching, and tuning.

## Contributing

### Prerequisites

- Rust 1.92+
- [mise](https://mise.jdx.dev/) for synchronized development tooling
- [just](https://github.com/casey/just) for convenient development commands

### Build and Test

```bash
git clone https://github.com/inferadb/ledger.git
cd ledger

# Install development tools
mise trust && mise install

# Build
just build

# Run tests
just test
```

## Documentation

- [Technical White Paper](WHITEPAPER.md) — Start here to understand how Ledger works, see benchmark results, and evaluate whether it fits your use case
- [Technical Design Document](DESIGN.md) — Authoritative specification for contributors; explains the reasoning behind architectural decisions

## Community

Join us on [Discord](https://discord.gg/inferadb) for questions and discussions.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE).
