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

[InferaDB](https://inferadb.com) Ledger is a distributed blockchain database optimized for authorization workloads. It commits every state change cryptographically, replicates via Raft consensus, and lets clients verify independently. Ledger is the persistent storage layer used by the [InferaDB Engine](https://github.com/inferadb/engine) and [InferaDB Control](https://github.com/inferadb/control).

- [Features](#features)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Development](#development)
- [Documentation](#documentation)
- [Community](#community)
- [License](#license)

## Features

- **Sub-millisecond Reads** — O(1) lookups via B+ tree indexes, no merkle overhead on hot path
- **Cryptographic Auditability** — Per-vault blockchain with chain-linked state roots, SHA-256 commitments, tamper-evident history
- **Strong Consistency** — Raft consensus ensures permission changes are immediately visible cluster-wide
- **Fault Isolation** — Per-vault chains prevent failures from cascading across tenants
- **Horizontal Scaling** — Shard groups distribute organizations across independent Raft clusters

## Quick Start

**Development or single-server deployment:**

```bash
inferadb-ledger --data /var/lib/ledger --single
```

**Production cluster (run on each of 3 nodes):**

```bash
inferadb-ledger --data /var/lib/ledger --cluster 3 --peers ledger.example.com
```

For clusters, `--peers` tells each node how to find the others. Pass one of:

- **DNS domain** (e.g., `ledger.example.com`) — looks up A records
- **File path** (e.g., `/var/lib/ledger/peers.json`) — reads addresses from JSON

See the [deployment guide](docs/operations/deployment.md) for multi-node setup, Kubernetes, adding/removing nodes, backup, and recovery.

## Configuration

| CLI         | Purpose                                                                                                | Default           |
| ----------- | ------------------------------------------------------------------------------------------------------ | ----------------- |
| `--listen`  | Bind address for gRPC API                                                                              | `127.0.0.1:50051` |
| `--data`    | Persistent [storage](docs/internals/storage.md#directory-layout) (logs, state, snapshots)              | _(ephemeral)_     |
| `--single`  | Development or single-server deployment ([details](docs/operations/deployment.md#single-node-cluster)) |                   |
| `--join`    | Add this server to an existing cluster ([details](docs/operations/deployment.md#adding-a-node))        |                   |
| `--cluster` | Start a new N-node cluster ([details](docs/operations/deployment.md#multi-node-cluster-3-nodes))       | `3`               |
| `--peers`   | How to [find other nodes](docs/operations/deployment.md#discovery-options): DNS domain or file path    | _(disabled)_      |

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
