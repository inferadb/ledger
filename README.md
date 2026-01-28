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

- [Features](#Features)
- [Installation](#Installation)
- [Quick Start](#Quick-Start)
- [Development](#Development)
- [Design](#Design)
- [Community](#Community)
- [License](#License)

## Features

- **Cryptographic Verification** — Per-vault blockchain with chain-linked state roots, Merkle proofs, SHA-256 commitments
- **Raft Consensus** — Strong consistency, automatic leader election, deterministic state recovery
- **Performance** — Sub-millisecond reads, <50ms p99 writes, bucket-based O(k) state roots, batched transactions
- **Multi-Tenancy** — Namespace isolation, multiple vaults per namespace, shard groups for scaling
- **Storage** — Embedded ACID database, hybrid K/V + merkle architecture, tiered snapshots

## Installation

## Configuration

| CLI         | Purpose                                                                                                | Default           |
| ----------- | ------------------------------------------------------------------------------------------------------ | ----------------- |
| `--listen`  | Bind address for gRPC API                                                                              | `127.0.0.1:50051` |
| `--data`    | Persistent [storage](docs/internals/storage.md#directory-layout) (logs, state, snapshots)              | _(ephemeral)_       |
| `--single`  | Development or single-server deployment ([details](docs/operations/deployment.md#single-node-cluster)) |                   |
| `--join`    | Add this server to an existing cluster ([details](docs/operations/deployment.md#adding-a-node))        |                   |
| `--cluster` | Start a new N-node cluster ([details](docs/operations/deployment.md#multi-node-cluster-3-nodes))       | `3`               |
| `--peers`   | How to [find other nodes](docs/operations/deployment.md#discovery-options): DNS domain or file path    | _(disabled)_        |

See [Configuration Reference](docs/operations/deployment.md#configuration-reference) for environment variables and all options including metrics, batching, and tuning.

## Quick Start

```bash
# Development or single-server deployment
inferadb-ledger --data /var/lib/ledger --single

# Production cluster (run on each of 3 nodes)
inferadb-ledger --data /var/lib/ledger --cluster 3 --peers ledger.example.com
```

For clusters, `--peers` tells each node how to find the others. The format is auto-detected:

- **DNS domain** (e.g., `ledger.example.com`) — looks up A records
- **File path** (e.g., `/var/lib/ledger/peers.json`) — reads addresses from JSON

See the [deployment guide](docs/operations/deployment.md) for multi-node setup, Kubernetes, adding/removing nodes, backup, and recovery.

## Development

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

# See all commands
just
```

## Design

See [DESIGN.md](DESIGN.md) for details on block structure, state root computation, ID generation, historical reads, multi-vault isolation, and shard group scaling.

## Community

Join us on [Discord](https://discord.gg/inferadb) for questions and discussions.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE).
