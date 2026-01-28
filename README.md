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

[InferaDB](https://inferadb.com) Ledger is a distributed blockchain database optimized for authorization workloads. It commits every state change cryptographically, replicates via Raft consensus, and lets clients verify independently.

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

| CLI           | Purpose                                                                                                             | Default           |
| ------------- | ------------------------------------------------------------------------------------------------------------------- | ----------------- |
| `--listen`    | Bind address for gRPC API                                                                                           | `127.0.0.1:50051` |
| `--data`      | Persistent [storage](docs/internals/storage.md#directory-layout) (logs, state, snapshots)                           | (ephemeral)       |
| `--expect`    | Nodes to wait for before [bootstrapping](docs/operations/deployment.md#cluster-setup) (`1`=solo, `0`=join existing) | `3`               |
| `--discover` | [Kubernetes](docs/operations/deployment.md#dns-based-discovery-production--kubernetes): find peers via DNS          | (disabled)        |
| `--join`      | [Static peers](docs/operations/deployment.md#multi-node-cluster-3-nodes): JSON file with node addresses             | (disabled)        |

See [Configuration Reference](docs/operations/deployment.md#configuration-reference) for environment variables and all options including metrics, batching, and tuning.

## Quick Start

```bash
# Single node
inferadb-ledger --data /var/lib/ledger --expect 1

# Cluster (run on each node)
inferadb-ledger --data /var/lib/ledger --expect 3 --discover ledger.example.com
```

For clusters, each node needs `--expect N` (nodes to wait for) and a way to find peers:
- **DNS** (`--discover`): Point to a domain with A records for each node, or a [Kubernetes headless Service](docs/operations/deployment.md#kubernetes-deployment)
- **Static** (`--join`): Point to a [JSON file](docs/operations/deployment.md#multi-node-cluster-3-nodes) listing peer addresses

See the [deployment guide](docs/operations/deployment.md) for complete single-machine, multi-node, and Kubernetes examples.

## Development

### Prerequisites

- [mise](https://mise.jdx.dev/) for tooling
- [just](https://github.com/casey/just) for commands

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
