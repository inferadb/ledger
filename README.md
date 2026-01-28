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

## Features

- **Cryptographic Verification** — Per-vault blockchain with chain-linked state roots, Merkle proofs, SHA-256 commitments
- **Raft Consensus** — Strong consistency, automatic leader election, deterministic state recovery
- **Performance** — Sub-millisecond reads, <50ms p99 writes, bucket-based O(k) state roots, batched transactions
- **Multi-Tenancy** — Namespace isolation, multiple vaults per namespace, shard groups for scaling
- **Storage** — Embedded ACID database, hybrid K/V + merkle architecture, tiered snapshots

## Quick Start

### Installation

### Configuration

| CLI           | ENV                                  | Purpose                                                             | Default           |
| ------------- | ------------------------------------ | ------------------------------------------------------------------- | ----------------- |
| `--listen`    | `INFERADB__LEDGER__LISTEN_ADDR`      | Host and port to accept connections                                 | `127.0.0.1:50051` |
| `--data`      | `INFERADB__LEDGER__DATA_DIR`         | Where to store data ([layout](docs/internals/storage.md#directory-layout)) | (ephemeral) |
| `--bootstrap` | `INFERADB__LEDGER__BOOTSTRAP_EXPECT` | Cluster size ([modes](docs/operations/deployment.md#cluster-setup)) | `3`               |
| `--discovery` | `INFERADB__LEDGER__DISCOVERY_DOMAIN` | Find nodes via [DNS](docs/operations/deployment.md#dns-based-discovery-production--kubernetes) | (disabled) |
| `--join`      | `INFERADB__LEDGER__DISCOVERY_CACHE_PATH` | [Peer file](docs/operations/deployment.md#multi-node-cluster-3-nodes) listing nodes to connect to | (disabled) |

See [Configuration Reference](docs/operations/deployment.md#configuration-reference) for all options including metrics, batching, and tuning.

### Run a Single Node

```bash
cargo run --release -p inferadb-ledger-server -- \
  --listen 127.0.0.1:50051 --data /tmp/ledger --bootstrap 1
```

For multi-node clusters, backup/restore, environment variables, and Kubernetes deployment, see [docs/operations/](docs/operations/).

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
