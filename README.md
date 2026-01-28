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

### Run a Single Node

```bash
inferadb-ledger --data /var/lib/ledger --expect 1
```

### Run Multiple Nodes

#### In Development or Staging

For local testing, create a peer file:

```bash
cat > /tmp/peers.json << 'EOF'
{"peers": [
  {"addr": "127.0.0.1:50051"},
  {"addr": "127.0.0.1:50052"},
  {"addr": "127.0.0.1:50053"}
]}
EOF
```

Then start each node in separate terminals with `--expect 3` (wait for 3 nodes) and `--join` pointing to the peer file:

```bash
# Node 1
inferadb-ledger --listen 127.0.0.1:50051 --data /tmp/ledger-1 \
  --expect 3 --join /tmp/peers.json

# Node 2
inferadb-ledger --listen 127.0.0.1:50052 --data /tmp/ledger-2 \
  --expect 3 --join /tmp/peers.json

# Node 3
inferadb-ledger --listen 127.0.0.1:50053 --data /tmp/ledger-3 \
  --expect 3 --join /tmp/peers.json
```

Nodes discover each other, coordinate, and the lowest-ID node bootstraps the cluster.

#### In Production

Configure DNS A records pointing to each node:

```text
ledger.example.com.  A  192.168.1.101
ledger.example.com.  A  192.168.1.102
ledger.example.com.  A  192.168.1.103
```

Then start each node with `--discover`:

```bash
# On 192.168.1.101
inferadb-ledger --listen 192.168.1.101:50051 --data /var/lib/ledger \
  --expect 3 --discover ledger.example.com

# On 192.168.1.102
inferadb-ledger --listen 192.168.1.102:50051 --data /var/lib/ledger \
  --expect 3 --discover ledger.example.com

# On 192.168.1.103
inferadb-ledger --listen 192.168.1.103:50051 --data /var/lib/ledger \
  --expect 3 --discover ledger.example.com
```

For Kubernetes, use a [headless Service](docs/operations/deployment.md#dns-based-discovery-production--kubernetes) which automatically creates DNS records for each pod.

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
