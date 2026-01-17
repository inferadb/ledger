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

[InferaDB](https://inferadb.com) Ledger is a distributed blockchain database optimized for authorization workfloads. It's optimized for low query latency, making it suitable for global applications. It commits every state change cryptographically, replicates via Raft consensus, and lets clients verify independently.

## Features

**Cryptographic Verification**

- Per-vault blockchain with chain-linked state roots
- Merkle proofs for transaction inclusion
- SHA-256 commitments clients can verify independently

**Raft Consensus**

- Strong consistency with quorum-based replication
- Automatic leader election and failover
- Log replay for deterministic state recovery

**Performance**

- Sub-millisecond reads from any replica
- <50ms p99 write latency (same datacenter)
- Bucket-based state roots for O(k) computation (k = dirty keys)
- Batched transactions amortize consensus overhead

**Multi-Tenancy**

- Namespace isolation per organization
- Multiple vaults per namespace with independent chains
- Shard groups for efficient Raft resource usage

**Storage**

- Embedded ACID database with zero-copy reads
- Hybrid architecture: fast K/V queries + merkle commitments
- Tiered snapshots (hot/warm/cold) for historical reads

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      gRPC API (HTTP/2)                      │
├─────────────────────────────────────────────────────────────┤
│                     Raft Consensus Layer                    │
│         (Leader election, log replication, quorum)          │
├─────────────────────────────────────────────────────────────┤
│  Namespace: org_acme          │  Namespace: org_startup     │
│  ├─ Vault: prod [chain]       │  ├─ Vault: main [chain]     │
│  └─ Vault: staging [chain]    │  └─ ...                     │
├─────────────────────────────────────────────────────────────┤
│                State Layer (inferadb-ledger-store)          │
│    Relationships │ Entities │ Indexes │ State Roots         │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- [mise](https://mise.jdx.dev/) for development tools (Rust, protoc, buf)

### Build

```bash
# Clone the repository
git clone https://github.com/inferadb/ledger.git
cd ledger

# Install development tools (rust, protoc, buf)
mise trust && mise install

# Build all crates
cargo build --release

# Run tests
cargo test
```

### Run a Single Node

```bash
# Create a config file from the example
cp inferadb-ledger.example.toml inferadb-ledger.toml

# Edit as needed, then start
cargo run --release -p inferadb-ledger-server

# Or start with environment variables (auto-bootstraps on fresh data directory)
INFERADB__LEDGER__NODE_ID=1 \
INFERADB__LEDGER__LISTEN_ADDR=127.0.0.1:50051 \
INFERADB__LEDGER__DATA_DIR=/tmp/ledger \
cargo run --release -p inferadb-ledger-server
```

### Run a 3-Node Cluster

```bash
./scripts/start-cluster.sh          # Start local development cluster
./scripts/start-cluster.sh status   # Check status
./scripts/start-cluster.sh stop     # Stop cluster
./scripts/start-cluster.sh clean    # Stop and remove all data
```

### Configuration

See [`inferadb-ledger.example.toml`](inferadb-ledger.example.toml) for all options. Key settings:

```toml
node_id = 1                           # Unique numeric node ID
listen_addr = "0.0.0.0:50051"         # gRPC listen address
data_dir = "/var/lib/ledger"          # Raft logs and state
```

Cluster membership is determined automatically:
- If discovery finds existing peers, the node waits to join via AdminService's JoinCluster RPC
- If no peers discovered, the node bootstraps a new single-node cluster

Environment variables override config file values using the `INFERADB__LEDGER__` prefix (e.g., `INFERADB__LEDGER__NODE_ID=1`).

## Crates

| Crate                        | Description                                    |
| ---------------------------- | ---------------------------------------------- |
| `inferadb-ledger-types`      | Core types, hashing, and protobuf definitions  |
| `inferadb-ledger-store`      | Embedded B+ tree database engine               |
| `inferadb-ledger-state`      | Domain state management, indexes, snapshots    |
| `inferadb-ledger-raft`       | Raft consensus, log storage, network transport |
| `inferadb-ledger-server`     | gRPC server, request routing, client handling  |
| `inferadb-ledger-sdk`        | Production-grade Rust SDK for client apps      |
| `inferadb-ledger-test-utils` | Shared test utilities                          |

## Design

See [DESIGN.md](DESIGN.md) for details on:

- Block structure and chain linking
- State root computation (bucket-based hashing)
- ID generation and determinism requirements
- Historical reads and snapshot tiers
- Multi-vault failure isolation
- Shard group scaling architecture

## Community

Join us on [Discord](https://discord.gg/inferadb) for questions, discussions, and contributions.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE).
