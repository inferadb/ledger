<div align="center">
    <p><a href="https://inferadb.com"><img src=".github/inferadb.png" width="100" alt="InferaDB Logo" /></a></p>
    <h1>Ledger</h1>
    <p>
        <a href="https://discord.gg/inferadb"><img src="https://img.shields.io/badge/Discord-Join%20us-5865F2?logo=discord&logoColor=white" alt="Discord" /></a>
        <a href="#license"><img src="https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg" alt="License" /></a>
        <a href="https://github.com/inferadb/ledger/actions"><img src="https://img.shields.io/github/actions/workflow/status/inferadb/ledger/ci.yml?branch=main" alt="CI" /></a>
    </p>
    <p><b>Blockchain storage for cryptographically verifiable authorization.</b></p>
</div>

> [!IMPORTANT]
> Under active development. Not production-ready.

Ledger is InferaDB's persistence layer — a blockchain database for authorization workloads. It commits every state change cryptographically, replicates via Raft consensus, and lets clients verify independently.

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
│                   State Layer (redb)                        │
│    Relationships │ Entities │ Indexes │ State Roots         │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Rust 1.85+ (edition 2024)
- Protocol Buffers compiler (`protoc`)

### Build

```bash
# Clone the repository
git clone https://github.com/inferadb/ledger.git
cd ledger

# Build all crates
cargo build --release

# Run tests
cargo test
```

### Configuration

Create a `config.toml`:

```toml
[node]
id = "node-1"
data_dir = "/var/lib/ledger"

[grpc]
listen_addr = "0.0.0.0:9000"

[raft]
listen_addr = "0.0.0.0:9001"
peers = ["node-2:9001", "node-3:9001"]

[storage]
max_wal_size = "1GB"
snapshot_interval = 10000  # blocks
```

### Run

```bash
# Start a single node (development)
cargo run --release -- --config config.toml

# Or run a 3-node cluster
./scripts/start-cluster.sh
```

## Crates

| Crate            | Description                                    |
| ---------------- | ---------------------------------------------- |
| `ledger-types`   | Core types, hashing, and protobuf definitions  |
| `ledger-storage` | Storage layer, state management, snapshots     |
| `ledger-raft`    | Raft consensus, log storage, network transport |
| `ledger-server`  | gRPC server, request routing, client handling  |

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
