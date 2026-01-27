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
- [just](https://github.com/casey/just) command runner

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

### Run a Single Node

```bash
cp inferadb-ledger.example.toml inferadb-ledger.toml

INFERADB__LEDGER__LISTEN_ADDR=127.0.0.1:50051 \
INFERADB__LEDGER__DATA_DIR=/tmp/ledger \
INFERADB__LEDGER__BOOTSTRAP_EXPECT=1 \
cargo run --release -p inferadb-ledger-server
```

For multi-node clusters, backup/restore, and Kubernetes deployment, see [OPERATIONS.md](OPERATIONS.md).

## Crates

| Crate                        | Description                                    |
| ---------------------------- | ---------------------------------------------- |
| `inferadb-ledger-types`      | Core types, hashing, and protobuf definitions  |
| `inferadb-ledger-store`      | Embedded B+ tree database engine               |
| `inferadb-ledger-state`      | Domain state management, indexes, snapshots    |
| `inferadb-ledger-raft`       | Raft consensus, log storage, network transport |
| `inferadb-ledger-server`     | gRPC server, request routing, client handling  |
| `inferadb-ledger-sdk`        | Rust SDK for client applications               |
| `inferadb-ledger-test-utils` | Shared test utilities                          |

## Design

See [DESIGN.md](DESIGN.md) for details on block structure, state root computation, ID generation, historical reads, multi-vault isolation, and shard group scaling.

## Community

Join us on [Discord](https://discord.gg/inferadb) for questions and discussions.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE).
