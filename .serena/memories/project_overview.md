# InferaDB Ledger - Project Overview

## Purpose
Ledger is InferaDB's storage layer — a blockchain database for cryptographically verifiable auditing.

## Tech Stack
- **Language**: Rust 1.85 (2024 edition)
- **Storage**: Inkwell (custom embedded ACID key-value store)
- **Consensus**: openraft (Raft implementation)
- **Networking**: gRPC via tonic/prost (HTTP/2 over TCP)
- **Crypto**: SHA-256, seahash, rs_merkle
- **Error Handling**: snafu with backtraces

## Crate Structure
- `ledger-types`: Core types, errors, crypto primitives
- `ledger-storage`: Inkwell storage engine, state layer, snapshots
- `ledger-raft`: Raft consensus, gRPC services
- `ledger-server`: Server binary entry point

## Key Concepts
- **Namespace**: Per-organization storage unit
- **Vault**: Relationship store with own cryptographic chain
- **Entity**: Key-value data with TTL/versioning
- **Relationship**: Authorization tuple (resource, relation, subject)
- **Shard**: Multiple namespaces sharing a Raft group
