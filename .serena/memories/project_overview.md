# InferaDB Ledger - Project Overview

## Purpose
Ledger is InferaDB's storage layer — a blockchain database for cryptographically verifiable auditing.

## Tech Stack
- **Language**: Rust 1.85 (2024 edition)
- **Storage**: inferadb-ledger-store (custom embedded ACID B+ tree engine)
- **Consensus**: openraft (Raft implementation)
- **Networking**: gRPC via tonic/prost (HTTP/2 over TCP)
- **Crypto**: SHA-256, seahash, rs_merkle
- **Error Handling**: snafu with backtraces

## Crate Structure
- `inferadb-ledger-types`: Core types, errors, crypto primitives
- `inferadb-ledger-store`: Embedded B+ tree database engine
- `inferadb-ledger-state`: Domain state management, indexes, snapshots
- `inferadb-ledger-raft`: Raft consensus, gRPC services
- `inferadb-ledger-server`: Server binary entry point
- `inferadb-ledger-sdk`: Production-grade Rust SDK
- `inferadb-ledger-test-utils`: Shared test utilities

## Key Concepts
- **Namespace**: Per-organization storage unit
- **Vault**: Relationship store with own cryptographic chain
- **Entity**: Key-value data with TTL/versioning
- **Relationship**: Authorization tuple (resource, relation, subject)
- **Shard**: Multiple namespaces sharing a Raft group
