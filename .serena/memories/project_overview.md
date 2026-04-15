# InferaDB Ledger - Project Overview

## Purpose
Ledger is InferaDB's storage layer — a blockchain database for cryptographically verifiable authorization.

## Tech Stack
- **Language**: Rust 1.92 (2024 edition, stable since 1.85)
- **Storage**: inferadb-ledger-store (custom embedded ACID B+ tree engine)
- **Consensus**: openraft 0.9 (multi-shard Raft via event-driven reactor, segmented WAL, pipelined replication)
- **Networking**: gRPC via tonic/prost (HTTP/2 over TCP)
- **Crypto**: SHA-256, seahash, rs_merkle, Ed25519 (JWT signing), AES-256-GCM (envelope encryption)
- **Error Handling**: snafu with implicit location tracking (server crates), thiserror (SDK crate)
- **Builders**: bon crate for type-safe builders

## Crate Structure (10 crates)
- `inferadb-ledger-types`: Core types, errors, crypto primitives, config, token claims, newtype IDs
- `inferadb-ledger-store`: Embedded B+ tree database engine, crypto key management
- `inferadb-ledger-proto`: Protobuf code generation and From/TryFrom conversions
- `inferadb-ledger-state`: Domain state, entity/relationship CRUD, system services (users, signing keys, tokens)
- `inferadb-ledger-consensus`: Multi-shard Raft, event-driven reactor, segmented WAL, pipelined replication
- `inferadb-ledger-raft`: Saga orchestrator, background jobs, rate limiting, coordination glue
- `inferadb-ledger-services`: gRPC service implementations, JwtEngine, LedgerServer assembly
- `inferadb-ledger-server`: Server binary entry point, bootstrap, CLI configuration
- `inferadb-ledger-sdk`: Production-grade Rust SDK, retry/circuit-breaker, cancellation, metrics
- `inferadb-ledger-test-utils`: Shared test utilities, crash injection, proptest strategies

Not all crates are published: `consensus`, `server`, `test-utils` set `publish = false`.

## Key Concepts
- **Organization**: Top-level tenant isolation boundary (dual-ID: OrganizationId/OrganizationSlug)
- **Vault**: Relationship store with own cryptographic chain (dual-ID: VaultId/VaultSlug)
- **Entity**: Key-value data with TTL/versioning
- **Relationship**: Authorization tuple (resource, relation, subject)
- **User**: Identity with email, role, status, token version (dual-ID: UserId/UserSlug)
- **App**: Organization-scoped client application (dual-ID: AppId/AppSlug)
- **Team**: Organization-scoped user group (dual-ID: TeamId/TeamSlug)
- **SigningKey**: Ed25519 JWT signing key with scope and lifecycle
- **RefreshToken**: Session token family with rotate-on-use and poison detection
- **OrganizationInvitation**: Invitation lifecycle (REGIONAL-only, Pattern 1)
- **Shard**: Multiple organizations sharing a Raft group

## gRPC Services (14)
Read, Write, Organization, Vault, Schema, Admin, User, Invitation, App, Token, Events, Health, SystemDiscovery, Raft
