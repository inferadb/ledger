# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

InferaDB Ledger is a blockchain database for cryptographically verifiable authorization. It commits state changes cryptographically, replicates via Raft consensus, and enables client-side verification. Built in Rust 1.85 (2024 edition) with gRPC API.

## Source of Truth

- **DESIGN.md** — Authoritative specification for all implementations. Code must match the design doc.
- **proto/** — gRPC service and message definitions. Keep in sync with DESIGN.md and implementation.

## Commands

```bash
# Build
cargo build                         # All crates
cargo build -p ledger-types         # Single crate

# Test
cargo test                          # All tests
cargo test -p ledger-storage        # Single crate
cargo test test_name -- --nocapture # Single test with output

# Lint & Format (nightly required for fmt)
cargo +nightly fmt
cargo clippy --all-targets -- -D warnings

# Full check before commit
cargo +nightly fmt --check && cargo clippy --all-targets -- -D warnings && cargo test

# Generate protobuf (from proto/ directory)
cd proto && buf generate

# Run server
cargo run -p ledger-server --release -- --config config.toml
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      gRPC Services                          │
│   ReadService │ WriteService │ AdminService │ HealthService │
├─────────────────────────────────────────────────────────────┤
│                 ledger-raft (consensus)                     │
│   Raft via openraft │ Log storage │ Batching │ Idempotency  │
├─────────────────────────────────────────────────────────────┤
│                 ledger-storage (state)                      │
│   redb engine │ Entity/Relationship stores │ Indexes        │
├─────────────────────────────────────────────────────────────┤
│                 ledger-types (shared)                       │
│   Hash primitives │ Merkle proofs │ Config │ Error types    │
└─────────────────────────────────────────────────────────────┘
```

**Crates:**
- `ledger-types` — Core types, SHA-256/seahash, merkle tree, snafu errors
- `ledger-storage` — redb wrapper, entity/relationship CRUD, dual indexes, state root computation
- `ledger-raft` — openraft integration, log storage, gRPC services, transaction batching
- `ledger-server` — Main binary, bootstrap, config loading

**Key abstractions:**
- `StorageEngine` (storage/engine.rs) — redb database wrapper with transaction helpers
- `StateLayer` (storage/state.rs) — Applies blocks, computes bucket-based state roots
- `LedgerServer` (raft/server.rs) — gRPC server combining all services with Raft

**Data model:**
- Namespace → isolated storage unit per organization
- Vault → relationship store within namespace, maintains its own blockchain
- Entity → key-value data with TTL and versioning
- Relationship → authorization tuple (resource, relation, subject)
- Shard → multiple namespaces sharing a Raft group for efficiency

## Code Conventions

**Lints (workspace-level):**
- `unsafe_code = "deny"` — No unsafe
- `unwrap_used = "deny"` — Use snafu `.context()` instead
- `panic = "deny"` — No panics
- `missing_docs = "warn"` — Document public items

**Error handling:** Use `snafu` with backtraces. Propagate with `?` operator.

**Formatting:** Nightly toolchain required (`cargo +nightly fmt`).

## Implementation Standards

- No `todo!()`, `unimplemented!()`, or placeholder stubs — fully implement or don't write
- No backwards compatibility shims, feature flags, or deprecation patterns
- No tech debt markers (TODO, FIXME, HACK)
- TDD: write tests before implementation, target 90%+ coverage

## Writing Style

For documentation, comments, and markdown files:

**Conciseness:**
- "because" not "due to the fact that"
- "to" not "in order to"
- "now" not "at this point in time"
- "if" not "in the event that"

**No filler or weak modifiers:**
- "can" not "has the ability to" / "is able to"
- Remove: very, really, quite, extremely, basically, actually

**Markdown:**
- Headers: plain text, no bold, no numbering
- Code blocks: always specify language
- File naming: kebab-case
