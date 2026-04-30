# InferaDB Ledger - Project Overview

## Purpose
Ledger is InferaDB's storage layer — a blockchain database for cryptographically verifiable authorization. Always multi-Raft in production. Strict data-residency rules: PII must stay REGIONAL, never under a GLOBAL key.

## Tech Stack
- **Language**: Rust 1.92 (2024 edition). Pinned `+1.92` for build/clippy/test, `+nightly` for fmt.
- **Storage**: `inferadb-ledger-store` (custom embedded ACID B+ tree engine) + per-vault AES-256-GCM segmented WAL in `crates/consensus/src/wal/`.
- **Consensus**: Custom in-house multi-shard Raft. No `openraft`. Lives in `crates/consensus/` (engine, shard, reactor, WAL) and `crates/raft/` (saga orchestrator, apply pipeline, leader-transfer).
- **Wire protocol**: Custom QUIC-based binary protocol (TigerBeetle-inspired). 88-byte fixed-size FrameHeader. Postcard serialization. rustls + QUIC at the link layer. Replaces tonic/gRPC and prost entirely.
- **Wire macro**: `define_protocol!` proc macro emits server traits, client structs, dispatch fns, and the `OpCode` enum from the single service catalog in `crates/wire-services/src/lib.rs`.
- **Crypto**: SHA-256, seahash, rs_merkle, Ed25519 (JWT signing), AES-256-GCM (envelope encryption).
- **Error Handling**: `snafu` with implicit `Location` tracking in server crates; `thiserror` in SDK + wire crates (consumer-facing types). `anyhow` is banned.
- **Builders**: `bon` crate (`#[derive(bon::Builder)]` + `#[bon::bon] impl`).

## Crate Structure
- `inferadb-ledger-types` — Core types, errors, crypto primitives, config, token claims, dual-ID newtypes (`define_id!` / `define_slug!`).
- `inferadb-ledger-store` — Embedded B+ tree database engine, page format, crypto key management.
- `inferadb-ledger-fs` — Low-level fs primitives (barrier fsync, page-cache eviction). The single allow-listed `unsafe` crate.
- `inferadb-ledger-state` — `StorageEngine`, `StateLayer`, `SystemKeys`, residency patterns 1/2/3.
- `inferadb-ledger-consensus` — Custom Raft: `ConsensusEngine`, `Shard` (returns `Action`, no I/O), `Reactor` (batches WAL + network), simulation harness.
- `inferadb-ledger-raft` — Saga orchestrator, background jobs, apply pipeline, rate limiter, leader-lease and leader-transfer orchestration.
- `inferadb-ledger-wire` — Wire-protocol message types, codec, frame, opcode, version, context. Per-service request/response types in `src/services/*.rs`.
- `inferadb-ledger-wire-macro` — `define_protocol!` proc macro (parser + emitter).
- `inferadb-ledger-wire-services` — Single `define_protocol!` invocation enumerating all 14 services + their opcode allocations. The canonical service catalog.
- `inferadb-ledger-wire-transport` — QUIC `WireServer`, `WireClient`, dispatcher, frame I/O, snapshot stream.
- `inferadb-ledger-services` — Wire-trait impls for all 14 services, `SlugResolver`, `JwtEngine`, `LedgerServer` assembly.
- `inferadb-ledger-server` — Binary entrypoint + single-binary integration tests (`autotests = false`, one `[[test]] name = "integration"`).
- `inferadb-ledger-sdk` — Production-grade Rust SDK with retry/circuit-breaker, cancellation, metrics, leader cache (region + per-vault).
- `inferadb-ledger-test-utils` — Shared test scaffolding, `CrashInjector`, proptest strategies.
- `inferadb-ledger-profile` — Profiling workload driver (SDK consumer).

`consensus`, `server`, `test-utils`, `profile` set `publish = false`.

## Key Concepts
- **Organization**: Top-level tenant isolation boundary (`OrganizationId(i64)` / `OrganizationSlug(u64)`).
- **Vault**: Relationship store with its own cryptographic chain (`VaultId(i64)` / `VaultSlug(u64)`). The unit of write consensus — every vault is its own Raft group.
- **Entity**: Key-value data with TTL/versioning.
- **Relationship**: Authorization tuple (resource, relation, subject).
- **User**: Identity with email, role, status, token version (`UserId` / `UserSlug`). REGIONAL (Pattern 1, holds PII).
- **App**: Organization-scoped client application (`AppId` / `AppSlug`). Pattern 2 (GLOBAL skeleton + REGIONAL profile).
- **Team**: Organization-scoped user group (`TeamId` / `TeamSlug`). Pattern 1.
- **SigningKey**: Ed25519 JWT signing key with scope and lifecycle. Pattern 3 (GLOBAL-only).
- **RefreshToken**: Session token family with rotate-on-use + poison detection. Pattern 3.
- **OrganizationInvitation**: Invitation lifecycle. Pattern 1 (REGIONAL-only, holds PII).

## Consensus Tiers
Four Raft tiers own disjoint operation sets via distinct group newtypes:
- **SystemGroup** — cluster control plane (membership, region directory, organization directory, signing keys).
- **RegionGroup** — regional control plane and unified leader (placement, hibernation, region quota, region audit).
- **OrganizationGroup** — per-organization data plane (org metadata).
- **VaultGroup** — per-vault data plane. The unit of write consensus. Defaults to `LeadershipMode::Delegated`: vaults follow their parent `OrganizationGroup`'s leader, orgs follow their parent `RegionGroup`'s leader, both via `Shard::adopt_leader` — no independent elections.

## Wire Services (14)
`Read`, `Write`, `Organization`, `Vault`, `Schema`, `Admin`, `User`, `Invitation`, `App`, `Token`, `Events`, `Health`, `SystemDiscovery`, `Raft`. Defined in `crates/wire-services/src/lib.rs` via a single `define_protocol!` invocation; per-service request/response types in `crates/wire/src/services/*.rs`.

## Routing
**Redirect-only** at the SDK boundary. Cross-region/leader requests return `WireError` with `ErrorCode::Unavailable` and a leader-hint `context` map; the SDK's `RegionLeaderCache` (region-level) and `VaultLeaderCache` (per-vault) reconnect directly. Server-side request forwarding is reserved for saga orchestration via the `SubmitRegionalProposal` RPC.

## Durability
Writes are WAL-durable on response. The four regional DBs (state.db, raft.db, blocks.db, events.db) materialize lazily via per-region `StateCheckpointer`, force-synced on shutdown / snapshot / backup boundaries. Handler-phase audit events batch separately through `EventHandle::record_handler_event` → `FlushQueue` → `EventFlusher`. On crash, state replays `(applied_durable, last_committed]` from the WAL.
