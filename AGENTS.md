# AGENTS.md

InferaDB Ledger: blockchain database for cryptographically verifiable authorization. Rust 1.92 (2024 edition), gRPC API.

## Constraints

- No `unsafe`, `panic!`, `todo!()`, `unimplemented!()`
- No placeholder stubs, TODO/FIXME/HACK comments, backwards-compat shims, or feature flags
- Write tests before implementation; target 90%+ coverage
- Never make git commits

## Serena (MCP)

Activate at session start: `mcp__plugin_serena_serena__activate_project`

| Task                 | Tool                                           | Not              |
| -------------------- | ---------------------------------------------- | ---------------- |
| Understand file      | `get_symbols_overview`                         | Read entire file |
| Find function/struct | `find_symbol` (pattern)                        | Grep/glob        |
| Find usages          | `find_referencing_symbols`                     | Text grep        |
| Edit function        | `replace_symbol_body`                          | Raw text edit    |
| Add code             | `insert_after_symbol` / `insert_before_symbol` | Line numbers     |
| Search patterns      | `search_for_pattern` (use `relative_path`)     | Global grep      |

Symbol paths: `ClassName/method_name`. Patterns: `Foo` (any), `Foo/bar` (nested), `/Foo/bar` (exact root).

Workflow: `get_symbols_overview` â†' `find_symbol` with `depth=1` â†' `include_body=True` only when needed â†' `find_referencing_symbols` before refactors.

## CI / Task Completion

A task is complete only when all pass â€" no "pre-existing issue" exceptions:

```bash
just ci                    # fmt + clippy + doc-check + test (primary gate)
cargo +1.92 build --workspace
cargo +1.92 test --workspace --lib
cargo +1.92 clippy --workspace --all-targets -- -D warnings
cargo +nightly fmt --check
```

## Commands

```bash
just check          # fmt + clippy + test
just check-quick    # fmt + clippy only
just ci             # fmt + clippy + doc-check + test
just ready          # proto + fmt + clippy + test
just test           # unit tests
just test-ff        # unit tests, fail-fast
just test-integration[-ff]
just test-stress[-ff]
just test-recovery[-ff]
just test-all
just test-proptest  # property tests (default 10000 iterations)
just test-external  # requires LEDGER_ENDPOINTS
just fmt / fmt-check
just clippy
just doc / doc-check
just proto
just run
just clean / clean-stale
just udeps
```

```bash
cargo +1.92 build -p <crate>
cargo +1.92 test -p <crate>
cargo +1.92 test <name> -- --nocapture
cargo +nightly fmt
```

## Architecture

```
gRPC (13 services): Read, Write, Admin, Organization, Vault, User, App, Token, Invitation, Events, Health, Discovery, Raft
  â†" inferadb-ledger-services   â€" gRPC impls, ProposalService trait, JWT engine, server assembly
  â†" inferadb-ledger-raft       â€" Saga orchestrator, background jobs, rate limiting, coordination
  â†" inferadb-ledger-consensus  â€" Multi-shard Raft, event-driven reactor, segmented WAL, pipelined replication
  â†" inferadb-ledger-state      â€" Entity/Relationship stores, state roots, system services
  â†" inferadb-ledger-store      â€" B+ tree, pages, transactions, backends, crypto (key management)
  â†" inferadb-ledger-types      â€" Hash primitives, Merkle proofs, config, errors, token types, newtype IDs

inferadb-ledger-proto          â€" Protobuf codegen, From/TryFrom conversions (cross-cutting)
```

**Crates:** `types`, `store`, `proto`, `state`, `consensus`, `raft`, `services`, `server`, `sdk`, `test-utils`

**Key types:**

- `ConsensusEngine` (consensus/lib.rs) â€" multi-shard Raft: propose/read_index/membership
- `Reactor` (consensus/reactor.rs) â€" single-task event loop, batches WAL writes and network sends
- `Shard` (consensus/shard.rs) â€" single Raft instance, event-driven (returns `Action`, no I/O)
- `WalBackend` trait (consensus/wal_backend.rs + consensus/wal/{segmented,encrypted,memory,io_uring_backend}.rs) â€" pluggable WAL; production uses per-vault AES-256-GCM segmented WAL with single fsync per batch
- `StorageEngine` (state/engine.rs) â€" store wrapper with transaction helpers
- `StateLayer` (state/state.rs) â€" `StateMachine` impl, applies blocks, computes state roots
- `LedgerServer` (services/server.rs) â€" gRPC server, all 13 services + consensus
- `NodeConnectionRegistry` (raft/node_registry.rs) â€" node-level connection pool; one `Channel` per peer, shared across all subsystems (consensus, discovery, admin, saga forwarding). HTTP/2 multiplexing serves all traffic through one TCP connection per peer.
- `RegionLeaderCache` (sdk) â€" SDK-side per-region leader endpoint cache with soft/hard TTLs, populated by `ResolveRegionLeader`, `NotLeader` hints, and `WatchLeader` push updates. All updates term-gated.
- Client routing is redirect-only: a node that receives a request for a region it does not lead returns `NotLeader` + `LeaderHint` in `ErrorDetails`, and the SDK reconnects directly to the regional leader. Server-side forwarding is reserved for saga orchestration (`ForwardRegionalProposal` RPC), not client-request proxying.

## Data Model

- Organization â†' isolated tenant storage
- Vault â†' relationship store per org, owns its blockchain (no `Vault` struct; represented by `VaultId`/`VaultSlug` + `VaultBlock`/`VaultEntry`)
- Entity â†' key-value with TTL and versioning
- Relationship â†' authorization tuple (resource, relation, subject)
- User â†' identity with email, role, status, token version
- App â†' org-scoped client application
- Team â†' org-scoped user group
- SigningKey â†' Ed25519 JWT key with scope (Global/Organization) and status (Active/Rotated/Revoked)
- RefreshToken â†' session token family with poison detection and TTL
- OrganizationInvitation â†' invitation lifecycle (REGIONAL-only, Pattern 1)
- Shard â†' orgs sharing a Raft group (managed by `ShardManager` in `state/src/shard.rs`, not persisted)

**Dual-ID:** Internal sequential `i64` IDs for storage; Snowflake `u64` slugs for external APIs. `SlugResolver` translates at gRPC boundaries.

- `OrganizationId(i64)` / `OrganizationSlug(u64)`, `VaultId`/`VaultSlug`, `UserId`/`UserSlug`, `AppId`/`AppSlug`, `TeamId`/`TeamSlug`, `InviteId`/`InviteSlug`
- Others: `SigningKeyId`, `RefreshTokenId`, `UserEmailId`, `EmailVerifyTokenId`, `ClientAssertionId`, `UserCredentialId`, `TokenVersion(u64)`
- Defined in `types/src/types/ids.rs` via `define_id!`/`define_slug!` macros; `TokenVersion` manual (needs `Default` + `increment()`)

**Storage key prefixes:**

| Prefix    | Purpose                                       | Tier     |
| --------- | --------------------------------------------- | -------- |
| _(bare)_  | Primary domain record                         | varies   |
| `_dir:`   | Directory routing (ID â†' region/slug/status) | GLOBAL   |
| `_idx:`   | Secondary index                               | varies   |
| `_meta:`  | Sequences, saga state, node membership        | GLOBAL   |
| `_shred:` | Crypto-shredding keys (destroyed on erasure)  | REGIONAL |
| `_tmp:`   | TTL-bound ephemeral onboarding records        | REGIONAL |
| `_audit:` | Compliance erasure records                    | GLOBAL   |

**Data residency:**

- **Pattern 1 (REGIONAL-only):** Full record under bare key. No GLOBAL counterpart. (`user:`, `team:`, `user_email:`, `invite:`)
- **Pattern 2 (skeleton+overlay):** GLOBAL skeleton (PII empty) + REGIONAL PII under `{entity}_profile:`. Merged on read. (`app:` + `app_profile:`, `org:` + `org_profile:`)
- **Pattern 3 (GLOBAL-only):** No PII, no regional presence. (`signing_key:`, `refresh_token:`)

Note: `:` (0x3A) < `_` (0x5F), so `app:{org}:` scans never match `app_profile:` keys.

`SystemKeys::validate_key_tier(key, expected_tier)` catches cross-tier write bugs. `KeyTier` in `state/src/system/keys.rs`.

## Error Handling

**Server crates** (`types`, `store`, `proto`, `state`, `raft`, `services`, `server`): `snafu` only â€" never `thiserror` or `anyhow`.

```rust
#[derive(Debug, Snafu)]
pub enum MyError {
    #[snafu(display("Storage error: {source}"))]
    Storage {
        source: SomeOtherError,
        #[snafu(implicit)]
        location: snafu::Location,
    },
}
// Propagate:
some_operation().context(StorageSnafu)?;
```

- All variants with `source` fields: add `#[snafu(implicit)] location: snafu::Location`
- Propagate via `.context(XxxSnafu)?`, never manual construction
- Leaf variants (no source): no location field needed
- API-facing errors: `message: String` for gRPC serialization
- `ErrorCode` enum in `types/src/error_code.rs` for SDK/gRPC classification

**SDK crate:** `thiserror` for consumer-facing types (`SdkError`, `ResolverError`).

## Builder Pattern (bon)

**Simple struct:**

```rust
#[derive(bon::Builder)]
struct Config {
    #[builder(default = 100)]
    max_connections: u32,
    #[builder(into)]
    name: String,
}
```

**Fallible constructor:**

```rust
#[bon]
impl TlsConfig {
    #[builder]
    pub fn new(
        #[builder(into)] ca_cert: Option<String>,
        use_native_roots: bool,
    ) -> Result<Self, ConfigError> { ... }
}
```

- `#[builder(into)]` for `String` fields
- Match `#[builder(default)]` with `#[serde(default)]` for config
- Prefer compile-time required fields over runtime checks

## Code Conventions

**Doc comments:** Use ` ```no_run ` for Rust examples (skipped by `cargo test`, validated by `cargo doc`). Use ` ```text ` for non-Rust content. Never use ` ```ignore `.

**Writing:** No filler words (very, really, basically); active voice; specific language.

**Markdown:** Concise, kebab-case filenames, always specify language in code blocks.

## Coding Behavior

- State assumptions explicitly; ask when uncertain
- Present multiple interpretations rather than silently choosing
- Minimum code that solves the problem â€" no speculative features, abstractions, or flexibility
- When editing: match existing style, don't improve adjacent code, don't refactor unrelated things
- Remove imports/vars/functions YOUR changes made unused; leave pre-existing dead code alone
- Transform tasks into verifiable goals; state a brief plan for multi-step tasks
