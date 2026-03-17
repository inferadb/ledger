# AGENTS.md

InferaDB Ledger: blockchain database for cryptographically verifiable authorization. Rust 1.92 (2024 edition), gRPC API.

## Critical Constraints

**These rules are non-negotiable:**

- No `unsafe` code
- No `panic!`, `todo!()`, `unimplemented!()`
- No placeholder stubs — fully implement or don't write
- No TODO/FIXME/HACK comments
- No backwards compatibility shims or feature flags
- Write tests before implementation, target 90%+ coverage
- Never make git commits

## Serena (MCP Server)

Activate at session start: `mcp__plugin_serena_serena__activate_project`

**Use semantic tools, not file operations:**

| Task                 | Use                                            | Not                  |
| -------------------- | ---------------------------------------------- | -------------------- |
| Understand file      | `get_symbols_overview`                         | Reading entire file  |
| Find function/struct | `find_symbol` with pattern                     | Grep/glob            |
| Find usages          | `find_referencing_symbols`                     | Grep for text        |
| Edit function        | `replace_symbol_body`                          | Raw text replacement |
| Add code             | `insert_after_symbol` / `insert_before_symbol` | Line number editing  |
| Search patterns      | `search_for_pattern` with `relative_path`      | Global grep          |

**Symbol paths:** `ClassName/method_name` format. Patterns: `Foo` (any), `Foo/bar` (nested), `/Foo/bar` (exact root path).

**Workflow:**

1. `get_symbols_overview` first
2. `find_symbol` with `depth=1` to see methods without bodies
3. `include_body=True` only when needed
4. `find_referencing_symbols` before any refactor

## Task Completion

**A task is not complete until all of these pass — no "pre-existing issue" exceptions:**

- `cargo +1.92 build --workspace` — no errors or warnings
- `cargo +1.92 test --workspace --lib` — all unit tests pass
- `cargo +1.92 clippy --workspace --all-targets -- -D warnings` — no warnings
- `cargo +nightly fmt --check` — no formatting issues

**Review workflow:**

1. Run `just ci` — all checks must pass
2. Fix all identified issues — no exceptions

## Code Conventions

**Builders (bon):**

- `#[builder(into)]` for `String` fields to accept `&str`
- Match `#[builder(default)]` with `#[serde(default)]` for config
- Fallible builders via `#[bon]` impl block when validation needed
- Prefer compile-time required fields over runtime checks

**Doc comments:** Use ` ```no_run ` (never ignore or text) — `cargo test` skips, `cargo doc` validates. To avoid documentation compiling problems, instead use hidden setup lines.

**Linting:** `cargo +1.92 clippy --workspace --all-targets -- -D warnings`

**Formatting:** `cargo +nightly fmt` (nightly required)

**Writing:** No filler (very, really, basically), no wordiness (in order to → to), active voice, specific language.

**Markdown:** Concise, kebab-case filenames, specify language in code blocks.

## Commands

Use `just` for common tasks (see `Justfile` for all commands):

```bash
just              # list available commands
just check        # pre-commit: fmt + clippy + test
just check-quick  # fast pre-commit: fmt + clippy only
just ci           # CI validation: fmt + clippy + doc-check + test
just ready        # pre-PR: proto + fmt + clippy + test
just test         # unit tests only
just test-ff      # unit tests, stop on first failure
just test-integration     # integration tests (spawns clusters)
just test-integration-ff  # integration tests, stop on first failure
just test-stress  # stress/scale tests
just test-recovery # crash recovery tests (store crate)
just test-all     # all tests including slow/ignored
just fmt          # format code
just clippy       # run linter
just doc          # build rustdoc
just doc-check    # build rustdoc with -D warnings
just proto        # generate protobuf code
just run          # run server (dev mode)
```

Or use cargo directly:

```bash
cargo +1.92 build -p <crate>               # build specific crate
cargo +1.92 test -p <crate>                # test specific crate
cargo +1.92 test <name> -- --nocapture     # run single test with output
cargo +nightly fmt                         # format (nightly required)
cargo +1.92 clippy --workspace --all-targets -- -D warnings
```

## Architecture

```
gRPC Services (13): Read, Write, Admin, Organization, Vault, User, App, Token, Invitation, Events, Health, Discovery, Raft
       ↓
inferadb-ledger-services — gRPC service implementations, JWT engine, server assembly
       ↓
inferadb-ledger-raft     — Raft consensus, log storage, batching, saga orchestrator, background jobs
       ↓
inferadb-ledger-state    — Entity/Relationship stores, state roots, system services (users, keys, tokens)
       ↓
inferadb-ledger-store    — B+ tree engine, pages, transactions, backends, crypto (key management)
       ↓
inferadb-ledger-types    — Hash primitives, Merkle proofs, config, errors, token types, newtype IDs
```

**Crates (9):**

- `types` — SHA-256/seahash, merkle tree, snafu errors, config types, token claims, newtype identifiers
- `store` — B+ tree engine, page management, memory/file backends, envelope encryption (RegionMasterKey/DEK)
- `proto` — Protobuf code generation and From/TryFrom conversions
- `state` — Domain state, entity/relationship CRUD, state root computation, system services (users, signing keys, refresh tokens, invitations)
- `raft` — openraft 0.9 integration, transaction batching, rate limiting, saga orchestrator, background jobs
- `services` — gRPC service implementations, JwtEngine, LedgerServer assembly
- `server` — Binary, bootstrap, CLI configuration, node ID generation
- `sdk` — Client library, retry/circuit-breaker, cancellation, metrics
- `test-utils` — Test fixtures, mock backends, crash injection, proptest strategies

**Key types:**

- `StorageEngine` (state/engine.rs) — store wrapper with transaction helpers
- `StateLayer` (state/state.rs) — applies blocks, computes state roots
- `LedgerServer` (services/server.rs) — gRPC server combining all 13 services with Raft

**Data model:**

- Organization → isolated storage per tenant
- Vault → relationship store within organization, owns its blockchain
- Entity → key-value with TTL and versioning
- Relationship → authorization tuple (resource, relation, subject)
- User → identity with email, role, status, token version
- App → organization-scoped client application
- Team → organization-scoped user group
- SigningKey → Ed25519 JWT signing key with scope (Global/Organization) and status (Active/Rotated/Revoked)
- RefreshToken → session token family with poison detection and TTL
- OrganizationInvitation → invitation lifecycle (REGIONAL-only, Pattern 1)
- Shard → organizations sharing a Raft group

**Dual-ID architecture:** Internal sequential IDs (`i64`) for storage, Snowflake slugs (`u64`) for external APIs. The `SlugResolver` translates at gRPC service boundaries.

| Internal ID (storage) | External Slug (API)     |
| --------------------- | ----------------------- |
| `OrganizationId(i64)` | `OrganizationSlug(u64)` |
| `VaultId(i64)`        | `VaultSlug(u64)`        |
| `UserId(i64)`         | `UserSlug(u64)`         |
| `AppId(i64)`          | `AppSlug(u64)`          |
| `TeamId(i64)`         | `TeamSlug(u64)`         |
| `InviteId(i64)`       | `InviteSlug(u64)`       |

Other newtypes: `SigningKeyId(i64)`, `RefreshTokenId(i64)`, `UserEmailId(i64)`, `EmailVerifyTokenId(i64)`, `ClientAssertionId(i64)`, `TokenVersion(u64)`. All defined in `types/src/types.rs` via `define_id!`/`define_slug!` macros.

**Storage key conventions:**

Underscore-prefixed keys (`_xxx:`) are system infrastructure; bare keys are domain entities.

| Prefix    | Purpose                                               | Tier     |
| --------- | ----------------------------------------------------- | -------- |
| _(bare)_  | Primary record (domain entity)                        | varies   |
| `_dir:`   | Directory routing (ID → region/slug/status)           | GLOBAL   |
| `_idx:`   | Secondary index (attribute → primary key)             | varies   |
| `_meta:`  | Bookkeeping (sequences, saga state, node membership)  | GLOBAL   |
| `_shred:` | Crypto-shredding keys (AES-256, destroyed on erasure) | REGIONAL |
| `_tmp:`   | Ephemeral state (TTL-bound onboarding records)        | REGIONAL |
| `_audit:` | Compliance trail (erasure records)                    | GLOBAL   |

**Data residency patterns:**

- **Pattern 1 (REGIONAL-only)**: Full record in REGIONAL under bare key. No GLOBAL counterpart. Examples: `user:`, `team:`, `user_email:`, `invite:`.
- **Pattern 2 (skeleton+overlay)**: GLOBAL skeleton under bare key (PII fields empty), REGIONAL PII under `{entity}_profile:` key. Service layer merges on read. Examples: `app:` + `app_profile:`, `org:` + `org_profile:`.
- **Pattern 3 (GLOBAL-only)**: No PII, no regional presence. Examples: `signing_key:`, `refresh_token:`.

**Prefix collision safety**: `:` (0x3A) < `_` (0x5F) in ASCII, so `app:{org}:` scans never match `app_profile:` keys.

**Tier-key safeguards**: `SystemKeys::validate_key_tier(key, expected_tier)` catches cross-tier write bugs. `KeyTier` enum in `state/src/system/keys.rs`.

## Error Handling

**Server crates** (`types`, `store`, `proto`, `state`, `raft`, `services`, `server`): Use `snafu` with implicit location tracking. Never use `thiserror` or `anyhow`.

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

// Propagate with context selectors:
some_operation().context(StorageSnafu)?;
```

- Add `#[snafu(implicit)] location: snafu::Location` to all variants with `source` fields
- Use `.context(XxxSnafu)?` for propagation, never manual error construction
- Leaf variants (no source) don't need location fields
- API-facing errors use `message: String` for gRPC serialization
- Structured `ErrorCode` enum in `types/src/error_code.rs` for SDK/gRPC error classification

**SDK crate**: Uses `thiserror` for consumer-facing error types (`SdkError`, `ResolverError`). Errors are constructed at point of failure — snafu's context selectors add no value here.

## Builder Pattern (bon)

Use the `bon` crate for type-safe builders. Two patterns:

**Simple structs** — `#[derive(bon::Builder)]`:

```rust
#[derive(bon::Builder)]
struct Config {
    #[builder(default = 100)]
    max_connections: u32,
    #[builder(into)]  // accepts &str, String, etc.
    name: String,
}
```

**Fallible constructors** — `#[bon]` on impl block:

```rust
#[bon]
impl TlsConfig {
    #[builder]
    pub fn new(
        #[builder(into)] ca_cert: Option<String>,
        use_native_roots: bool,
    ) -> Result<Self, ConfigError> {
        // Validation logic here
    }
}
```
