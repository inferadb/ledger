# AGENTS.md

InferaDB Ledger: blockchain database for cryptographically verifiable authorization. Rust 1.85 (2024 edition), gRPC API.

## Critical Constraints

**These rules are non-negotiable:**

- No `unsafe` code
- No `.unwrap()` — use snafu `.context()`
- No `panic!`, `todo!()`, `unimplemented!()`
- No placeholder stubs — fully implement or don't write
- No TODO/FIXME/HACK comments
- No backwards compatibility shims or feature flags
- Write tests before implementation, target 90%+ coverage

## Source of Truth

1. **DESIGN.md** — authoritative spec. Code must match.
2. **proto/** — gRPC definitions. Keep synced with DESIGN.md.

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

## Commands

```bash
# Build
cargo build                                # all crates
cargo build -p inferadb-ledger-types       # single crate

# Test
cargo test                                 # all tests
cargo test -p inferadb-ledger-state        # single crate
cargo test test_name -- --nocapture        # single test with output

# Lint & Format
cargo +nightly fmt
cargo +1.85 clippy --all-targets -- -D warnings

# Pre-commit check
cargo +nightly fmt --check && cargo +1.85 clippy --all-targets -- -D warnings && cargo test

# Protobuf generation
cd proto && buf generate

# Run server
cargo run -p inferadb-ledger-server --release -- --config config.toml
```

## Architecture

```
gRPC Services: ReadService | WriteService | AdminService | HealthService
       ↓
inferadb-ledger-raft    — Raft consensus, log storage, batching, idempotency
       ↓
inferadb-ledger-state   — Entity/Relationship stores, state roots, indexes
       ↓
inferadb-ledger-store   — B+ tree engine, pages, transactions, backends
       ↓
inferadb-ledger-types   — Hash primitives, Merkle proofs, config, errors
```

**Crates:**

- `types` — SHA-256/seahash, merkle tree, snafu errors
- `store` — B+ tree engine, page management, memory/file backends
- `state` — Domain state, entity/relationship CRUD, state root computation
- `raft` — openraft integration, gRPC services, transaction batching
- `server` — Binary, bootstrap, config, node ID generation

**Key types:**

- `StorageEngine` (state/engine.rs) — store wrapper with transaction helpers
- `StateLayer` (state/state.rs) — applies blocks, computes state roots
- `LedgerServer` (raft/server.rs) — gRPC server combining services with Raft

**Data model:**

- Namespace → isolated storage per org
- Vault → relationship store within namespace, owns its blockchain
- Entity → key-value with TTL and versioning
- Relationship → authorization tuple (resource, relation, subject)
- Shard → namespaces sharing a Raft group

## Error Handling

Use `snafu` with backtraces. Propagate with `?`. No `.unwrap()`.

## Code Quality

**Linting:** `cargo +1.85 clippy --all-targets -- -D warnings`

**Formatting:** `cargo +nightly fmt` (nightly required)

**Doc comments:** Use ```` ```no_run ```` for code examples — `cargo test` skips execution; `cargo doc` validates syntax.

**Markdown:** Be concise, no filler words, kebab-case filenames, specify language in code blocks. Prefer showing to telling.

**Writing:** No filler (very, really, basically), no wordiness (in order to → to, due to the fact → because), active voice, specific language.
