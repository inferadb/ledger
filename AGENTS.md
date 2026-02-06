# AGENTS.md

InferaDB Ledger: blockchain database for cryptographically verifiable authorization. Rust 1.92 (2024 edition), gRPC API.

## Critical Constraints

**These rules are non-negotiable:**

- No `unsafe` code
- No `.unwrap()` — use snafu `.context()`
- No `panic!`, `todo!()`, `unimplemented!()`
- No placeholder stubs — fully implement or don't write
- No TODO/FIXME/HACK comments
- No backwards compatibility shims or feature flags
- Write tests before implementation, target 90%+ coverage
- Never make git commits

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

## Task Completion

**A task is not complete until all of these pass — no "pre-existing issue" exceptions:**

- `cargo build --workspace` — no errors or warnings
- `cargo nextest run` — all tests pass
- `cargo +1.92 clippy --workspace --all-targets -- -D warnings` — no warnings
- `cargo +nightly fmt --all -- --check` — no formatting issues

**Review workflow:**

1. Run `just ci` — all checks must pass
2. Review changes with CodeRabbit: `mcp__coderabbit__review_changes`
3. Fix all identified issues — no exceptions
4. Re-review if fixes were substantial

## Code Conventions

**Builders (bon):**

- `#[builder(into)]` for `String` fields to accept `&str`
- Match `#[builder(default)]` with `#[serde(default)]` for config
- Fallible builders via `#[bon]` impl block when validation needed
- Prefer compile-time required fields over runtime checks

**Doc comments:** Use ` ```no_run ` — `cargo test` skips, `cargo doc` validates.

**Writing:** No filler (very, really, basically), no wordiness (in order to → to), active voice, specific language.

**Markdown:** Concise, kebab-case filenames, specify language in code blocks.

## Commands

Use `just` for common tasks (see `Justfile` for all commands):

```bash
just              # list available commands
just check        # pre-commit: fmt + clippy + test
just check-quick  # fast pre-commit: fmt + clippy only
just test-fast    # quick tests (~15s)
just test         # standard tests (~30s)
just test-full    # all tests including slow (~5min)
just fmt          # format code
just clippy       # run linter
just proto        # generate protobuf code
just run          # run server (dev mode)
```

Or use cargo directly:

```bash
cargo +1.92 build -p <crate>               # build specific crate
cargo +1.92 test -p <crate>                # test specific crate
cargo +1.92 test <name> -- --nocapture     # run single test with output
cargo +nightly fmt                         # format (nightly required)
cargo +1.92 clippy --all-targets -- -D warnings
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

Use `snafu` with implicit location tracking. Never use `thiserror` or `anyhow`.

**Why snafu over alternatives:**

- `thiserror` — No context selectors, requires manual error construction
- `anyhow` — No structured types, unsuitable for libraries
- `snafu` — Context selectors + implicit locations + structured types

**Pattern for error types with source fields:**

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
```

**Propagate with context selectors:**

```rust
// Good: captures location automatically
some_operation().context(StorageSnafu)?;

// Bad: loses location information
some_operation().map_err(|e| MyError::Storage { source: e, location: ??? })?;
```

**Conventions:**

- Add `#[snafu(implicit)] location: snafu::Location` to all variants with `source` fields
- Use `.context(XxxSnafu)?` for propagation, never manual error construction
- Leaf variants (no source) don't need location fields
- API-facing errors use `message: String` for gRPC serialization

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

let config = Config::builder()
    .max_connections(50)
    .name("test")
    .build();
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

**Conventions:**

- Use `#[builder(into)]` for `String` fields to accept `&str`
- Match `#[builder(default)]` with `#[serde(default)]` for config structs
- Use fallible builders (`#[bon]` impl block) when validation is needed
- Prefer compile-time required field enforcement over runtime checks

**Performance:**

- bon is a proc-macro generating code at compile-time with zero runtime overhead
- Full workspace clean build: ~74 seconds (20 structs with builders)
- Estimated compile-time impact: ~2 seconds per 10 structs based on bon benchmarks
- Builder construction optimizes to direct struct initialization in release builds

## Code Quality

**Linting:** `cargo +1.92 clippy --all-targets -- -D warnings`

**Formatting:** `cargo +nightly fmt` (nightly required)

**Doc comments:** Use ` ```no_run ` for code examples — `cargo test` skips execution; `cargo doc` validates syntax.

**Markdown:** Be concise, no filler words, kebab-case filenames, specify language in code blocks. Prefer showing to telling.

**Writing:** No filler (very, really, basically), no wordiness (in order to → to, due to the fact → because), active voice, specific language.
