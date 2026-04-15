# Development Guidelines

## Version Control Policy
**Read-only access.** Never execute git write operations (add, commit, push, pull, merge, rebase, etc.). The user handles all version control.

Allowed: `git status`, `git log`, `git diff`, `git show`, `git branch` (listing), `git blame`.

## Code Quality Standards

### No Legacy Patterns
This is a new product. Every change should be optimal from day one.

**Blocked:** backwards-compat shims · TODO/FIXME/HACK markers · feature flags/toggles · deprecation patterns.

**Instead:** make breaking changes directly · implement correctly now, not later · ship the final implementation · remove old code entirely.

### Implementation Quality (DRY & KISS)
No duplicated code that should be extracted. Shared logic properly abstracted. As simple as possible, but no simpler. No unnecessary abstractions or indirection.

**Either implement fully and correctly, or don't implement at all.**

### No Aspirational Code
Never produce stubs, placeholders, or deferred implementations.

**Blocked:** `todo!()` · `unimplemented!()` · "// TODO: implement this" · skeleton code without logic · dummy return values.

## TDD Requirements
Target: 90%+ coverage.

1. **Red**: tests written before implementation, initially failing
2. **Green**: minimal code to make them pass
3. **Refactor**: clean up while keeping tests green

## Error Handling

**Server crates** (types, store, proto, state, consensus, raft, services, server): `snafu` only. Never `thiserror` or `anyhow`.

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
some_operation().context(StorageSnafu)?;
```

Rules:
- All variants with `source` fields: add `#[snafu(implicit)] location: snafu::Location`
- Propagate via `.context(XxxSnafu)?`, never manual construction
- Leaf variants (no source): no location field needed
- API-facing errors: `message: String` for gRPC serialization
- Classify via `ErrorCode` enum in `crates/types/src/error_code.rs`

**SDK crate**: `thiserror` for consumer-facing `SdkError` / `ResolverError`.

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

## Serena (MCP) Tooling

Activate at session start: `mcp__plugin_serena_serena__activate_project`.

| Task                 | Tool                                           | Not              |
| -------------------- | ---------------------------------------------- | ---------------- |
| Understand file      | `get_symbols_overview`                         | Read entire file |
| Find function/struct | `find_symbol` (pattern)                        | Grep/glob        |
| Find usages          | `find_referencing_symbols`                     | Text grep        |
| Edit function        | `replace_symbol_body`                          | Raw text edit    |
| Add code             | `insert_after_symbol` / `insert_before_symbol` | Line numbers     |
| Search patterns      | `search_for_pattern` (use `relative_path`)     | Global grep      |

Symbol paths: `ClassName/method_name`. Patterns: `Foo` (any), `Foo/bar` (nested), `/Foo/bar` (exact root).

Workflow: `get_symbols_overview` → `find_symbol` with `depth=1` → `include_body=True` only when needed → `find_referencing_symbols` before refactors.

## Coding Behavior

- State assumptions explicitly; ask when uncertain.
- Present multiple interpretations rather than silently choosing.
- Minimum code that solves the problem — no speculative features, abstractions, or flexibility.
- When editing: match existing style, don't improve adjacent code, don't refactor unrelated things.
- Remove imports/vars/functions YOUR changes made unused; leave pre-existing dead code alone.
- Transform tasks into verifiable goals; state a brief plan for multi-step tasks.

## Architecture Reference

See `/Users/evan/Developer/inferadb/ledger/CLAUDE.md` §Architecture for the crate stack, key types, dual-ID system, storage key prefixes, and data residency patterns (Pattern 1/2/3).

## Dependencies

When adding: latest stable version · security advisories checked · license compatible (MIT/Apache-2.0) · run `just udeps` after removing code.
