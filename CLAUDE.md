# AGENTS.md

InferaDB Ledger: blockchain database for cryptographically verifiable authorization. Rust 1.92 (2024 edition), gRPC API.

This file is the index. Crate-specific invariants live in `crates/<crate>/CLAUDE.md`. Triggered workflows live in `.claude/skills/`. Proactive audits live in `.claude/agents/`.

## Constraints

- No `unsafe`, `panic!`, `todo!()`, `unimplemented!()` — `unsafe-panic-auditor` agent enforces proactively.
- No placeholder stubs, `TODO`/`FIXME`/`HACK` comments, backwards-compat shims, or feature flags.
- Remove imports/vars/functions YOUR changes made unused; leave pre-existing dead code alone.
- Write tests before implementation; target 90%+ coverage.
- Never make git commits — the PreToolUse hook blocks `git commit`.
- Never hand-edit `crates/proto/src/generated/` or `Cargo.lock` — both are blocked by hooks. Edit `.proto` and run `just proto`; edit `Cargo.toml` and run `cargo +1.92 build --workspace`.

## Serena (MCP)

Auto-activated at session start via hook. Use symbolic tools — not text operations — for Rust navigation and edits:

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

## CI / task completion

`/just-ci-gate` runs the authoritative gate: `fmt-check + clippy + doc-check + test`. A task is complete only when it passes — no "pre-existing issue" exceptions.

Toolchains are pinned: `cargo +1.92` for build/clippy/test, `cargo +nightly` for fmt. `just --list` for the full recipe list. Everyday use:

- `just check-quick` — fmt + clippy (fast iteration)
- `just ci` — the gate before declaring done
- `just proto` — regenerate after `.proto` changes
- `just ready` — proto + fmt + clippy + test

## Workspace layout

Each crate has its own `CLAUDE.md` with crate-specific invariants and conventions.

| Crate | Purpose |
| --- | --- |
| `types` | Hash primitives, Merkle proofs, config, errors, token types, newtype IDs |
| `store` | B+ tree, pages, transactions, backends, crypto |
| `proto` | Protobuf codegen, `From`/`TryFrom` conversions |
| `state` | Entity/Relationship stores, state roots, system services, storage-key discipline |
| `consensus` | Multi-shard Raft, reactor, segmented WAL, pipelined replication |
| `raft` | Saga orchestrator, background jobs, rate limiting, service-layer helpers |
| `services` | 13 gRPC services, `SlugResolver`, JWT engine, server assembly |
| `server` | Binary entrypoint, integration tests |
| `sdk` | Consumer-facing Rust client (the one crate that uses `thiserror`) |
| `test-utils` | Shared test scaffolding, `CrashInjector`, property strategies |

## Data model quick reference

Entities: Organization, Vault, Entity (kv), Relationship (authz tuple), User, App, Team, SigningKey, RefreshToken, OrganizationInvitation. Shards are Raft-group mappings and are not persisted.

**Dual-ID**: internal `{Entity}Id(i64)` for storage and state layer; external `{Entity}Slug(u64)` (Snowflake) for gRPC and SDK. `SlugResolver` translates at the gRPC boundary — slugs never enter the state layer. See `/add-new-entity` skill when introducing a new entity family.

**Storage key prefixes** (see `/add-storage-key` skill; audited by `data-residency-auditor` agent):

| Prefix    | Purpose                                       | Tier     |
| --------- | --------------------------------------------- | -------- |
| _(bare)_  | Primary domain record                         | varies   |
| `_dir:`   | Directory routing (ID → region/slug/status)   | GLOBAL   |
| `_idx:`   | Secondary index                               | varies   |
| `_meta:`  | Sequences, saga state, node membership        | GLOBAL   |
| `_shred:` | Crypto-shredding keys (destroyed on erasure)  | REGIONAL |
| `_tmp:`   | TTL-bound ephemeral onboarding records        | REGIONAL |
| `_audit:` | Compliance erasure records                    | GLOBAL   |

Ordering invariant: `:` (0x3A) < `_` (0x5F), so `app:{org}:` scans never match `app_profile:` keys.

**Residency patterns** (Pattern 1 REGIONAL-only, Pattern 2 GLOBAL skeleton + REGIONAL overlay, Pattern 3 GLOBAL-only): see `crates/state/CLAUDE.md`. PII → REGIONAL, always.

## Errors and builders

- Server crates (`types`, `store`, `proto`, `state`, `consensus`, `raft`, `services`, `server`): `snafu` only. See `/define-error-type` skill and `snafu-error-reviewer` agent.
- SDK: `thiserror` for consumer-facing types (`SdkError`, `ResolverError`).
- `bon` for struct builders and fallible constructors. See `/use-bon-builder` skill.

## Tooling map

**Proactive audit agents** (fire on matching file changes):

- `unsafe-panic-auditor` — banned constructs
- `snafu-error-reviewer` — error discipline
- `data-residency-auditor` — PII and tier correctness
- `proto-reviewer` — RPC + conversion changes
- `test-isolation-auditor` — integration-test hygiene
- `consensus-reviewer` — Raft/WAL invariants

**Skills** (invoke via `/skill-name` or auto-triggered by context):

- `/add-new-entity` — dual-ID entity rollout
- `/add-storage-key` — new key constant with registry + tier validation
- `/add-proto-conversion` — `From`/`TryFrom` conversion discipline
- `/new-rpc` — full gRPC method addition
- `/use-bon-builder` — bon patterns and gotchas
- `/define-error-type` — snafu variant shapes and `ErrorCode` integration
- `/just-ci-gate` — authoritative pre-PR gate
- `/debug-integration-test` — debugging the consolidated server integration binary

## Code conventions

**Doc comments**: ` ```no_run ` for Rust examples (skipped by `cargo test`, validated by `cargo doc`); ` ```text ` for non-Rust content. Never ` ```ignore `.

**Writing**: active voice, no filler words, specific language. Markdown: kebab-case filenames, language-tagged code blocks.
