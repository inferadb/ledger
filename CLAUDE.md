# CLAUDE.md — InferaDB Ledger

## Project Overview

InferaDB Ledger is a blockchain database for cryptographically verifiable authorization. Every write produces a Merkle-chained block; every authorization check is provable. The system is always multi-Raft in production and handles PII under strict data-residency rules (EU-region data must stay in EU-region storage). Treat every change to storage keys, gRPC surfaces, error handling, or consensus primitives with that level of seriousness — a silent data-residency violation is a compliance incident, and a silent consensus bug is data loss.

Writes are WAL-durable on response; state-DB materialization is lazy via `StateCheckpointer` (per-region, `crates/raft/src/state_checkpointer.rs`) and forced on shutdown, snapshot, and backup boundaries. On crash, state is re-derived by replaying `(applied_durable, last_committed]` from the WAL — see [`docs/operations/durability.md`](docs/operations/durability.md).

## Tech Stack

| Layer                  | Technology                                   | Version / Notes                                                               |
| ---------------------- | -------------------------------------------- | ----------------------------------------------------------------------------- |
| Language               | Rust                                         | 1.92 (2024 edition); pinned `+1.92` for build/clippy/test, `+nightly` for fmt |
| Consensus              | Custom in-house multi-shard Raft             | No openraft. Lives in `crates/consensus/`.                                    |
| Storage                | B+ tree, per-vault AES-256-GCM segmented WAL | `crates/store/` + `crates/consensus/src/wal/`                                 |
| RPC                    | gRPC / Protocol Buffers                      | tonic 0.14                                                                    |
| Errors (server crates) | `snafu`                                      | `types`, `store`, `proto`, `state`, `consensus`, `raft`, `services`, `server` |
| Errors (SDK)           | `thiserror`                                  | SDK only — consumer-facing types                                              |
| Builders               | `bon`                                        | Both `#[derive(bon::Builder)]` and `#[bon::bon] impl`                         |
| Task runner            | `just`                                       | `just --list` is the source of truth                                          |

## Repo Structure

```text
ledger/
├── CLAUDE.md              # You are here. Golden rules, conventions, escalation.
├── AGENTS.md              # Symlink → CLAUDE.md (for tools that look for AGENTS.md).
├── Justfile               # Every command. Run `just --list` for the catalog.
├── proto/ledger/v1/       # Protobuf source. Hand-edit here; regenerate with `just proto`.
├── crates/
│   ├── types/             # Newtype IDs, errors, config, Merkle, hash, snowflake.
│   ├── store/             # B+ tree, pages, backends, crypto keys.
│   ├── proto/             # Protobuf codegen + From/TryFrom conversions.
│   ├── state/             # StorageEngine, StateLayer, SystemKeys, residency patterns.
│   ├── consensus/         # Custom in-house Raft: Engine, Reactor, Shard, WAL, simulation.
│   ├── raft/              # Saga orchestrator, background jobs, apply pipeline, rate limiter.
│   ├── services/          # 14 gRPC services, SlugResolver, JwtEngine, server assembly.
│   ├── server/            # Binary entrypoint + single-binary integration tests.
│   ├── sdk/               # Consumer Rust client (the one crate using thiserror).
│   ├── test-utils/        # Shared test scaffolding, CrashInjector, proptest strategies.
│   └── profile/           # Profiling workload driver — SDK-consumer CLI.
```

Each crate has its own `CLAUDE.md` (symlinked to `AGENTS.md`) with crate-specific invariants that **extend — never relax — the golden rules below**. When a crate-level rule conflicts with a root rule, **the root rule wins**.

---

## GOLDEN RULES

**Non-negotiable. Every agent, contributor, and reviewer follows these. If a rule looks wrong, raise it explicitly — never silently violate.**

1. **Never hand-edit `crates/proto/src/generated/` or `Cargo.lock`.** Both are generated. Edit `proto/ledger/v1/*.proto` and run `just proto`; edit `Cargo.toml` and run `cargo +1.92 build --workspace`. The `.claude/settings.json` `PreToolUse` hook blocks edits to these paths.

2. **Never run `git commit` from an agent.** The `PreToolUse` hook blocks `git commit`. The human operator commits when work is ready.

3. **Every `*_PREFIX` / `*_KEY` constant on `SystemKeys` in `crates/state/src/system/keys.rs` has a matching `KEY_REGISTRY` entry.** A missing entry silently disables `SystemKeys::validate_key_tier()` for that key — the exact data-residency bug the registry exists to catch. Audited proactively by the `data-residency-auditor` agent.

4. **Storage keys take internal `{Entity}Id(i64)` newtypes only — never `{Entity}Slug(u64)`.** Slug ↔ ID translation lives at the gRPC boundary in `crates/services/src/services/slug_resolver.rs`. A `*Slug` argument on any key builder in `crates/state/src/system/keys.rs` is a bug.

5. **Never write PII to a `KeyTier::Global` key.** PII → REGIONAL, always. Use Pattern 1 (REGIONAL-only bare key), Pattern 2 (GLOBAL skeleton + REGIONAL `{entity}_profile:` overlay), or Pattern 3 (GLOBAL-only, no PII). Full pattern detail in `crates/state/CLAUDE.md`. Audited by `data-residency-auditor`.

6. **Every storage write calls `SystemKeys::validate_key_tier(&key, expected_tier)` in the same transaction, immediately before the put/insert.** Construct keys via `SystemKeys::*` builders only — never `format!("_idx:foo:{id}")` at the call site. Inline construction bypasses both the registry and the tier check.

7. **Server crates use `snafu` only. The SDK uses `thiserror`.** `anyhow` is banned everywhere. Every variant with a `source` field includes `#[snafu(implicit)] location: snafu::Location`. Propagate via `.context(XxxSnafu)?` — never manually construct an error variant. Audited by `snafu-error-reviewer`.

8. **No `unsafe`, `panic!`, `todo!()`, `unimplemented!()`, or `TODO`/`FIXME`/`HACK`/`XXX` comments in production code.** No placeholder stubs, no backwards-compat shims, no feature-flag dead paths. `.unwrap()` / `.expect()` outside `#[cfg(test)]` requires a `SAFETY:` comment stating why the call cannot fail. Audited by `unsafe-panic-auditor`.

9. **Never introduce `openraft` or any external Raft crate.** Consensus is custom in-house. Leadership primitives live in `crates/consensus/src/lease.rs` + `leadership.rs`; leader-transfer orchestration lives in `crates/raft/src/leader_lease.rs` + `leader_transfer.rs`. `grep openraft` across the workspace must return zero dependency references — only historical notes.

10. **`Shard` in `crates/consensus/src/shard.rs` returns `Action` values and performs no I/O.** Any blocking call, disk read, or network send inside `Shard` is a correctness bug. All I/O executes in `Reactor` (`crates/consensus/src/reactor.rs`). WAL writes are batched with a single `fsync` per batch — never per proposal.

11. **External gRPC messages use `{Entity}Slug { slug: u64 }`, never internal `{Entity}Id(i64)`.** Server-side request forwarding is allowed **only** for saga orchestration — the `RegionalProposal` RPC. All other cross-region / cross-leader traffic uses redirect-only routing: return `NotLeader` + `LeaderHint` inside `ErrorDetails` and let the SDK's `RegionLeaderCache` reconnect.

12. **All gRPC error responses go through `status_with_correlation()` in `crates/services/src/services/metadata.rs`.** Never construct `tonic::Status` manually — the helper auto-attaches `ErrorDetails` built by `build_error_details()` in the same directory. Errors without `ErrorDetails` lose retryability, error-code, and suggested-action metadata on the SDK side.

13. **Server integration tests are a single binary.** `crates/server/Cargo.toml` sets `autotests = false` and declares exactly one `[[test]] name = "integration"`. Every test file is a submodule of `crates/server/tests/integration.rs` using `use crate::common::` — never `mod common;`. Audited by `test-isolation-auditor`.

14. **A task is complete only when `/just-ci-gate` passes** (`fmt-check` + `clippy` + `doc-check` + `test`). No "pre-existing issue" exceptions. Write the test before the implementation; target 90%+ line coverage.

---

## Conventions

### Toolchain

- `cargo +1.92` for build, clippy, test. `cargo +nightly` for fmt. Never fall back to unpinned `cargo`.
- Everyday commands: `just check-quick` (fast iteration), `just ci` (pre-merge gate), `just proto` (after `.proto` edits), `just ready` (proto + ci). Full catalog: `just --list`.

### Identifiers

- **Internal IDs** — `define_id!({Entity}Id)` in `crates/types/src/types/ids.rs`. `i64`, sequential, used in storage + state + Raft.
- **External slugs** — `define_slug!({Entity}Slug)` in same file. `u64` Snowflake, used in gRPC + SDK.
- `TokenVersion(u64)` is the only manual newtype (needs `Default` + `increment()`).

### Storage key families

| Prefix    | Purpose                                                      | Tier     |
| --------- | ------------------------------------------------------------ | -------- |
| _(bare)_  | Primary domain record                                        | varies   |
| `_dir:`   | Directory routing (ID → region/slug/status)                  | GLOBAL   |
| `_idx:`   | Secondary index                                              | varies   |
| `_meta:`  | Sequences, saga state, node membership                       | GLOBAL   |
| `_shred:` | Crypto-shredding keys (destroyed on erasure)                 | REGIONAL |
| `_tmp:`   | TTL-bound ephemeral records (e.g. `_tmp:saga_pii:{saga_id}`) | REGIONAL |
| `_audit:` | Compliance erasure records                                   | GLOBAL   |

Ordering invariant: `:` (0x3A) < `_` (0x5F), so `app:{org}:*` scans never match `app_profile:*` keys. Don't introduce a prefix that breaks this.

### Serena MCP (navigation)

Auto-activated by `SessionStart` hook. Prefer symbolic tools over text operations in Rust code:

| Task                   | Tool                                           | Not              |
| ---------------------- | ---------------------------------------------- | ---------------- |
| Understand file        | `get_symbols_overview`                         | Read entire file |
| Find function / struct | `find_symbol` (pattern)                        | Grep / Glob      |
| Find usages            | `find_referencing_symbols`                     | Text grep        |
| Edit function          | `replace_symbol_body`                          | Raw text edit    |
| Add code               | `insert_after_symbol` / `insert_before_symbol` | Line numbers     |
| Search patterns        | `search_for_pattern` with `relative_path`      | Global grep      |

Symbol paths: `ClassName/method_name`. Workflow: `get_symbols_overview` → `find_symbol` (`depth=1`) → `include_body=True` only when needed → `find_referencing_symbols` before refactors.

### Doc comments

- ` ```no_run ` for Rust examples (skipped by `cargo test`, validated by `cargo doc`).
- ` ```text ` for non-Rust content. Never ` ```ignore `.

### Writing

Active voice. No filler words ("very", "really", "basically"). Specific language. Markdown: kebab-case filenames, language-tagged code blocks.

---

## Testing Standards

- **Unit tests** — `just test` runs `cargo test --workspace --lib` (~8s). Fast loop.
- **Integration** — `just test-integration`. Server integration is a single binary (golden rule 13).
- **Property tests** — `just test-proptest`. Strategies in `crates/test-utils/src/strategies.rs`. Dev default 256 iterations; nightly CI runs 10k (`PROPTEST_CASES` overrides).
- **Stress / recovery** — `just test-stress`, `just test-store-recovery`.
- **Tooling** — standard `cargo test` only. No `cargo nextest`.
- **Crash recovery** — use `CrashInjector` from `crates/test-utils`. Never `panic!` in production code to simulate failure.
- **Mocks** — SDK tests use mocked transport via `tonic` test fixtures. State-layer tests use in-memory backend. Integration tests use real B+ tree + real WAL.

---

## Agent Escalation — When to Stop and Ask

Pause and flag the human operator when any of these is true:

- The task would **break a golden rule** or appears to require an exception.
- The task requires **reintroducing `openraft`** or any external Raft implementation (golden rule 9).
- The task edits a **generated file** (`crates/proto/src/generated/`, `Cargo.lock`) — the hook will block, but the plan itself is wrong.
- The task adds a **new storage-key prefix family** without a data-residency plan. Invoke the `/add-storage-key` skill first; surface to a human if the residency pattern is unclear.
- The task adds or renames a **gRPC service or RPC** without a matching update to `proto/ledger/v1/*.proto` + `crates/proto/src/convert/**`.
- An audit agent (`unsafe-panic-auditor`, `snafu-error-reviewer`, `data-residency-auditor`, `proto-reviewer`, `test-isolation-auditor`, `consensus-reviewer`) raises a finding you believe is wrong — surface the contradiction rather than override.
- A golden rule appears **outdated** or contradicts another rule.
- A proposed change would **break an existing API, schema, wire contract, storage layout, or public signature** but produces materially better code (clearer, more correct, more efficient, simpler). Pause and confirm with the human operator — breaking changes are welcome when the tradeoff is worthwhile; silent breakage never is. Describe the break, the replacement, and the migration plan before proceeding.

Escalation is not failure. It's how risky changes get caught before they ship — and how beneficial breaking changes get approved instead of avoided.

---

## When to Add a New Rule

CLAUDE.md is long-lived. Update conventions when they change — not per feature. Add a new golden rule when:

- **The same class of bug has surfaced twice.** One incident is an oversight; two is a pattern that belongs in rules.
- **A subagent audit surfaces an invariant that was previously tacit.** Write it down so the next audit doesn't have to re-discover it.
- **A refactor makes an existing rule obsolete** or shifts the file/function/pattern it names. Update the rule or remove it — stale rules mislead worse than missing ones.
- **Adding a new crate or entity family introduces a non-obvious constraint.** Capture it before it becomes institutional folklore.

Every rule must name a specific file, function, or pattern, and include how the violation is detected (review, hook, agent, CI). "Write clean code" is a vibe, not a rule. "`Shard::shard.rs` returns `Action` and performs no I/O" is a rule.

When a new rule is added here, also update the relevant **agent definition** (if an existing audit agent should enforce it) or propose a **new agent** (for categorically new invariants).

---

## Tooling Map

**Proactive audit agents** (fire on matching file changes; read-only):

- `unsafe-panic-auditor` — banned constructs, stubs, shims, feature flags
- `snafu-error-reviewer` — snafu discipline, `ErrorCode` integration
- `data-residency-auditor` — PII and tier correctness, `KEY_REGISTRY` completeness
- `proto-reviewer` — proto + conversion + service wiring, `SlugResolver`, `ErrorDetails`
- `test-isolation-auditor` — server integration test hygiene
- `consensus-reviewer` — custom Raft / WAL / shard / saga invariants
- `docs-drift-auditor` — user-facing docs (`README.md`, `CONTRIBUTING.md`, `DESIGN.md`, `WHITEPAPER.md`, `MANIFEST.md`, `Justfile`, `docs/**`) for factual accuracy against code + developer-experience principles (audience, problem framing, Hello World, single source of truth, progressive disclosure). Dispatches parallel subagents.

**Skills** (invoke via `/skill-name` or auto-triggered):

- `/add-new-entity` — dual-ID entity rollout
- `/add-storage-key` — new key constant with registry + tier validation
- `/add-proto-conversion` — From/TryFrom conversion discipline
- `/new-rpc` — full gRPC method addition
- `/use-bon-builder` — bon patterns and gotchas
- `/define-error-type` — snafu variant shapes and `ErrorCode` wiring
- `/just-ci-gate` — authoritative pre-PR gate
- `/debug-integration-test` — debugging the consolidated server integration binary
- `/audit-claude-md` — periodic audit of CLAUDE.md / skills / agents / memories against current code

**Hooks** (`.claude/settings.json`):

- `PreToolUse` — blocks `git commit`, edits to `crates/proto/src/generated/`, edits to `Cargo.lock`.
- `PostToolUse` on `.rs` edits — `cargo +nightly fmt` + `cargo +1.92 check -p <crate>` (first 15 errors surfaced).
- `PostToolUse` on `.proto` edits — reminder to run `just proto`.
- `PostToolUse` on `.rs` / `.md` edits — writing-check: flags fenced `ignore` blocks, untagged code-fence openers, and non-kebab-case markdown filenames.
- `PostToolUse` on `crates/state/src/system/keys.rs` edits — auto-spawns `data-residency-auditor` as a subagent; findings surface in the transcript.
- `PostToolUse` on `crates/server/tests/**` edits — auto-spawns `test-isolation-auditor` as a subagent.
- `PostToolUse` on docs-drift sentinel paths (`proto/ledger/v1/**/*.proto`, `Justfile`, root `Cargo.toml`, `crates/services/src/services/**`, `crates/server/src/{main,config}.rs`, root docs, `docs/**/*.md`) — auto-spawns `docs-drift-auditor`, which fans out into parallel `Explore` subagents across doc partitions.
- `SessionStart` — reminder to call `mcp__plugin_serena_serena__activate_project` for this workspace.
