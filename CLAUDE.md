# CLAUDE.md — InferaDB Ledger

## Project Overview

InferaDB Ledger is a blockchain database for cryptographically verifiable authorization. Every write produces a Merkle-chained block; every authorization check is provable. The system is always multi-Raft in production and handles PII under strict data-residency rules (EU-region data must stay in EU-region storage). Treat every change to storage keys, RPC surfaces, error handling, or consensus primitives with that level of seriousness — a silent data-residency violation is a compliance incident, and a silent consensus bug is data loss.

Writes are WAL-durable on response. The four regional DBs — state.db, raft.db, blocks.db, events.db — all materialize lazily via `StateCheckpointer` (per-region, `crates/raft/src/state_checkpointer.rs`) and are force-synced on shutdown, snapshot, and backup boundaries. Handler-phase audit events (no WAL backstop) are batched separately: `EventHandle::record_handler_event` enqueues into a bounded `FlushQueue`; the `EventFlusher` drains into the events.db page cache via `commit_in_memory`, so durability lands on the next StateCheckpointer tick (~500ms default) — same class as apply-phase events, except the emission itself is not WAL-backed. `GracefulShutdown` Phase 5b drains the queue, Phase 5c fsyncs all four DBs; clean shutdown preserves zero-loss. Strict-durable exceptions: `RaftLogStore::save_vote` (election safety), `EventWriter::write_entry` (only reached on the `EventWriterBatchConfig::enabled=false` escape hatch and in tests — the flusher now commits in-memory directly), `StateLayer::apply_operations` (admin / recovery callers — IN-APPLY-PIPELINE arms use `apply_operations_lazy`), backup producers, and the two background-compaction paths (`BlockArchive::compact_before`, `EventsGc::tick_inner`). On crash, state is re-derived by replaying `(applied_durable, last_committed]` from the WAL; handler-phase events flushed but not yet checkpointed at the moment of the crash are lost (not WAL-backed). See [`docs/architecture/durability.md`](docs/architecture/durability.md).

## Tech Stack

| Layer                  | Technology                                   | Version / Notes                                                                                          |
| ---------------------- | -------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| Language               | Rust                                         | 1.92 (2024 edition); pinned `+1.92` for build/clippy/test, `+nightly` for fmt                            |
| Consensus              | Custom in-house multi-shard Raft             | No openraft. Lives in `crates/consensus/`.                                                               |
| Storage                | B+ tree, per-vault AES-256-GCM segmented WAL | `crates/store/` + `crates/consensus/src/wal/`                                                            |
| RPC                    | Custom QUIC wire protocol                    | `crates/wire/` (types, codec, frame), `crates/wire-macro/` (`define_protocol!`), `crates/wire-services/` (single macro invocation enumerating all 14 services), `crates/wire-transport/` (`WireServer`, `WireClient`, dispatcher). Postcard serialization, rustls/QUIC at the link layer. |
| Errors (server crates) | `snafu`                                      | `types`, `store`, `state`, `consensus`, `raft`, `services`, `server`, `wire`, `wire-services`, `wire-transport` |
| Errors (SDK)           | `thiserror`                                  | SDK only — consumer-facing types                                                                         |
| Builders               | `bon`                                        | Both `#[derive(bon::Builder)]` and `#[bon::bon] impl`                                                    |
| Task runner            | `just`                                       | `just --list` is the source of truth                                                                     |

## Repo Structure

```text
ledger/
├── CLAUDE.md              # You are here. Golden rules, conventions, escalation.
├── AGENTS.md              # Symlink → CLAUDE.md (for tools that look for AGENTS.md).
├── Justfile               # Every command. Run `just --list` for the catalog.
├── crates/
│   ├── types/             # Newtype IDs, errors, config, Merkle, hash, snowflake.
│   ├── store/             # B+ tree, pages, backends, crypto keys.
│   ├── fs/                # Low-level fs primitives (barrier fsync, page-cache eviction). The single allow-listed `unsafe` crate.
│   ├── state/             # StorageEngine, StateLayer, SystemKeys, residency patterns.
│   ├── consensus/         # Custom in-house Raft: Engine, Reactor, Shard, WAL, simulation.
│   ├── raft/              # Saga orchestrator, background jobs, apply pipeline, rate limiter.
│   ├── wire/              # Wire-protocol message types, codec, frame, opcode, version, context.
│   ├── wire-macro/        # `define_protocol!` proc macro — emits server traits, client structs, dispatch fns, OpCode enum.
│   ├── wire-services/     # Single `define_protocol!` invocation enumerating all 14 services + their opcode allocations.
│   ├── wire-transport/    # QUIC `WireServer`, `WireClient`, dispatcher, frame I/O, snapshot stream.
│   ├── services/          # 14 RPC service implementations (wire-trait impls), SlugResolver, JwtEngine, server assembly.
│   ├── server/            # Binary entrypoint + single-binary integration tests.
│   ├── sdk/               # Consumer Rust client (the one crate using thiserror).
│   ├── test-utils/        # Shared test scaffolding, CrashInjector, proptest strategies.
│   └── profile/           # Profiling workload driver — SDK-consumer CLI.
```

Each crate has its own `CLAUDE.md` (symlinked to `AGENTS.md`) with crate-specific invariants that **extend — never relax — the golden rules below**. When a crate-level rule conflicts with a root rule, **the root rule wins**.

---

## GOLDEN RULES

**Non-negotiable. Every agent, contributor, and reviewer follows these. If a rule looks wrong, raise it explicitly — never silently violate.**

1. **Wire-protocol types and dispatch are macro-generated; never hand-edit `Cargo.lock`.** RPC request/response types live in `crates/wire/src/services/*.rs` (one file per service); the canonical service catalog is the single `define_protocol!` invocation in `crates/wire-services/src/lib.rs`, which expands into server traits, client structs, dispatch fns, and the `OpCode` enum. Add or rename an RPC by editing the wire types + the `define_protocol!` invocation, never by hand-editing macro-emitted code. `Cargo.lock` is regenerated by cargo — edit `Cargo.toml` and run `cargo +1.92 build --workspace`. The `.claude/settings.json` `PreToolUse` hook blocks edits to `Cargo.lock`.

2. **Never run `git commit` from an agent.** The `PreToolUse` hook blocks `git commit`. The human operator commits when work is ready.

3. **Every `*_PREFIX` / `*_KEY` constant on `SystemKeys` in `crates/state/src/system/keys.rs` has a matching `KEY_REGISTRY` entry.** A missing entry silently disables `SystemKeys::validate_key_tier()` for that key — the exact data-residency bug the registry exists to catch. Audited proactively by the `data-residency-auditor` agent.

4. **Storage keys take internal `{Entity}Id(i64)` newtypes only — never `{Entity}Slug(u64)`.** Slug ↔ ID translation lives at the wire-protocol boundary in `crates/services/src/services/slug_resolver.rs`. A `*Slug` argument on any key builder in `crates/state/src/system/keys.rs` is a bug.

5. **Never write PII to a `KeyTier::Global` key.** PII → REGIONAL, always. Use Pattern 1 (REGIONAL-only bare key), Pattern 2 (GLOBAL skeleton + REGIONAL `{entity}_profile:` overlay), or Pattern 3 (GLOBAL-only, no PII). Full pattern detail in `crates/state/CLAUDE.md`. Audited by `data-residency-auditor`.

6. **Every storage write calls `SystemKeys::validate_key_tier(&key, expected_tier)` in the same transaction, immediately before the put/insert.** Construct keys via `SystemKeys::*` builders only — never `format!("_idx:foo:{id}")` at the call site. Inline construction bypasses both the registry and the tier check.

7. **Server crates use `snafu` only. The SDK uses `thiserror`.** `anyhow` is banned everywhere. Every variant with a `source` field includes `#[snafu(implicit)] location: snafu::Location`. Propagate via `.context(XxxSnafu)?` — never manually construct an error variant. The wire crate's `WireError` (in `crates/wire/src/error.rs`) is the canonical RPC-boundary error type returned to clients. Audited by `snafu-error-reviewer`.

8. **No `unsafe`, `panic!`, `todo!()`, `unimplemented!()`, or `TODO`/`FIXME`/`HACK`/`XXX` comments in production code.** No placeholder stubs, no backwards-compat shims, no feature-flag dead paths. `.unwrap()` / `.expect()` outside `#[cfg(test)]` requires a `SAFETY:` comment stating why the call cannot fail. Audited by `unsafe-panic-auditor`. **Single narrowly-scoped exception: `crates/fs/`** wraps `fcntl(F_BARRIERFSYNC)` on Apple platforms and `posix_fadvise(POSIX_FADV_DONTNEED)` on Linux (no audited safe-syscall crate exposes either). Every `unsafe` block there must carry a `SAFETY:` comment and map to a single syscall. Any other crate tripping `unsafe_code` is still a hard failure.

9. **Never introduce `openraft` or any external Raft crate.** Consensus is custom in-house. Leadership primitives live in `crates/consensus/src/lease.rs` + `leadership.rs`; leader-transfer orchestration lives in `crates/raft/src/leader_lease.rs` + `leader_transfer.rs`. `grep openraft` across the workspace must return zero dependency references — only historical notes.

10. **`Shard` in `crates/consensus/src/shard.rs` returns `Action` values and performs no I/O.** Any blocking call, disk read, or network send inside `Shard` is a correctness bug. All I/O executes in `Reactor` (`crates/consensus/src/reactor.rs`). WAL writes are batched with a single `fsync` per batch — never per proposal.

11. **External wire messages carry typed `{Entity}Slug(u64)` newtypes, never internal `{Entity}Id(i64)`.** Wire-message structs in `crates/wire/src/services/*.rs` use the typed slug newtypes from `crates/types/src/types/ids.rs` directly — there is no proto-style wrapper message. Server-side request forwarding is allowed **only** for saga orchestration — the `SubmitRegionalProposal` RPC. All other cross-region / cross-leader traffic uses redirect-only routing: return `NotLeader` + `LeaderHint` inside `WireError.context` and let the SDK's `RegionLeaderCache` / `VaultLeaderCache` reconnect.

12. **All RPC error responses go through `wire_error_with_correlation()` in `crates/services/src/services/wire_helpers.rs`.** Never construct a `WireError` directly inside a handler and return it — the helper stamps `request_id` / `trace_id` into `WireError.context` so the dispatcher can surface them in response-frame headers. Use `build_wire_error()` (also in `wire_helpers`) to construct rich errors from a handler with structured context (rate-limit reason, validation field, leader-hint coordinates, etc.). Errors that bypass `wire_error_with_correlation` lose correlation IDs on the SDK side.

13. **Server integration tests are a single binary.** `crates/server/Cargo.toml` sets `autotests = false` and declares exactly one `[[test]] name = "integration"`. Every test file is a submodule of `crates/server/tests/integration.rs` using `use crate::common::` — never `mod common;`. Audited by `test-isolation-auditor`.

14. **A task is complete only when `/just-ci-gate` passes** (`fmt-check` + `clippy` + `doc-check` + `test`). No "pre-existing issue" exceptions. Write the test before the implementation; target 90%+ line coverage.

15. **Tier discipline.** The four Raft tiers — `SystemGroup` (cluster control plane), `RegionGroup` (regional control plane and unified leader), `OrganizationGroup` (per-organization data plane), `VaultGroup` (per-vault data plane, the unit of write consensus) — own disjoint operation sets and route through disjoint request enums. Vault-scoped entity writes apply through `VaultGroup` only via `OrganizationRequest` proposed through the vault's shard (`ProposalService::propose_organization_request_to_vault`); org-level metadata operations apply through `OrganizationGroup` via `OrganizationRequest`; placement / hibernation / wake / region quota / region audit apply through `RegionGroup` only via `RegionRequest`; cluster membership / region directory / organization directory / cluster signing keys apply through `SystemGroup` only via `SystemRequest`. The four group types are distinct newtypes around `Arc<InnerGroup>` / `Arc<InnerVaultGroup>` — no shared base type, no `Vec<u8>` opaque payload that could "decode inside the wrong apply pipeline". `RaftManager::route_request` resolves the incoming payload to the group that owns its tier; the typed `LedgerRequest::{System, Region, Organization}` discriminant plus the optional `(organization_id, vault_id)` target are the tier key, not a hint. Per-organization and per-vault Raft groups default to `LeadershipMode::Delegated`: orgs follow their parent `RegionGroup`'s leader, vaults follow their parent `OrganizationGroup`'s leader, both via `Shard::adopt_leader` — no independent elections. Per rule 17 (in `crates/raft/CLAUDE.md`), every vault shares the parent org's `ConsensusEngine` + `Reactor` + `GrpcConsensusTransport`; per-vault state lives on `InnerVaultGroup` (its own per-shard `RaftLogStore`, per-vault `BlockArchive`, per-vault state.db). Audited by `consensus-reviewer`.

16. **Vault-scoped membership changes flow through `MembershipDispatcher` with per-vault timeout + stalled-state observability.** When an organization's voter set changes, `RegionMembershipWatcher` (`crates/raft/src/region_membership_watcher.rs`) cascades the org-level change synchronously to the parent `OrganizationGroup`, then enqueues one `MembershipChangeRequest` per child vault into the org's `MembershipQueue` (a bounded, rate-limited semaphore — default 2 concurrent in-flight, 1000 backlog). `MembershipDispatcher` drains the queue one entry at a time, wrapping each per-vault `apply_cascade_action_for_vault` call in a `tokio::time::timeout` of `vault_conf_change_timeout_secs` (default 60s). On timeout the dispatcher logs a WARN, increments `ledger_vault_conf_change_stalled_total{reason=timeout}`, drops the request, and lets the next region-state delta re-derive the change. The legacy synchronous `RaftManager::cascade_membership_to_children` is preserved as a public API for the test path; no production callers remain. The disabled-by-default rate limiter (`--rate-limit`), reflection-always-on policy, and arbitrary-string-region (`ProvisionRegion`) defaults are documented in `docs/reference/configuration.md`.

---

## Conventions

### Toolchain

- `cargo +1.92` for build, clippy, test. `cargo +nightly` for fmt. Never fall back to unpinned `cargo`.
- Everyday commands: `just check-quick` (fast iteration), `just ci` (pre-merge gate), `just ready` (the same — there is no separate proto regen step under the wire protocol). Full catalog: `just --list`.

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
- The task edits a **generated file** (`Cargo.lock`, or any code emitted by the `define_protocol!` macro inside `crates/wire-services/`) — the hook blocks `Cargo.lock` directly, but emitting hand-edits into macro output is silently overwritten on the next build.
- The task adds a **new storage-key prefix family** without a data-residency plan. Invoke the `/add-storage-key` skill first; surface to a human if the residency pattern is unclear.
- The task adds or renames an **RPC method or service** without a matching update to `crates/wire/src/services/*.rs` (request/response types) + the `define_protocol!` invocation in `crates/wire-services/src/lib.rs`.
- An audit agent (`unsafe-panic-auditor`, `snafu-error-reviewer`, `data-residency-auditor`, `wire-reviewer`, `test-isolation-auditor`, `consensus-reviewer`) raises a finding you believe is wrong — surface the contradiction rather than override.
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
- `wire-reviewer` — wire-protocol message types, `define_protocol!` invocation, service wiring, `SlugResolver`, `WireError` attachment, opcode-range bounds
- `test-isolation-auditor` — server integration test hygiene
- `consensus-reviewer` — custom Raft / WAL / shard / saga invariants
- `documentation-reviewer` — user-facing docs (`README.md`, `CONTRIBUTING.md`, `DESIGN.md`, `WHITEPAPER.md`, `Justfile`, `docs/**`) plus user-facing source surface (`crates/types/src/config/**`, `crates/types/src/error_code.rs`, `crates/sdk/src/{lib,client}.rs`, `crates/services/src/services/**`, `crates/server/src/{main,config}.rs`, `crates/wire/src/services/**`, `crates/wire-services/src/lib.rs`) for factual accuracy against code + developer-experience principles (audience, problem framing, Hello World, single source of truth, progressive disclosure). Dispatches parallel subagents.

**Skills** (invoke via `/skill-name` or auto-triggered):

- `/add-new-entity` — dual-ID entity rollout
- `/add-storage-key` — new key constant with registry + tier validation
- `/new-rpc` — full RPC method addition (wire types + `define_protocol!` invocation + service trait impl)
- `/use-bon-builder` — bon patterns and gotchas
- `/define-error-type` — snafu variant shapes and `ErrorCode` wiring
- `/just-ci-gate` — authoritative pre-PR gate
- `/debug-integration-test` — debugging the consolidated server integration binary
- `/audit-claude-md` — periodic audit of CLAUDE.md / skills / agents / memories against current code

**Hooks** (`.claude/settings.json`):

- `PreToolUse` — blocks `git commit`, edits to `Cargo.lock`.
- `PostToolUse` on `.rs` edits — `cargo +nightly fmt` + `cargo +1.92 check -p <crate>` (first 15 errors surfaced).
- `PostToolUse` on `.rs` / `.md` edits — writing-check: flags fenced `ignore` blocks, untagged code-fence openers, and non-kebab-case markdown filenames.
- `PostToolUse` on `crates/state/src/system/keys.rs` edits — auto-spawns `data-residency-auditor` as a subagent; findings surface in the transcript.
- `PostToolUse` on `crates/server/tests/**` edits — auto-spawns `test-isolation-auditor` as a subagent.
- `PostToolUse` on documentation-sensitive paths (`Justfile`, root `Cargo.toml`, `crates/services/src/services/**`, `crates/server/src/{main,config}.rs`, `crates/types/src/config/**`, `crates/types/src/error_code.rs`, `crates/sdk/src/{lib,client}.rs`, root docs, `docs/**/*.md`) — auto-spawns `documentation-reviewer`, which fans out into parallel `Explore` subagents across doc partitions.
- `SessionStart` — reminder to call `mcp__plugin_serena_serena__activate_project` for this workspace.
