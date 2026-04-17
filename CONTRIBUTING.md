# Contributing to InferaDB Ledger

Thanks for your interest in contributing. This guide covers what you need for a first PR to land cleanly. Reference material for advanced testing (fuzzing, property tests, simulation) lives in [`docs/testing/`](docs/testing/).

- [Code of Conduct](#code-of-conduct)
- [Reporting Issues](#reporting-issues)
- [Development Setup](#development-setup)
- [Before You Submit](#before-you-submit)
- [Pull Requests](#pull-requests)
- [Using AI Assistants](#using-ai-assistants)
- [Project Conventions](#project-conventions)
- [Advanced Testing](#advanced-testing)
- [Troubleshooting](#troubleshooting)
- [Review Process](#review-process)
- [License](#license)
- [Questions?](#questions)

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). Report unacceptable behavior to [open@inferadb.com](mailto:open@inferadb.com).

## Reporting Issues

- **Bugs**: Search existing issues first. Include version, reproduction steps, expected vs. actual behavior, and logs.
- **Features**: Describe the use case, proposed solution, and alternatives considered.
- **Security**: Email [security@inferadb.com](mailto:security@inferadb.com). Do not open public issues.

## Development Setup

```bash
# Clone
git clone https://github.com/inferadb/ledger.git
cd ledger

# Install toolchain (Rust 1.92 pinned + nightly for fmt, via mise)
just setup

# Fast inner loop — fmt-check + clippy + unit tests
just check

# Full pre-PR gate — adds doc-check
just ci

# Everything the Justfile exposes
just --list
```

Prerequisites: Rust 1.92+, [mise](https://mise.jdx.dev/), [just](https://github.com/casey/just). Platform notes are in [README.md](README.md).

## Before You Submit

Run through this checklist before opening a PR:

- [ ] `just ci` passes locally (fmt-check + clippy + doc-check + unit tests)
- [ ] PR title follows [Conventional Commits](https://www.conventionalcommits.org/) (e.g. `feat(api): ...`, `fix(storage): ...`)
- [ ] New or changed behavior has a test (unit, integration, or proptest — pick the lightest level that exercises the change)
- [ ] Any `.proto` edits were followed by `just proto` and the regenerated code is staged
- [ ] No `unsafe`, `panic!`, `todo!()`, or `TODO`/`FIXME`/`HACK` comments in production code (enforced by the `unsafe-panic-auditor` agent)
- [ ] New storage-key prefixes have a matching `KEY_REGISTRY` entry and a residency tier (`data-residency-auditor` will flag otherwise)
- [ ] If you touched a golden rule surface (see [CLAUDE.md](CLAUDE.md)), the corresponding CLAUDE.md is still accurate

## Pull Requests

1. Fork and branch from `main`.
2. Follow [Conventional Commits](https://www.conventionalcommits.org/) in both commits and the PR title (the PR title becomes the squashed commit message).
3. Run `just ci` before submitting.
4. Update docs for API or behavior changes.
5. Submit with a description that covers the *why*, not just the *what*.

**Conventional Commit scope examples used in this repo:**

| Scope       | Surface                                |
| ----------- | -------------------------------------- |
| `api`       | gRPC services, proto                   |
| `storage`   | B+ tree, pages, WAL                    |
| `consensus` | Raft engine, reactor, shard, simulation|
| `raft`      | Saga orchestrator, apply pipeline      |
| `sdk`       | Rust client crate                      |
| `docs`      | Any `*.md` or `crates/*/CLAUDE.md`     |
| `ci`        | GitHub Actions, Justfile gates         |

## Using AI Assistants

This codebase is structured for AI-assisted development. Claude Code, Codex, and Cursor all have what they need to contribute safely:

- **[CLAUDE.md](CLAUDE.md)** (symlinked as `AGENTS.md`) — 14 non-negotiable golden rules covering proto codegen, storage keys, PII residency, error handling, consensus I/O boundaries, and test hygiene. Read this before editing any of those surfaces.
- **Per-crate `CLAUDE.md`** — `crates/*/CLAUDE.md` files extend the root rules with crate-specific invariants.
- **Proactive audit agents** (`.claude/agents/`) — six reviewers (`consensus-reviewer`, `data-residency-auditor`, `proto-reviewer`, `snafu-error-reviewer`, `test-isolation-auditor`, `unsafe-panic-auditor`) fire on matching file changes and surface violations.
- **Task skills** (`.claude/skills/`) — `/add-new-entity`, `/add-proto-conversion`, `/add-storage-key`, `/new-rpc`, `/define-error-type`, `/use-bon-builder`, `/debug-integration-test`, `/just-ci-gate`, `/audit-claude-md`. Use these instead of improvising when their trigger applies.
- **Hooks** (`.claude/settings.json`) — block `git commit` from agents, block edits to generated proto and `Cargo.lock`, auto-run `cargo +nightly fmt` + `cargo +1.92 check` after `.rs` edits. These exist so agents cannot silently violate golden rules.

If an agent review contradicts your judgment, surface the contradiction in the PR rather than overriding it — the audit rules exist because the class of bug has been observed at least twice.

## Project Conventions

Detailed conventions (toolchain invocation, identifier newtypes, storage-key families, doc-comment style, Serena MCP navigation) are in [CLAUDE.md](CLAUDE.md). One-line summary for humans skimming:

- `cargo +1.92` for build/clippy/test, `cargo +nightly` for fmt. Never unpinned `cargo`.
- Server crates use `snafu` only; SDK uses `thiserror`; `anyhow` is banned.
- Internal IDs are `{Entity}Id(i64)`; external slugs are `{Entity}Slug(u64)`. Translation happens at the gRPC boundary.
- Storage-key prefixes: bare (primary), `_dir:`, `_idx:`, `_meta:`, `_shred:`, `_tmp:`, `_audit:`. Each has a residency tier.

## Advanced Testing

Everyday tests are covered by `just test`, `just test-integration`, and `just test-proptest`. Deeper references live in [`docs/testing/`](docs/testing/):

- [`docs/testing/fuzzing.md`](docs/testing/fuzzing.md) — cargo-fuzz targets, crash-artifact workflow
- [`docs/testing/property.md`](docs/testing/property.md) — proptest suites and strategies
- [`docs/testing/simulation.md`](docs/testing/simulation.md) — deterministic consensus simulation

## Troubleshooting

**`just test-integration` fails with `address already in use`.** The integration suite spawns real clusters on real ports. Parallelism is capped at 4 in the Justfile; don't override `--test-threads` upward without understanding `scripts/run-integration-tests.sh`.

**`just proto` fails with "expected exactly one build output directory".** Previous build artifacts left multiple hash directories under `target/debug/build/inferadb-ledger-proto-*`. Run `cargo clean -p inferadb-ledger-proto` then `just proto` again.

**Clippy complains about formatting after `just check`.** Run `just fix` to auto-apply `cargo fmt` and `cargo clippy --fix`, then rerun `just check`.

**An audit agent flagged my change and I think it's wrong.** Surface the contradiction in the PR description rather than working around it. Golden rules exist because the same class of bug has surfaced at least twice.

More operational issues: [`docs/faq.md`](docs/faq.md) and [`docs/operations/`](docs/operations/).

## Review Process

1. CI runs fmt-check, clippy, doc-check, and tests.
2. A maintainer reviews. Expect initial feedback within a few working days; ping on Discord if a PR is stalled for more than a week.
3. Address feedback. Force-push or push new commits — the PR will be squashed on merge, so history inside a PR is flexible.
4. A maintainer merges on approval.

## License

Contributions are dual-licensed under [Apache 2.0](LICENSE-APACHE) and [MIT](LICENSE-MIT).

## Questions?

- [Discord](https://discord.gg/inferadb)
- [open@inferadb.com](mailto:open@inferadb.com)
