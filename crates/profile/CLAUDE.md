# CLAUDE.md — profile

> Extends [root CLAUDE.md](../../CLAUDE.md). Root rules always take precedence.

## Purpose

Profiling workload driver. An SDK-consumer CLI that connects to a running ledger node and drives one of a small set of reproducible load presets (`throughput-writes`, `mixed-rw`, `check-heavy`). Used by `scripts/profile-server.sh` to provide consistent load during flamegraph capture.

## Owned Surface

- **Binary** (`src/main.rs`): clap CLI with one subcommand per preset.
- **Harness** (`src/harness.rs`): connect to the server, onboard a synthetic user, create an organization and vault, run a timed loop, print summary stats.
- **Workloads** (`src/workloads/*.rs`): one preset per file. Each exposes a single `async fn run(&Harness, Duration) -> Result<Summary>`.

## Local Golden Rules

1. **Deterministic load shape.** Workloads must generate load that's shape-stable across runs — fixed concurrency, fixed key-space size, no randomized think-time. Flamegraphs are compared across runs; variance in the driver pollutes the signal.
2. **No server/state internal deps.** Only `inferadb-ledger-sdk` and `inferadb-ledger-types`. Treat the cluster as a black-box gRPC endpoint. Depending on server internals re-couples what profiling is meant to measure.
3. **External slugs only.** `OrganizationSlug(u64)`, `VaultSlug(u64)`, `UserSlug(u64)` — never internal `*Id(i64)`. This crate is a consumer; identity discipline matches the SDK boundary (root rule 11).
4. **snafu, not thiserror.** This is not the SDK. Server-adjacent crates use snafu (root rule 7).

## Test Patterns

- Unit tests cover CLI arg parsing and harness configuration. Top-level attributes permit `unwrap_used`/`expect_used` in `#[cfg(test)]` modules (workspace clippy lints apply to every target).
- Runtime verification happens via `scripts/profile-server.sh` against a local server — no integration tests here.
