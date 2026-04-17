# CLAUDE.md — server

> Extends [root CLAUDE.md](../../CLAUDE.md). Root rules always take precedence.

## Purpose

Binary entrypoint. Thin — most logic lives in `services` and below. This crate owns the CLI, bootstrap, graceful shutdown wiring, and all server integration tests (consolidated into a single binary per root rule 13).

## Load-Bearing Files

These files are load-bearing — their invariants ripple beyond the local file. Not off-limits; use caution and understand the ramifications before editing.

| File                                                                 | Reason                                                                                                                             |
| -------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `tests/integration.rs`                                               | Single-binary submodule registry. Files not registered here don't run; restructuring breaks the `test-isolation-auditor` contract. |
| `tests/common/mod.rs`                                                | Shared test helpers (`allocate_ports`, `TestCluster`). Per-test copies of this diverge and leak.                                   |
| `Cargo.toml` (`autotests = false` + `[[test]] name = "integration"`) | Consolidation discipline. Removing this splits tests back into per-file binaries, blowing up CI time and breaking `common` usage.  |

## Owned Surface

- **CLI + main**: `src/main.rs` — argument parsing, tracing setup, bootstrap call, shutdown wait.
- **Bootstrap wiring**: `src/bootstrap.rs` / equivalent — wires `HealthState`, shutdown channels, service registration.
- **Integration test binary**: `tests/integration.rs` + submodules.

## Test Patterns

- 24+ integration test modules as submodules of `tests/integration.rs`. Add a file → add `mod <name>;` to `integration.rs`.
- Every test file starts with `use crate::common::` — **never** `mod common;` at the file top. Per-test `mod common;` copies silently diverge.
- Port allocation via `allocate_ports` from `tests/common/mod.rs`. Hardcoded ports cause flaky tests under parallel cargo runs.
- `TestCluster` drops all nodes before the test returns. Leaked clusters pollute subsequent tests.
- Required clippy allows live at the top of each test file (e.g. `#![allow(clippy::unwrap_used)]`, `#![allow(clippy::panic)]`). Tests are exempt from the `unsafe-panic-auditor` discipline.

## Local Golden Rules

1. **Integration tests use `use crate::common::`; never `mod common;`.** Audited by `test-isolation-auditor` on every change under `tests/`.
2. **A new integration test file requires `mod <name>;` in `tests/integration.rs`.** Unregistered files silently don't run — the test you wrote provides zero coverage.
3. **Port allocation goes through `allocate_ports`.** Hardcoded port literals cause flaky test runs under `cargo test --test-threads`.
4. **`bootstrap_node()` takes `HealthState` + `watch::Receiver<bool>` as inputs.** Don't construct them inside and return them out — ownership gets tangled with the shutdown coordinator.
5. **`serve_with_shutdown()` accepts any `Future<Output = ()>`.** A `watch::Receiver<bool>` awaiting `true` is the canonical shutdown signal; don't invent a new signal type.
6. **Cross-test shared mutable state is forbidden.** Each test owns its own `TestCluster` and cleans up. Static `OnceCell<Cluster>` constructs are a bug — tests must not share state.
