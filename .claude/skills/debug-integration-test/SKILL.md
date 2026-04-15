---
name: debug-integration-test
description: Diagnostic playbook for failing integration tests in crates/server/tests/. Use when an integration test hangs, flakes, port-conflicts, fails to compile because of a `mod common;` mistake, or only fails under parallel `--test-threads`. Covers the single-binary submodule layout, the `NEXT_PORT` allocator, TestCluster teardown, and the commands to narrow a failure quickly.
disable-model-invocation: true
---

# debug-integration-test

Integration tests in `crates/server/tests/` spawn real Raft clusters (1–3 nodes each), real tonic gRPC servers, real file-backed storage, real saga orchestrators, and real background jobs. They are consolidated into a single binary (`integration.rs`) and run at `--test-threads=4`. Failures are rarely "the code is wrong" in isolation — they are usually port contention, submodule wiring, task-leak across tests, or timing under load. This skill is the diagnostic order-of-operations.

## Layout invariants

- `crates/server/Cargo.toml` sets `autotests = false` and declares exactly one `[[test]] name = "integration"`.
- `crates/server/tests/integration.rs` declares every test file as a submodule: `mod write_read;` etc.
- `crates/server/tests/common/mod.rs` is `TestCluster`, `TestNode`, port allocator, client builders.
- **Every test file** MUST:
  - Start with `#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]`.
  - Use `use crate::common::{...}` — **never** `mod common;`. A `mod common;` inside a submodule is the #1 cause of "file compiles alone but the integration binary fails."
  - Get ports via `crate::common::allocate_ports(n)` — a `NEXT_PORT: AtomicU16 = 30_000` counter. Hardcoded ports are the #1 cause of flakes under parallel runs.

## Diagnostic order

### 1. Reproduce deterministically

Single test, single thread, live output:

```bash
cargo +1.92 test -p inferadb-ledger-server --test integration <test_name> -- --test-threads=1 --nocapture
```

If the test passes single-threaded but fails at `--test-threads=4`, the bug is state shared across tests — ports, global statics, filesystem paths, or a `tokio::spawn` that outlives the test.

### 2. Fail-fast the whole binary

```bash
just test-integration-ff
```

Stops at first failure — useful when you've made a change and want to find the first test that regressed rather than waiting for the full run.

### 3. Classify the failure

| Symptom                                           | Likely cause                                                               | Next step                                                            |
| ------------------------------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| "address already in use" / `AddrInUse`            | Hardcoded port, or `allocate_ports` called with too few                    | grep for literal `127.0.0.1:` + check call sites                     |
| `cannot find type `TestCluster`in module`common`` | Submodule used `mod common;` instead of `use crate::common::`              | Fix the submodule's imports                                          |
| Test passes alone, fails in full run              | Task leak, shared FS state, global counter not reset                       | Check for missing `cluster.shutdown().await` or static mutable state |
| Test hangs forever                                | Raft election never completes, or a client awaits a reply that never comes | Enable `RUST_LOG=debug` and re-run with `--nocapture`                |
| Intermittent: passes 9/10                         | Timing: election timeout, WAL fsync race, port reuse                       | Bump timeouts locally; look for missing `wait_for_leader`            |
| `#[test]` not discovered                          | Forgot `mod <file>;` in `integration.rs`                                   | Add the mod line                                                     |
| Panic in a completely unrelated test              | Previous test leaked a background task that panicked later                 | Run with `--test-threads=1` to confirm; then audit teardown          |
| `common`-only change breaks one test              | Test-specific assumption baked into `common::`                             | Roll back and add a local helper instead                             |

### 4. Enable tracing

All production services route through `tracing`. For stubborn failures:

```bash
RUST_LOG=inferadb_ledger_raft=debug,inferadb_ledger_consensus=debug,inferadb_ledger_services=info \
  cargo +1.92 test -p inferadb-ledger-server --test integration <name> -- --test-threads=1 --nocapture
```

Raft leader election, proposal apply, and snapshot paths all log at `debug`. The reactor loop logs at `trace` — use sparingly; it drowns everything else.

### 5. Inspect the test's cluster lifecycle

Every test that spawns a `TestCluster` must also drop it cleanly. Look for:

- `let cluster = TestCluster::new(...).await;` — make sure it's dropped at scope end (not early-returned past without cleanup).
- Any `tokio::spawn(async move { ... })` inside the test that doesn't have a matching `.abort()` or join on test exit.
- Clients (`create_write_client`, `create_read_client`) — no explicit close needed, but outstanding streams keep channels alive.

### 6. Confirm it's not a pre-existing failure

MEMORY records 4 pre-existing `backup_restore` failures (connection refused) unrelated to most changes. Before debugging further, run the same test on `main` to see if it was already failing.

## Adding a new integration test

Checklist to avoid creating a new class of problem:

- [ ] New file under `crates/server/tests/<name>.rs`.
- [ ] Add `mod <name>;` to `crates/server/tests/integration.rs` (alphabetical with the existing list).
- [ ] Top-of-file `#![allow(...)]` matching the other test files.
- [ ] `use crate::common::{...}` — never `mod common;`.
- [ ] Ports via `crate::common::allocate_ports(n)`. Never a literal port.
- [ ] If the test spawns 2+ nodes, use `TestCluster::with_data_regions` (not `new` + manual wiring).
- [ ] `.await` the cluster's `shutdown` (or let it drop) before the test returns.
- [ ] If the test is slow (>5s), mark it `#[ignore]` and put it under `test-stress` instead.

## Stress / external / recovery variants

Separate binaries, separate commands:

| Binary        | Command                      | When to use                                          |
| ------------- | ---------------------------- | ---------------------------------------------------- |
| `integration` | `just test-integration[-ff]` | Default. Small, fast, parallel.                      |
| `stress`      | `just test-stress[-ff]`      | Heavy workloads, throughput, scale                   |
| External      | `just test-external`         | Against a live cluster (requires `LEDGER_ENDPOINTS`) |
| Recovery      | `just test-recovery[-ff]`    | Crash injection, WAL replay, snapshot recovery       |

## Common mistakes

- **`mod common;` in a submodule**: copy-pasted from a standalone-test template. Always `use crate::common::`.
- **Literal port**: `"127.0.0.1:50051"` in a test. Every occurrence is a flake-in-waiting. Use `allocate_ports`.
- **Forgot `mod X;` in `integration.rs`**: test file compiles on its own but isn't part of the binary. `cargo test <name>` reports zero tests.
- **Cross-test leakage via static**: a `OnceCell` or `lazy_static!` in test code that carries state between tests. Should be `fn`-local or `TestCluster`-scoped.
- **Test-specific logic in `common/mod.rs`**: when one test needs a variant setup, the fix is a helper in that test file, not a new branch in `common`. `common` is already large; keep it generic.
- **Assuming elections finish in N ms**: under `--test-threads=4` + file-backed WAL, election can take longer than a tight timeout. Use the helpers in `common` that poll-with-timeout instead of sleeping.

## References

- `crates/server/tests/integration.rs` — canonical submodule list
- `crates/server/tests/common/mod.rs` — `TestCluster`, `allocate_ports`, client builders
- `Justfile` — `test-integration`, `test-integration-ff`, `test-stress`, `test-recovery`, `test-external`
- `CLAUDE.md` — server integration test wiring notes
