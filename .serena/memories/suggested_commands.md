# Development Commands

## Validation gates

```bash
/validate           # comprehensive 10-phase gate (fmt, clippy, build matrix, tests, doctests, docs, udeps, shellcheck)
/validate --full    # above + integration tests
just ci             # primary CI gate: fmt-check + clippy + doc-check + test (unit)
just check          # pre-commit: fmt + clippy + test
just check-quick    # faster pre-commit: fmt + clippy only
just ready          # pre-PR: proto + fmt + clippy + test
```

## Tests (via `just`)

```bash
just test                    # unit tests (--workspace --lib), ~8s
just test-ff                 # unit tests, fail-fast
just test-integration        # integration tests (spawns real clusters)
just test-integration-ff     # integration tests, fail-fast
just test-stress             # throughput/scale stress tests
just test-stress-ff          # stress tests, fail-fast
just test-recovery           # crash recovery tests (store crate dual-slot commit)
just test-recovery-ff        # crash recovery tests, fail-fast
just test-all                # full suite including #[ignore] tests
just test-proptest [cases]   # property tests (default 10000 iterations)
just test-external           # against live cluster (requires LEDGER_ENDPOINTS)
just test-external-cluster   # spawn a cluster + run server external tests
just test-sdk-cluster        # spawn a cluster + run SDK e2e tests
just test-crate <crate>      # tests for a specific crate
just test-one <name>         # single test with output
```

## Stress + lifecycle scripts (via `just`)

```bash
just stress-quick            # smoke-level throughput (~1 min)
just stress-throughput       # batched + multi-region throughput vs advisory targets
just stress-correctness      # scale correctness (state root parity, snapshot determinism, watch)
just cluster-lifecycle       # 6-phase e2e (bootstrap, join, transfer, shutdown, rebuild)
just crash-recovery          # SIGKILL node mid-write, verify convergence
just check-port-consumption  # gRPC channel caching regression (TIME_WAIT tracking)
```

## Build, format, lint, docs

```bash
just build           # debug build (--workspace)
just build-release   # release build
just fmt             # format (nightly required)
just fmt-check       # verify formatting without modifying
just clippy          # lint
just doc             # build rustdoc
just doc-check       # build rustdoc with -D warnings (CI gate)
just doc-open        # build rustdoc and open in browser
just proto           # regenerate protobuf code (after .proto changes)
just proto-lint      # buf lint on proto definitions
just udeps           # detect unused dependencies (nightly)
just run             # run server (dev mode)
just run-release     # run server (release)
just clean           # cargo clean
just clean-stale [age]  # prune stale target/ artifacts older than N days (default 7)
```

## Using cargo directly

```bash
cargo +1.92 build --workspace               # all crates
cargo +1.92 build -p inferadb-ledger-types  # specific crate
cargo +1.92 test --workspace --lib          # unit tests
cargo +1.92 test -p inferadb-ledger-state   # specific crate's tests
cargo +1.92 test <name> -- --nocapture      # single test with output
cargo +nightly fmt                          # format (nightly required)
cargo +1.92 clippy --workspace --all-targets -- -D warnings
cargo +1.92 doc --workspace --no-deps       # generate docs
cargo +nightly udeps --workspace            # unused dependency detection
```
