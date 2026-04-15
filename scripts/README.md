# scripts/

Operational and integration test scripts for InferaDB Ledger. Unit, lib, and
Rust-level integration tests live in `crates/*/tests/` and run via `just test*`.
These scripts cover scenarios that require a real binary, real processes, and
real signals.

## Running

Every script is invoked via a `just` recipe — see `just --list`. Direct
invocation (`./scripts/foo.sh`) is always supported and equivalent.

## Layout

| Script                      | Just recipe                                            | Purpose                                                                                   |
| --------------------------- | ------------------------------------------------------ | ----------------------------------------------------------------------------------------- |
| `stress-quick.sh`           | `just stress-quick`                                    | Smoke-level throughput tests (~1 min, release build).                                     |
| `stress-throughput.sh`      | `just stress-throughput`                               | Batched + multi-region throughput against advisory targets.                               |
| `stress-correctness.sh`     | `just stress-correctness`                              | Scale-level correctness: state root parity, snapshot determinism, watch streaming.        |
| `check-port-consumption.sh` | `just check-port-consumption`                          | Regression test for gRPC channel caching via TIME_WAIT accounting.                        |
| `cluster-lifecycle-test.sh` | `just cluster-lifecycle`                               | 6-phase e2e: bootstrap → join → non-leader writes → leader transfer → shutdown → rebuild. |
| `crash-recovery.sh`         | `just crash-recovery`                                  | SIGKILL a node mid-write, restart, verify convergence + zero data loss.                   |
| `run-integration-tests.sh`  | `just test-external-cluster` / `just test-sdk-cluster` | Spawn a local cluster and run server or SDK integration tests against it.                 |
| `update-dependencies.sh`    | —                                                      | Update `Cargo.lock` against crates.io (disables `[patch.crates-io]` temporarily).         |
| `lib/cluster-bootstrap.sh`  | —                                                      | Shared helpers for spawning/tearing down a local cluster. Sourced, not executed.          |

## Performance targets

Stress-test performance thresholds live in `crates/server/tests/stress_test.rs`
as the single source of truth. The shell scripts invoke the tests; the tests
emit advisory warnings when targets are missed and panic on hard violations.

## Adding a script

1. Place it in `scripts/`, make it executable, add `#!/usr/bin/env bash` and
   `set -euo pipefail`.
2. Share helpers via `lib/cluster-bootstrap.sh` (or extend it).
3. Add a `just` recipe so it's discoverable via `just --list`.
4. Update this table.
5. Verify with `shellcheck -x scripts/*.sh scripts/lib/*.sh` — zero findings is
   required (the `/validate` slash command enforces this).

## Dependencies

- `grpcurl`, `jq` — used by `cluster-lifecycle-test.sh` and `crash-recovery.sh`
  to drive gRPC endpoints. Install via `brew install grpcurl jq`.
- `shellcheck` — `brew install shellcheck`.
- `uuidgen` — macOS/BSD ships it; Linux needs `util-linux`.
