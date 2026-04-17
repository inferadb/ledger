# server

Binary entrypoint. Thin — most logic lives in `services` and below. Integration tests also live here.

## Bootstrap

CockroachDB-style: nodes auto-initialize on `start`, and the `init` CLI command confirms cluster membership. No `--single` / `--cluster` / `--peers` flags.

`bootstrap_node()` takes `HealthState` + `watch::Receiver<bool>` as **inputs** — don't create them inside and try to extract them afterwards, the ownership gets tangled.

`serve_with_shutdown()` accepts any `Future<Output = ()>`. A `watch::Receiver<bool>` waiting for `true` is the cleanest signal source.

## Integration tests

Consolidated into a single binary via `autotests = false` in `Cargo.toml` + an explicit `[[test]] name = "integration"` entry. All 18 server test files are submodules of `tests/integration.rs`.

Test files use `use crate::common::` — **never** `mod common;` at the file top. The `test-isolation-auditor` agent enforces this on every change under `crates/server/tests/`.

- `just test` — unit tests only (~8s)
- `just test-integration` — integration tests
- `just test-all` — full suite including `#[ignore]`

No `cargo nextest`. Standard `cargo test` only.

## Toolchain reminder

`cargo +1.92` for build/clippy/test; `cargo +nightly` for fmt. The Justfile pins these — don't silently fall back to `cargo` without the flag.

## Related tooling

- Agent: `test-isolation-auditor`
- Skill: `/debug-integration-test`
