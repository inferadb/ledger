---
name: just-ci-gate
description: Run the authoritative `just ci` gate (fmt-check + clippy + doc-check + test) with clear pass/fail reporting. Use this as the explicit checkpoint before claiming work is complete. Never claim "done" without this passing.
disable-model-invocation: true
---

# just-ci-gate

This skill runs `just ci` — the authoritative readiness check defined in `Justfile` as `fmt-check + clippy + doc-check + test`. It is **user-only**: you invoke it with `/just-ci-gate` when you want the CI gate run and reported honestly.

## Why user-only

CLAUDE.md requires all four checks pass — no "pre-existing issue" exceptions. Making this model-invocable invites opportunistic partial runs. Explicit invocation keeps the checkpoint semantics.

## Procedure

1. Run the full gate and capture both stdout and exit code:

   ```bash
   just ci
   ```

2. Do NOT run individual steps in isolation as a substitute. The ordering in `Justfile` (`fmt-check → clippy → doc-check → test`) matters: each earlier step catches faster failures.

3. If the command exits non-zero, **do not** summarize success. Report:
   - Which stage failed (`fmt-check`, `clippy`, `doc-check`, or `test`).
   - The first ~20 lines of failure output (the root cause, not the cascade).
   - Whether the failure is in code touched in this session (actionable) or pre-existing (still blocks — CLAUDE.md is explicit: no "pre-existing issue" exceptions).

4. On success, report a single line: `just ci passed (fmt + clippy + doc + test).`

## Variants to remember

- `just check` — fmt + clippy + test (skips doc-check)
- `just check-quick` — fmt + clippy only (fast iteration)
- `just ready` — proto + fmt + clippy + test (use after proto changes)
- `just test-ff` — fail-fast unit tests (faster debug loop)
- `just test-integration` / `just test-stress` / `just test-recovery` — targeted integration

Use these during iteration. `just ci` is the gate before declaring done.

## Toolchain reminder

`Justfile` pins `cargo +1.92` for build/clippy/test and `cargo +nightly` for fmt. If `just ci` fails with a "toolchain not found" error, run:

```bash
rustup toolchain install 1.92 && rustup toolchain install nightly
```

Do **not** silently fall back to `cargo` without the toolchain flag — clippy lints differ between versions and will drift from CI.

## Anti-patterns

- Running `cargo test` alone and calling it CI-passing.
- Using `|| true` or `2>/dev/null` to suppress failures.
- Running in a subset crate (`cargo test -p foo`) — `just ci` operates on the whole workspace.
- Declaring success based on compiler output without reading the test result summary at the end.
