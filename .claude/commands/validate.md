---
description: Full pre-PR validation — builds, tests, lints, docs, udeps, shellcheck. Pass `--full` to include integration tests.
---

# /validate

Run the complete validation gate for InferaDB Ledger. Stop and report on the **first failing phase** — do not proceed past a failure. Within a phase, collect every finding before reporting.

Arguments: `$ARGUMENTS`

If `$ARGUMENTS` contains `--full`, also run `just test-integration` in Phase 6.

## Auto-fix policy

- **Trivial, always auto-fix and re-run the phase:** formatting (`cargo +nightly fmt`).
- **Everything else:** report each issue with file path, line number, exact error excerpt, and a proposed fix. Do not modify code without the user's confirmation. This includes clippy violations, failed tests, doc warnings, udeps findings, shellcheck diagnostics, and build errors.

When proposing fixes, honor the project conventions in `CLAUDE.md`: `snafu` only in server crates, no `unwrap`/`expect` outside tests, no `TODO`/`FIXME`/`HACK`, no backwards-compat shims, no speculative abstractions. Use Serena symbolic tools for edits, not raw text grep.

## Phases

Run sequentially. After each phase prints its command, surface the exact invocation you ran and its exit status.

### Phase 1 — Format

```bash
cargo +nightly fmt --all --check
```

If it fails, run `cargo +nightly fmt --all` (auto-fix), then re-check. If it still fails, stop and report.

### Phase 2 — Lint

```bash
cargo +1.92 clippy --workspace --all-targets --all-features -- -D warnings
cargo +1.92 clippy --workspace --all-targets --no-default-features -- -D warnings
```

Report all warnings with file:line. Do not auto-apply `clippy --fix`.

### Phase 3 — Build (workspace)

```bash
cargo +1.92 build --workspace --all-targets
cargo +1.92 build --workspace --all-targets --all-features
cargo +1.92 build --workspace --all-targets --no-default-features
```

### Phase 4 — Build (per publishable crate, standalone)

For each of these crates, run the three feature variants below:

Crates: `inferadb-ledger-types`, `inferadb-ledger-store`, `inferadb-ledger-proto`, `inferadb-ledger-state`, `inferadb-ledger-raft`, `inferadb-ledger-services`, `inferadb-ledger-sdk`.

```bash
cargo +1.92 build -p <crate>
cargo +1.92 build -p <crate> --all-features
cargo +1.92 build -p <crate> --no-default-features
```

Rationale: workspace builds mask missing feature gates and transitive dep declarations that only surface when a crate is built in isolation (the state consumers will see when installing from crates.io).

If `--no-default-features` fails for a crate that genuinely requires defaults to compile, flag it — the crate should compile in some minimal configuration or document a required feature set.

### Phase 5 — Unit tests (mirror `just ci`)

```bash
just test
```

(equivalent to `cargo +1.92 test --workspace --lib`)

### Phase 6 — Integration tests (only if `--full` in `$ARGUMENTS`)

```bash
just test-integration
```

Skip this phase when `--full` is not supplied.

### Phase 7 — Doc tests

```bash
cargo +1.92 test --workspace --doc
```

### Phase 8 — Doc build (warnings denied)

```bash
just doc-check
```

(equivalent to `RUSTDOCFLAGS="-D warnings" cargo +1.92 doc --workspace --no-deps`)

### Phase 9 — Unused dependencies

```bash
just udeps
```

(equivalent to `cargo +nightly udeps --workspace`)

Report every unused dep. Do not remove without confirmation — some deps are intentionally present (e.g., re-exported types, build-graph influence). Ask before deleting.

### Phase 10 — Shell script lint

```bash
shellcheck -x scripts/*.sh scripts/lib/*.sh
```

If `shellcheck` is not installed, report with installation hint (`brew install shellcheck`) and mark the phase as skipped rather than passed. Do not execute the scripts themselves — most of them spawn live clusters and are out of scope for this gate.

## Final report format

After all phases complete (or the first one fails), emit a compact summary:

```
Validate: [PASS|FAIL]  (mode: standard|full)
  1. fmt         [✓|✗|skip]
  2. clippy      [✓|✗|skip]
  3. build-ws    [✓|✗|skip]
  4. build-crate [✓|✗|skip]
  5. test        [✓|✗|skip]
  6. test-int    [✓|✗|skip]
  7. doctest     [✓|✗|skip]
  8. doc         [✓|✗|skip]
  9. udeps       [✓|✗|skip]
 10. shellcheck  [✓|✗|skip]
```

Then, for any `✗` phase, include:
- Exact command that failed
- Stderr/stdout excerpt with the diagnostic
- Proposed fix (file:line + description)
- Whether auto-fix was applied or awaiting confirmation

Do not declare success on any phase without having actually run it and observed exit status 0.
