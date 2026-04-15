# Task Completion Checklist

## Before Marking Complete

Per CLAUDE.md: *A task is complete only when all pass — no "pre-existing issue" exceptions.*

### Primary gates (pick one)

```bash
/validate          # 10-phase comprehensive (fmt, clippy, build matrix, unit, doctest, doc, udeps, shellcheck)
/validate --full   # above + integration tests
just ci            # fmt-check + clippy + doc-check + test (the canonical CI command)
```

### Manual one-liner equivalent to `just ci`

```bash
cargo +nightly fmt --all --check \
  && cargo +1.92 clippy --workspace --all-targets -- -D warnings \
  && RUSTDOCFLAGS="-D warnings" cargo +1.92 doc --workspace --no-deps \
  && cargo +1.92 test --workspace --lib
```

Note: `doc-check` is part of `just ci` — don't omit it.

### Pre-PR (regenerate protos if `.proto` files changed)

```bash
just ready   # proto + fmt + clippy + test
```

### Optional deeper validation

```bash
just test-integration    # real Raft clusters
just test-stress         # throughput/scale
just test-proptest 10000 # property tests at high iteration count
just cluster-lifecycle   # 6-phase end-to-end
just crash-recovery      # SIGKILL mid-write drill
```

## Critical Rules (non-negotiable)
- Never `.unwrap()` / `.expect()` outside tests — use `.context(XxxSnafu)?` (server) or `?` with thiserror (SDK)
- Never `panic!()`, `todo!()`, `unimplemented!()`
- No `unsafe`
- No TODO/FIXME/HACK comments
- No backwards-compatibility shims, feature flags, or placeholder stubs
- Document all public items
- Never commit — the user handles git

## Toolchain
- Build / test / clippy: `cargo +1.92`
- Format / udeps: `cargo +nightly`
