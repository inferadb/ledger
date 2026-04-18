# Profiling InferaDB Ledger

Opt-in, manual CPU profiling across three targets: criterion benchmarks,
integration tests, and the server under load. All targets share the
`[profile.profiling]` Cargo profile (release optimization with debug symbols
restored). For the server target, two complementary lenses: stack-frame
sampling and semantic span sampling.

## When to reach for profiling

Profiling is hypothesis-driven, not exploratory. Reach for it when you have a
specific question:

- "Writes feel slow — is the bottleneck WAL fsync, B-tree split, or state-root
  recompute?"
- "This benchmark regressed 15% on `main` — where did the extra time come from?"
- "Integration test X times out intermittently under load — what's blocking?"

If you don't have a hypothesis, a flamegraph will tell you very little beyond
what you already suspect. Start with metrics (`metrics-exporter-prometheus`
under `/metrics`) or tracing spans.

## Install

One-time setup:

```bash
cargo install samply         # sampling profiler (default on macOS)
cargo install flamegraph     # sampling profiler (default on Linux, fallback on macOS)
cargo install inferno        # folded-stack → SVG post-processor (needed for spans mode)
```

Verify:

```bash
just doctor-profiling
```

### Platform prerequisites

**macOS.** `samply` is the default sampler — it uses the native
CoreSymbolication unwinder and handles inlined Rust frames well. No `sudo`
required. If you prefer `cargo-flamegraph`, set `PROFILER=flamegraph`; it
invokes `dtrace`, which the recipes run without `sudo` by spawning the target
as a child.

**Linux.** `cargo-flamegraph` is the default sampler — it wraps `perf record`
and performance is excellent. `perf` needs `kernel.perf_event_paranoid <= 1`:

```bash
sudo sysctl -w kernel.perf_event_paranoid=1
```

`just doctor-profiling` reads the current value and prints the fix if it's too
high. If you can't tune that sysctl, set `PROFILER=samply` — samply works on
Linux too, without the perf_event_paranoid requirement.

## Two lenses: stack vs. span

The server is async-tokio-heavy. That means stack-sampling profilers see
`tokio::runtime::task::raw::poll` and combinator machinery on nearly every
sample — technically correct, practically unhelpful if you want to know which
*logical operation* the program is spending time in.

There are two complementary lenses. Pick based on what you're asking:

| Question | Lens | Recipe |
|---|---|---|
| "What code is on-CPU right now?" | **Stack** — sampling at ~997 Hz | `just profile-server` |
| "Which logical operation is taking the time?" | **Span** — tracing-flame over existing instrumentation | `just profile-server-spans` |
| "What's the leaf-level hot function?" | Stack | `just profile-server` |
| "Why is this request path slow end-to-end?" | Span (then stack once you've localized) | `just profile-server-spans` |

Example: suppose `write()` p99 is regressing. The span flamegraph shows
`write` → `raft_apply` → `wal_append` dominating — now you know `wal_append`
is where the time went. A stack flamegraph on the same workload points at
`memcpy` and `sync_data` under `wal_append` — now you know whether it's a CPU
copy or an fsync stall. Stack and span answer different questions; use both.

Span flamegraphs only show **traced** code. If you see a blind spot (a
region you expected to see but don't), that's a missing `#[tracing::instrument]`
annotation, not a profiling bug — add the span and re-run.

## The four recipes

### `just profile-bench <crate> <bench>`

Profiles a criterion benchmark. The benchmark is compiled under
`[profile.profiling]` and driven by criterion's `--profile-time` phase.

```bash
just profile-bench types merkle
# → profiles/bench-types-merkle-20260417T181500Z.svg
```

### `just profile-test <crate> <test-name>`

Profiles a single integration test from the consolidated integration binary.

```bash
just profile-test server onboarding::test_verify_email_code_new_user
# → profiles/test-server-onboarding__test_verify_email_code_new_user-20260417T181500Z.svg
```

The test runs with `--test-threads=1` to avoid cross-test noise in the capture.

### `just profile-server <workload> [duration]` — stack lens

Starts the ledger node under the platform-default sampler, bootstraps via
`inferadb-ledger init`, warms up for 5 seconds, then drives the measured
workload for `duration` seconds (default 60). Sends `SIGINT` to the server for
a clean shutdown; the sampler writes its output as the server exits.

```bash
just profile-server throughput-writes 60
# → profiles/server-throughput-writes-20260417T181500Z.{svg|json.gz}
```

### `just profile-server-spans <workload> [duration]` — span lens

Same orchestration, but runs the server with `--features profiling` and
`--flamegraph-spans <path>`. No sampler. On shutdown, `inferno-flamegraph`
post-processes the folded-stack file into an SVG that matches the sampling
recipe's output shape.

```bash
just profile-server-spans throughput-writes 60
# → profiles/server-spans-throughput-writes-20260417T181500Z.svg
```

Span mode has measurable overhead (~1–5% depending on span density). Use it
for structural analysis, not absolute-throughput measurements.

### Workloads (both server recipes)

- `throughput-writes` — tight write loop, 10k-entry key-space, exercises the
  update path.
- `mixed-rw` — 70/30 writes-to-reads against a pre-seeded vault.
- `check-heavy` — 90% `CheckRelationship` RPC calls against a 1,000-tuple
  pre-seeded relationship set (10 resources × 10 relations × 10 subjects),
  10% relationship writes. Exercises the storage-primitive existence check
  the Engine layer calls into when serving authorization decisions. The
  right target for read-path latency, slug resolution, validation, and
  state-layer lookup regressions.

All workloads use fixed concurrency and fixed key-space size so the flame
graph shape is stable across runs.

## Reading a flamegraph

- **Width = total time**, not wall-clock. A box twice as wide was on-CPU twice
  as long (stack mode) or in a span twice as long (span mode).
- **X-axis is not time-ordered.** Sibling boxes are sorted alphabetically by
  symbol name, not by when they ran.
- **Height = stack depth.** A tall tower means deep inlining (stack mode) or
  deep span nesting (span mode).
- **Click to zoom.** Drill into a subtree; `Ctrl+F` searches symbols and
  highlights matching frames across the entire graph.

Recurring patterns in this codebase:

**Stack flamegraphs (profile-server):**

- **tokio runtime frames** at the base — expected in async code. Look at what
  sits on top of them.
- **`sha2::Sha256::finalize`** in tall narrow columns — state-root or block
  hashing. Expected hot. If it's wide, check for fan-out in the caller (e.g.,
  a loop that could be batched).
- **B+ tree leaf split** frames under a write path — a hot spot signals a
  high-churn page; consider tuning `BTreeCompactionConfig` or key-space
  layout.
- **`memcpy` or `sync_data`** under a wide `wal` frame — fsync cost.

**Span flamegraphs (profile-server-spans):**

- **`raft_apply`** as a wide top-level frame — expected on a write-heavy
  workload. Drill into it.
- **`wal_append`** wide under `raft_apply` — I/O dominant. Cross-check with
  the stack flamegraph to see whether it's fsync or memcpy.
- **Blind spots** — a region you expected to see but don't appear. Means the
  code isn't currently instrumented; add `#[instrument]` and re-run.

## Gotchas

- **`codegen-units = 1` and `lto = "thin"`** (inherited from release) cause
  aggressive inlining. Small functions merge into their callers, so leaves
  of a stack flamegraph can look unfamiliar. This is the point — you're
  profiling what production runs, not what `cargo build` produces.
- **First build is slow.** The profiling profile has its own
  `target/profiling/` cache; the first `just profile-*` invocation spends a
  minute or two in cargo. Subsequent runs are incremental.
- **Warmup matters.** For server profiling, the first ~5 seconds of traffic
  populate caches and advance Raft past initial leader election. Shorter
  captures are dominated by startup noise. The recipes already include a 5s
  warmup phase — don't skip it.
- **Don't compare macOS and Linux flamegraphs.** The stack unwinders differ;
  what looks like a bug may be an unwinder artifact. Pick one platform and
  stay there within a comparison.
- **Span mode has overhead.** 1–5% slower than unmeasured for span-dense
  workloads. Don't use it for absolute-throughput comparisons; use it for
  structural analysis.
- **Span mode needs the feature.** The `--flamegraph-spans` flag only exists
  when the server is built with `--features profiling`. `scripts/profile-server.sh
  spans` handles this automatically; if you're invoking the server by hand,
  remember the feature flag.
- **Don't `kill -9` the server during span capture.** The `FlushGuard`'s
  `Drop` impl is what writes the folded-stack file. SIGKILL skips drops; the
  file ends up empty. `SIGINT` (or Ctrl-C) is the right signal.
- **Profile outputs are gitignored.** Share via PR attachments or a gist;
  they're too large to commit and rot quickly.

## Alternate tooling

- **`tokio-console`** — live interactive view of tokio task states, blocked
  time, resource waits. Not a flamegraph; the right tool for "which task is
  stuck, and on what?". Requires `RUSTFLAGS="--cfg tokio_unstable"` and a
  console subscriber; not wired into these recipes.
- **`cargo-instruments`** — macOS-only, Xcode-backed. Richer on-CPU + I/O
  breakdown. Not wired into recipes.

## Future work

- `pprof-rs` HTTP endpoint for live production profiling (always-on
  instrumentation; separate concern from this manual flow).
- Baseline-and-diff regression gates once the bench suite is mature.
- Heap profiling (`dhat`, `heaptrack`, jemalloc stats) — separate concern from
  CPU.
- Wired `tokio-console` integration for blocked-task analysis.
