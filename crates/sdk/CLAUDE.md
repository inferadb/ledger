# CLAUDE.md — sdk

> Extends [root CLAUDE.md](../../CLAUDE.md). Root rules always take precedence.

## Purpose

Consumer-facing Rust client. Every external caller — first-party services, third-party integrators — uses this crate. **The one workspace crate that uses `thiserror` instead of `snafu`**, because error ergonomics matter more here than server-side diagnostic detail. Retry, cancellation, leader caching, and circuit-breaker behavior live here; a regression is visible to every consumer.

## Load-Bearing Files

These files are load-bearing — their invariants ripple beyond the local file. Not off-limits; use caution and understand the ramifications before editing.

| File                     | Reason                                                                                                                                                                 |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/error.rs`           | `SdkError` shape. `ServerErrorDetails` must stay `Box`-wrapped — bare `Option<ServerErrorDetails>` inflates `Result<T, SdkError>` and trips clippy `result_large_err`. |
| `src/retry.rs`           | `with_retry_cancellable` loop. Retry ordering + cancellation via `tokio::select! { biased; }`; a naive `backon` retry loop cannot be cancelled.                        |
| `src/client.rs`          | `LedgerClient` retry cycle, metrics emission, connection-pool integration.                                                                                             |
| `src/circuit_breaker.rs` | State machine (Closed / Open / HalfOpen). A bug here either hammers failing endpoints or rejects healthy ones.                                                         |

## Owned Surface

- **`LedgerClient`** — public API: authenticate, read/write, admin, leader discovery.
- **Config** (`src/config.rs`): `ClientConfig`, `TlsConfig`, `CircuitBreakerConfig` — all bon fallible builders.
- **Errors** (`src/error.rs`): `SdkError`, `ResolverError` (thiserror).
- **Connection layer**: `connection.rs`, `circuit_breaker.rs`, leader caching via `region_resolver.rs` / `discovery.rs`.
- **Metrics**: `SdkMetrics` trait + `NoopSdkMetrics` / `MetricsSdkMetrics` in `src/metrics.rs`.
- **Retry entry point**: `with_retry_cancellable(method, pool, ...)` in `src/retry.rs` — used by every RPC call in `client.rs`.
- **Streaming / ops**: `src/streaming.rs`, `src/ops/`.

## Test Patterns

- Unit tests use `mock` fixtures (`src/mock/`) and mocked `tonic::Channel`.
- Retry tests use fake endpoints with known-failing addresses to exercise the `tokio::select!` cancellation path.
- E2E tests (`tests/`) require a running cluster; they fail with transport errors in local dev by design.
- Circuit-breaker tests parameterize thresholds; never assume production defaults.

## Local Golden Rules

1. **`thiserror` only.** Do not import `snafu` types into SDK error definitions. The crate boundary is contractual: server crates are snafu, SDK is thiserror.
2. **`ServerErrorDetails` is boxed** — `Option<Box<ServerErrorDetails>>` on every error variant that carries it. Bare `Option<ServerErrorDetails>` inflates `Result<T, SdkError>` (clippy `result_large_err`).
3. **`with_retry_cancellable` requires `method: &str`.** Used for retry metrics labels and circuit-breaker key. Call sites that pass `""` lose observability; call sites that pass a dynamic string blow up label cardinality.
4. **bon `Option<T>` setters accept `T`, not `Option<T>`.** `.field(Some(x))` is a compile error — write `.field(x)`. bon auto-wraps.
5. **`#[builder(default)]` on `Option<T>` is redundant.** bon auto-infers `None`; adding the attribute produces a compiler error.
6. **Leader cache updates are term-gated.** Hints for older terms MUST be dropped — a stale reply reaching the cache first would overwrite a newer leader and cause a retry storm.
7. **`SdkError::CircuitOpen` is non-retryable.** The retry loop exits immediately when the circuit opens; otherwise retries would hammer an open circuit.
8. **Routing is redirect-only.** The SDK reconnects directly to the leader on `NotLeader` + `LeaderHint`. Never ask the server to forward client requests — server-side forwarding is reserved for saga orchestration (see root rule 11).
9. **UDS endpoints bypass TCP keep-alive.** Endpoints starting with `/` are treated as Unix-socket paths; don't apply TCP-specific settings to them.
