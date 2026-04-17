# sdk

Consumer-facing Rust client. **The one crate that does not use snafu** — public SDK types use `thiserror` (`SdkError`, `ResolverError`) for cleaner consumer error UX.

## Key types

| Type | Purpose |
| --- | --- |
| `LedgerClient` | Public API: authenticate, read/write, admin calls |
| `ClientConfig` | bon fallible builder — precedent for the pattern across the workspace |
| `RegionLeaderCache` | Per-region leader endpoint cache with soft/hard TTLs, populated by `ResolveRegionLeader`, `NotLeader` hints, and `WatchLeader` push updates. All updates term-gated. |
| `ConnectionPool` | Per-endpoint circuit breaker + connection metrics |
| `SdkMetrics` trait | `NoopSdkMetrics` (default) or `MetricsSdkMetrics` (metrics crate facade). `Arc<dyn SdkMetrics>` — dynamic dispatch, not generics (avoids type-param infection). |

## Conventions

- **`thiserror` only in this crate.** Server crates use snafu; do not import snafu types into SDK error types.
- **`ClientConfig` uses `#[bon::bon] impl ... { #[builder] pub fn new(...) -> Result<Self, _> }`.** Same precedent for `TlsConfig`, `CircuitBreakerConfig`. Tests call `::builder().build()`, not `::new().build()`.
- **`Option<T>` setters on bon builders accept `T`, not `Option<T>`.** `.field(Some(x))` is a type error — use `.field(x)`. bon auto-infers the wrap.
- **`#[builder(default)]` on `Option<T>` is redundant** — bon auto-infers `None` for `Option`. It also causes a compiler error, not a warning.
- **`ServerErrorDetails` is boxed** (`Option<Box<ServerErrorDetails>>`) to keep `Result<T, SdkError>` small — clippy `result_large_err` otherwise.
- **`with_retry_cancellable` signature requires `method: &str`** — 16 call sites in `client.rs` and 7 in `retry.rs` tests pass a method name string.
- **Circuit-breaker metrics** are recorded at `ConnectionPool` level (has both circuit breaker + metrics handles).

## Leader routing

- Routing is redirect-only: when a server returns `NotLeader` + `LeaderHint`, the SDK reconnects directly to the leader. Server-side forwarding is **only** for saga orchestration.
- Leader cache updates are term-gated — ignore hints for older terms.

## Related tooling

- Skill: `/use-bon-builder` (builder precedent lives here)
