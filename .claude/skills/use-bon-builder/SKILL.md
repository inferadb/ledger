---
name: use-bon-builder
description: Use when adding or modifying a struct with configurable fields, a fallible constructor that validates at build time, or a config type loaded via serde. Covers the `#[derive(bon::Builder)]` vs `#[bon::bon] impl` split, pairing `#[builder(default)]` with `#[serde(default)]`, and the `Option<T>` setter trap that causes compile errors.
---

# use-bon-builder

InferaDB uses [`bon`](https://docs.rs/bon) for struct builders. The pattern is consistent across crates — `ClientConfig`, `TlsConfig`, `StorageConfig`, `RaftConfig`, `BatchConfig`, `CircuitBreakerConfig`, and `BTreeCompactionConfig` all follow it.

Prefer compile-time required fields over runtime `validate()` checks wherever possible. If a field must be set, make bon require it.

## Two patterns

### Simple (no validation)

```rust
#[derive(bon::Builder)]
pub struct Config {
    #[builder(default = 100)]
    max_connections: u32,

    #[builder(into)]
    name: String,

    // Option<T>: setter accepts T, NOT Option<T>.
    // Do not add #[builder(default)] — bon auto-infers None.
    description: Option<String>,
}

// Call site:
let cfg = Config::builder()
    .name("primary")          // not .name(String::from("primary")) — `into` handles it
    .max_connections(50)       // optional override of default
    .description("primary cfg") // not .description(Some("primary cfg"))
    .build();
```

### Fallible constructor (validates at build time)

```rust
#[bon::bon]
impl TlsConfig {
    #[builder]
    pub fn new(
        #[builder(into)] ca_cert: Option<String>,
        use_native_roots: bool,
    ) -> Result<Self, ConfigError> {
        if ca_cert.is_none() && !use_native_roots {
            return Err(ConfigError::MissingTrustAnchor);
        }
        Ok(Self { ca_cert, use_native_roots })
    }
}

// Call site:
let tls = TlsConfig::builder()
    .use_native_roots(true)
    .build()?;
```

Tests of fallible builders use `::builder().build()` — **never** `::new().build()`. bon generates a `builder()` associated fn on the impl block, not a separate `new()` constructor.

## Serde pairing for config types

Config structs are loaded from env/JSON and built programmatically. Both paths must agree on defaults:

```rust
#[derive(serde::Deserialize, bon::Builder)]
pub struct HealthCheckConfig {
    #[builder(default = 2)]
    #[serde(default = "HealthCheckConfig::default_dependency_check_timeout_secs")]
    pub dependency_check_timeout_secs: u64,
}

impl HealthCheckConfig {
    fn default_dependency_check_timeout_secs() -> u64 { 2 }
}
```

Every `#[builder(default = …)]` has a matching `#[serde(default = …)]`. Otherwise deserialization rejects configs that the builder accepts.

## Gotchas

| Symptom | Cause | Fix |
| --- | --- | --- |
| `expected T, found Option<T>` at call site | Wrapped `Option<T>` setter in `Some(..)` | Pass the inner value: `.field(x)`, not `.field(Some(x))` |
| `#[builder(default)] on Option<T> is redundant` compiler error | `#[builder(default)]` attribute on an `Option<T>` field | Remove the attribute — bon auto-infers `None` |
| Fallible builder test fails with `::new()` not found | Using `::new().build()` pattern | `#[bon::bon] impl` generates `::builder()` — call `Config::builder().build()` |
| `#[builder(into)]` not accepting `&str` | Field is not `String` | Only use `into` for `String` fields (or types with `From<&str>`) |
| Config deserializes but builder rejects the same values | `#[serde(default)]` missing where `#[builder(default)]` is present | Add a matching serde default function |

## Required over optional

If a field is load-bearing, do not give it a default. Force the caller to specify it at build time — the type system catches the omission. Runtime `validate()` should be the last resort.

## References

- Workspace precedents: `crates/sdk/src/client.rs` (`ClientConfig`), `crates/sdk/src/tls.rs` (`TlsConfig`), `crates/types/src/config/` (fallible builders for storage/raft/batch configs).
- `CLAUDE.md` (types crate) — config dual pattern.
