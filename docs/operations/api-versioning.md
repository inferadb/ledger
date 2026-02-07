# API Version Negotiation

InferaDB Ledger uses header-based API version negotiation to detect
incompatible clients before they encounter undefined behavior.

## Headers

| Header                 | Direction | Description                                   |
| ---------------------- | --------- | --------------------------------------------- |
| `x-ledger-api-version` | Request   | API version the client expects (integer)      |
| `x-ledger-api-version` | Response  | API version the server supports (integer)     |
| `x-sdk-version`        | Request   | SDK build identifier (e.g., `rust-sdk/0.1.0`) |

## Version Compatibility Matrix

| Client Version | Server Range | Result                                 |
| -------------- | ------------ | -------------------------------------- |
| 1              | 1–1          | OK                                     |
| 0              | 1–1          | `FAILED_PRECONDITION` — client too old |
| 2              | 1–1          | `FAILED_PRECONDITION` — client too new |
| (missing)      | 1–1          | OK (defaults to version 1)             |

## Behavior

- The SDK injects `x-ledger-api-version: 1` on every request via
  `TraceContextInterceptor`.
- The server validates the header on client-facing RPCs (Read, Write, Admin).
- Health, Discovery, and Raft RPCs are exempt — they serve infrastructure
  probes and inter-node consensus traffic that must work regardless of
  API version.
- If the version is outside `[MIN_SUPPORTED, CURRENT]`, the server
  returns `FAILED_PRECONDITION` with an actionable error message.
- Missing headers default to version 1 for backwards compatibility
  during rollout.

## Proto Deprecation Convention

Fields planned for removal are annotated with `[deprecated = true]` in
the proto file, along with a comment specifying the removal version:

```protobuf
string old_field = 5 [deprecated = true]; // Removed in API v2
```

## Upgrading

When bumping the API version:

1. Update `CURRENT_API_VERSION` in `crates/raft/src/api_version.rs`
2. Update `API_VERSION_VALUE` in `crates/sdk/src/tracing.rs`
3. Update the `api_version_header_value` static string in `api_version.rs`
4. Update this document's compatibility matrix
5. If dropping old versions, update `MIN_SUPPORTED_API_VERSION`
