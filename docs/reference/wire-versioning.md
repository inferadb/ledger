# Wire Protocol Versioning

Policy reference for evolving the InferaDB Ledger wire protocol. Distinguishes "extension" changes — forward- and backward-compatible additions that ship without bumping the protocol version — from "breaking" changes that require a new ALPN string and a parallel-deployment compatibility window.

## Overview

The wire protocol carries an explicit `version: u8` field in every frame header (`CURRENT_PROTOCOL_VERSION` in [`crates/wire/src/version.rs`](../../crates/wire/src/version.rs)) plus an ALPN-negotiated identifier (`ALPN_LEDGER_WIRE_V1 = b"ledger-wire/1"`). Today both are `0x01` / `b"ledger-wire/1"`. The ALPN value is the authoritative version source — it is negotiated during the QUIC TLS handshake before any application bytes flow — and the header byte is a defence-in-depth check that catches misconfigured deployments.

This document defines the rules for changing either, and the criteria SDK and server authors use to decide whether a given change ships as an extension or as v2.

## Extension vs. version bump

The decision is binary: either every existing peer can still parse every frame after the change (extension) or it cannot (v2). The table below maps the changes that have come up so far.

| Change                                                | Extension or v2?            | Rationale                                                                                          |
| ----------------------------------------------------- | --------------------------- | -------------------------------------------------------------------------------------------------- |
| Add a new opcode                                      | Extension                   | Reserved opcode space is wide; clients reject unknown opcodes locally; old servers return `UnknownOpcode` at dispatch. |
| Add a new `ErrorCode` variant                         | Extension                   | Clients tolerate unknown codes via `UnknownErrorCode` (forward-compatibility built into `try_from_u32`). |
| Add a field to a postcard-encoded payload struct      | Extension                   | postcard tolerates trailing bytes and unknown fields when the schema is purely additive.            |
| Remove a field from a postcard-encoded payload struct | v2                          | Old clients fail to decode; no graceful fallback path.                                              |
| Repurpose a header field                              | v2                          | Silent semantic shift; cross-version peers cannot reconcile.                                        |
| Add a header field                                    | v2                          | Header size is fixed at 88 bytes by a compile-time assert; growing it changes the parse contract.   |
| Change endianness or alignment                        | v2                          | Wire-corrupting; affects every frame.                                                               |
| Change `MAX_FRAME_SIZE`                               | v2 if reduced; extension if increased | Reduction breaks legitimate clients already sending large frames; increase is opt-in for clients that want larger limits. |
| Add a new ALPN string                                 | v2                          | A new ALPN string is the definition of a new protocol version.                                      |
| Change the encoding of a payload (e.g. postcard → bincode) | v2                     | Wire-corrupting at the payload level even when the header is unchanged.                            |

The first row covers the common case for SDK and server feature work: adding RPCs is always an extension and never blocks on a version bump.

## Compatibility window

When v2 lands, servers advertise both the v1 and v2 ALPN strings simultaneously for **at least one full release cycle**. This guarantees that v1-pinned clients keep working while operators roll v2 out. The SDK's `ClientConfig` exposes `min_protocol_version` / `max_protocol_version` knobs so consumers can pin a specific version explicitly; the default is "latest stable", which lets the QUIC stack negotiate the highest mutual ALPN.

A version is removed only after every supported SDK release has migrated and the server-side telemetry shows zero v1 connections for the target retention period. The exact window per release is documented at the time of the v2 bump, not pre-committed here.

## Versioning of nested types

Payload encodings — `WireError`, the per-RPC postcard structs, `RequestContext`-derived metadata — version independently of the wire protocol itself. Their evolution follows the same extension-vs-break rules above, applied per-type:

- `ErrorCode` discriminants are wire-stable; renumbering is a v2-class change for the wire protocol because the discriminant value is mirrored into the frame header's `error_code` field.
- `WireError` is additive-friendly: new optional fields can land as extensions, but field removals are v2-class.
- Per-RPC payload structs (the ones generated by Phase 0.0.D's `define_protocol!`) version per-RPC; the `Deprecated` `ErrorCode` lets the server reject specific RPCs that have aged out without forcing a wire-protocol bump.

## Compile-time invariants

Two compile-time asserts catch unintentional version drift in PRs:

- [`crates/wire/src/frame.rs`](../../crates/wire/src/frame.rs) asserts `std::mem::size_of::<FrameHeader>() == HEADER_SIZE` (88 bytes). Adding or removing a header field — accidentally or otherwise — fails the build.
- [`crates/wire/src/version.rs`](../../crates/wire/src/version.rs) asserts `CURRENT_PROTOCOL_VERSION == 0x01`. Bumping the constant fails the build, which forces the author to update the assert and the ALPN list together rather than bumping the version byte in isolation.

When v2 lands, both asserts are updated in lockstep with the new ALPN entry in `SUPPORTED_ALPN_PROTOCOLS`, the new variant in any version-aware enums, and the row added to the compatibility window section above.

## Cross-references

- [`docs/architecture/wire-protocol.md`](../architecture/wire-protocol.md) — frame layout, ALPN negotiation, decode safety contract.
- [`docs/reference/opcodes.md`](opcodes.md) — opcode allocation map; adding an opcode is an extension.
- [`crates/wire/src/version.rs`](../../crates/wire/src/version.rs) — `CURRENT_PROTOCOL_VERSION`, `ALPN_LEDGER_WIRE_V1`, `SUPPORTED_ALPN_PROTOCOLS`, `supported_alpn_protocols_owned`.
- [`crates/wire/src/frame.rs`](../../crates/wire/src/frame.rs) — `FrameHeader`, `HEADER_SIZE`, header-size compile-time assert.
- [`crates/wire/src/error.rs`](../../crates/wire/src/error.rs) — `ErrorCode` enum and `try_from_u32` forward-compatibility.
