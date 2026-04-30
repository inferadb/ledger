# Wire Protocol

InferaDB Ledger's custom QUIC-based wire protocol — a fixed-size 88-byte frame header plus a length-prefixed payload — used by the SDK and server in place of gRPC over HTTP/2.

## Overview

The wire protocol replaces tonic / gRPC / HTTP/2 with a TigerBeetle-inspired layout: a fixed `#[repr(C)]` `FrameHeader` of exactly 88 bytes followed by a variable-length payload, multiplexed over QUIC bidirectional streams. The motivation is fourfold: removing HTTP/2's stream-level head-of-line blocking, owning the framing layer end-to-end (so the SDK and server share one decode path), making per-frame allocation deterministic for capacity planning, and recovering the perf overhead the gRPC stack imposes between the SDK and the dispatcher. The crate lives at [`crates/wire/`](../../crates/wire/); every claim in this document maps to a specific file there.

## Frame layout

The header is `#[repr(C)]` (not `#[repr(C, packed)]`) and lives in [`crates/wire/src/frame.rs`](../../crates/wire/src/frame.rs). Field order on the wire matches declaration order; multi-byte integers are encoded big-endian by the codec (see [Endianness invariant](#endianness-invariant)).

| Field                    | Type       | Offset | Size | Purpose                                                                                  |
| ------------------------ | ---------- | ------ | ---- | ---------------------------------------------------------------------------------------- |
| `request_id`             | `[u8; 16]` | 0      | 16   | 128-bit correlation ID; server echoes on response. Drives request multiplexing and structured-log correlation. |
| `idempotency_token`      | `[u8; 16]` | 16     | 16   | Phase 12 idempotency token; all-zero bytes mean no idempotency was requested.            |
| `trace_id`               | `[u8; 16]` | 32     | 16   | OpenTelemetry W3C trace ID.                                                              |
| `causality_commit_index` | `u64`      | 48     | 8    | Phase 5 causality token; zero means no constraint.                                       |
| `deadline_unix_nanos`    | `u64`      | 56     | 8    | Wall-clock deadline in nanoseconds since UNIX epoch; zero means no deadline.             |
| `span_id`                | `[u8; 8]`  | 64     | 8    | OpenTelemetry W3C span ID.                                                               |
| `payload_length`         | `u32`      | 72     | 4    | Variable payload size in bytes; rejected by the server above `MAX_FRAME_SIZE`.            |
| `error_code`             | `u32`      | 76     | 4    | Non-zero on error frames; payload then carries a `WireError`.                             |
| `opcode`                 | `u16`      | 80     | 2    | RPC identifier; opcode space is global across all services.                               |
| `version`                | `u8`       | 82     | 1    | Protocol version (v1 = `0x01`); also negotiated via QUIC ALPN.                            |
| `flags`                  | `u8`       | 83     | 1    | Bit 0 `is_response`, bit 1 `is_stream_chunk`, bit 2 `is_stream_end`, bit 3 `compressed`, bits 4-7 reserved. |
| `trace_flags`            | `u8`       | 84     | 1    | OpenTelemetry W3C trace flags.                                                            |
| `_padding`               | `[u8; 3]`  | 85     | 3    | Reserved; must be zero on the wire. Brings the struct to 8-byte alignment.                |

Total: 88 bytes.

`#[repr(C)]` (not `packed`) is used deliberately. Packed structs containing `u64` / `u128` fields require `unsafe` to access the unaligned fields on ARM64, which would violate the workspace-wide `unsafe_code = "deny"` lint. The natural-alignment layout above produces zero implicit padding because each field's offset is already a multiple of its size.

`request_id` is stored as `[u8; 16]` rather than `u128` to keep the struct's alignment at 8 (driven by the `u64` fields). Switching to `u128` would force 16-byte alignment, which makes `#[repr(C)]` insert 8 bytes of tail-padding and grow the header to 96 bytes — breaking the wire contract.

A compile-time assert in `frame.rs` enforces `size_of::<FrameHeader>() == HEADER_SIZE` so any field-add accident is caught at build time.

## Endianness invariant

Multi-byte integer fields (`causality_commit_index`, `deadline_unix_nanos`, `payload_length`, `error_code`, `opcode`) are encoded **big-endian (network byte order)** on the wire. Single-byte fields (`version`, `flags`, `trace_flags`) and opaque byte arrays (`request_id`, `idempotency_token`, `trace_id`, `span_id`) are written verbatim with no byte-order conversion.

The in-memory layout is native-endian. The codec — `Frame::encode` / `Frame::decode` in [`crates/wire/src/codec.rs`](../../crates/wire/src/codec.rs) — handles byte-order conversion via `to_be_bytes()` and `from_be_bytes()`. Any little-endian shortcut at a non-codec call site will silently corrupt traffic between hosts of different endianness; do not introduce one.

## Decode safety contract

`Frame::decode` is the trust boundary between untrusted network bytes and the rest of the server. It enforces these checks **in this order**, and crucially performs every check **before consuming any bytes from the input buffer**:

1. `src.remaining() >= HEADER_SIZE` — returns `CodecError::Truncated` if not. The header bounds check must run first because the version, length, and error fields all live inside the header.
2. `version == CURRENT_PROTOCOL_VERSION` — returns `CodecError::UnsupportedVersion(version)` if not. Reading the rest of the header before the version check would risk parsing a future-version layout against today's offsets.
3. `payload_length <= MAX_FRAME_SIZE` — returns `CodecError::PayloadTooLarge` if not. **This check happens before the second bounds check** so the decoder never computes `HEADER_SIZE + adversarially_large_payload_length`. This is the protocol's primary DoS prevention.
4. `src.remaining() >= HEADER_SIZE + payload_length` — returns `CodecError::Truncated` if not. Only after the payload-size cap above has been applied is this arithmetic safe.
5. If `error_code != 0`, then `payload_length != 0` — returns `CodecError::ErrorFrameMissingPayload(error_code)` if violated. Every error frame must carry a `WireError` payload, so a zero-length error frame is malformed.

Only after all five checks pass does the decoder advance the buffer cursor. Reordering the checks would either leak bytes from the input on a malformed frame or allow oversized payload claims to be allocated before being rejected. Opcode validation is deferred to the protocol layer (see [Opcode allocation](#opcode-allocation)) — frames with unknown opcodes parse cleanly here and surface as `UnknownOpcode` at dispatch.

## Opcode allocation

Opcodes are 16-bit unsigned integers identifying RPCs on the wire. The allocation is global across all services; cross-service collisions are detected at compile time by Phase 0.0.D's `define_protocol!` macro. Per-service ranges are 256 opcodes wide (`SERVICE_RANGE_SIZE = 0x0100`); the reserved-control range is `0x0000`–`0x000F` (`RESERVED_RANGE_END = 0x0010`); the global ceiling is `OPCODE_SPACE_END = 0x1000`.

Constants and the per-service base table live in [`crates/wire/src/opcode.rs`](../../crates/wire/src/opcode.rs); the operator-facing allocation table lives in [`docs/reference/opcodes.md`](../reference/opcodes.md).

Validation is the responsibility of the protocol layer, not the codec. The current `is_valid_opcode` is a syntactic check (non-zero, below `OPCODE_SPACE_END`); Phase 0.0.D will tighten it to "registered opcodes only" using the macro-generated registry. Frames carrying an unrecognised opcode are decoded successfully and then rejected at dispatch with `CodecError::UnknownOpcode`.

## Version negotiation via ALPN

Protocol version 1 is identified by the ALPN protocol string `b"ledger-wire/1"` (`ALPN_LEDGER_WIRE_V1`, defined in [`crates/wire/src/version.rs`](../../crates/wire/src/version.rs)). Negotiation happens at QUIC handshake time, before any application frames are exchanged. Both client and server advertise the ALPN values they support; the QUIC stack picks the highest mutually-supported value or fails the handshake outright when there is no overlap.

The header's `version: u8` field (`CURRENT_PROTOCOL_VERSION = 0x01`) is a defence-in-depth check, not the authoritative version source — the authoritative version is the ALPN value negotiated during the TLS handshake. The header byte catches misconfigured deployments where the same ALPN string is reused across genuinely-incompatible builds, and it makes pcap-level diagnosis straightforward.

Future protocol versions add new ALPN strings (`b"ledger-wire/2"` for v2 and so on); a single endpoint can advertise multiple versions in its ALPN list, which is how rollouts maintain compatibility across a mixed fleet. See [`docs/reference/wire-versioning.md`](../reference/wire-versioning.md) for the full versioning policy.

## Error model

Every error frame has `error_code != 0` in the header and a non-empty payload that decodes (via postcard) to a [`WireError`](../../crates/wire/src/error.rs) struct:

| Field              | Type                       | Purpose                                                                  |
| ------------------ | -------------------------- | ------------------------------------------------------------------------ |
| `code`             | `ErrorCode`                | Same numeric value as the header's `error_code`; redundant for fast-path callers but useful for log-line completeness. |
| `message`          | `String`                   | Human-readable error message; operator-friendly; no internal IDs or stack traces. |
| `retryable`        | `bool`                     | True if the SDK should retry after `retry_after_ms`.                     |
| `retry_after_ms`   | `u64`                      | Suggested backoff in milliseconds; zero when not retryable or no specific delay applies. |
| `context`          | `BTreeMap<String, String>` | Structured key/value pairs (entity name, region slug, etc.).             |
| `suggested_action` | `String`                   | Operator-facing remediation hint; empty when none.                       |

The `context` field is `BTreeMap` (not `HashMap`) for deterministic byte-level encoding — required by content-addressable caching and reproducible log assertions.

`ErrorCode` is a `#[repr(u32)]` enum with explicit, wire-stable discriminants (`NotFound = 1`, `AlreadyExists = 2`, … `Deprecated = 17`). Discriminant `0` is reserved for "no error" in the frame header and intentionally has no enum variant. Discriminants are part of the wire contract; renumbering one is a protocol-breaking change that requires a v2 ALPN bump. See [`docs/reference/errors.md`](../reference/errors.md) for the full operator-facing error reference and the corresponding gRPC-status mappings.

## MAX_FRAME_SIZE

`MAX_FRAME_SIZE` is currently 16 MiB (`16 * 1024 * 1024 = 16,777,216` bytes), measuring the payload only — the 88-byte header is not counted. The constant lives in [`crates/wire/src/frame.rs`](../../crates/wire/src/frame.rs).

The cap exists for two reasons: it is the primary bound on per-frame allocation (so the server cannot be forced to allocate gigabyte buffers from a malformed `payload_length`), and it nudges streaming RPCs (the various `Watch*` endpoints) toward emitting many small frames rather than fewer large ones — improving fairness across multiplexed streams on a single QUIC connection.

The server rejects oversized frames at decode time with `CodecError::PayloadTooLarge`, before any allocation. Clients that need larger logical messages must split them into multiple frames using the streaming flag bits (`is_stream_chunk`, `is_stream_end`).

## RequestContext

For every RPC the dispatcher constructs a `RequestContext` (in [`crates/wire/src/context.rs`](../../crates/wire/src/context.rs)) and hands it to the handler. The context surfaces every value a handler reads today from `tonic::Request<T>` so the gRPC-to-wire migration is a mechanical rewrite rather than a redesign: `request_id`, `idempotency_token`, `trace_id`, `span_id`, `trace_flags`, `causality_commit_index`, `deadline`, `caller_identity`, `peer_addr`, `sdk_version`, `forwarded_for`, `cancellation` token, plus connection-level `conn_metrics`. The detailed handler-API surface is documented alongside the dispatcher in Phase 0.0.B; the wire protocol's only contract here is that the header fields above feed the context exactly once per frame.

## Snapshot streaming

The `RaftService.install_snapshot_stream` RPC is the only multi-message client-streaming RPC in the wire protocol. The `define_protocol!` macro DSL only models unary and server-streaming shapes, so install-snapshot is implemented as a **transport-level sub-protocol** rather than a generated handler. The macro registers `install_snapshot_stream` at opcode `0x0D03` as a unary placeholder; the dispatcher recognises that opcode at runtime and routes the bidi stream into the dedicated receive loop in [`crates/wire-transport/src/snapshot_stream.rs`](../../crates/wire-transport/src/snapshot_stream.rs). The sender side opens an outbound bidi stream via `WireClient::snapshot_stream` (or `SnapshotSender::open` for non-pooled callers) and drives the chunked exchange manually.

### Frame ordering

```text
client → server: header   (flags = 0)
client → server: data @ offset 0
client → server: data @ offset N
server → client: ack @ offset N           (FLAG_IS_RESPONSE | FLAG_IS_STREAM_CHUNK)
client → server: data @ offset N+M
...
client → server: footer (CRC32, byte count)  (FLAG_IS_STREAM_END)
server → client: terminal ack             (FLAG_IS_RESPONSE | FLAG_IS_STREAM_END)
```

All frames carry opcode `OPCODE_INSTALL_SNAPSHOT_STREAM` (`0x0D03`) and the sender's 128-bit `request_id`. Client→server frames carry a postcard-encoded `InstallSnapshotChunk` payload (header / data / footer variants); server→client frames carry a postcard-encoded `InstallSnapshotAck { offset_received: u64 }`.

### Backpressure

The sender enforces an `MAX_INFLIGHT_BYTES` ceiling (default 4 MiB) on unacked bytes — when the most-recent ack offset trails the most-recent sent offset by more than this, `send_chunk` blocks awaiting the next ack frame from the receiver. The receiver writes one ack frame after every committed data chunk; the sender uses the most-recent `offset_received` to advance its window.

### Staging file naming

Receivers write each in-progress snapshot to `staging-{index:020}.snap.staged.partial` under the per-scope staging directory (resolved by the `SnapshotHandler` impl from `header.scope`). On successful footer + CRC validation the file is atomically renamed to `staging-{index:020}.snap.staged`; the install path consumes the renamed file via the standard `SnapshotPersister::list_staged` enumeration.

### Crash recovery

`.partial` files represent in-flight snapshots that crashed mid-stream and cannot be resumed (the sender's CRC state is gone). The `cleanup_partial_snapshots(staging_dir)` helper deletes every `.partial` file under the staging directory and is invoked at startup before any Raft engines come up. The leader's heartbeat replicator re-emits `Action::SendSnapshot` on the next cycle, so dropping these files is safe — transient failures self-heal without explicit retry state in the sub-protocol.

## Cross-references

- [`docs/reference/opcodes.md`](../reference/opcodes.md) — opcode allocation map and reserved-range listing.
- [`docs/reference/wire-versioning.md`](../reference/wire-versioning.md) — versioning policy, extension-vs-bump decision table.
- [`docs/reference/errors.md`](../reference/errors.md) — operator reference for every `ErrorCode` variant.
- [`crates/wire/src/frame.rs`](../../crates/wire/src/frame.rs) — `FrameHeader`, `HEADER_SIZE`, `MAX_FRAME_SIZE`, compile-time size assert.
- [`crates/wire/src/codec.rs`](../../crates/wire/src/codec.rs) — `Frame::encode` / `Frame::decode` and the validation order above.
- [`crates/wire/src/opcode.rs`](../../crates/wire/src/opcode.rs) — reserved opcodes, service bases, range constants.
- [`crates/wire/src/error.rs`](../../crates/wire/src/error.rs) — `ErrorCode` enum, `WireError` payload struct.
- [`crates/wire/src/version.rs`](../../crates/wire/src/version.rs) — `CURRENT_PROTOCOL_VERSION`, `ALPN_LEDGER_WIRE_V1`, `SUPPORTED_ALPN_PROTOCOLS`.
- [`crates/wire/src/context.rs`](../../crates/wire/src/context.rs) — `RequestContext`, `CallerIdentity`, `AuthMethod`, `ConnectionMetrics`.
- [`crates/wire-transport/src/snapshot_stream.rs`](../../crates/wire-transport/src/snapshot_stream.rs) — `SnapshotSender`, `SnapshotReceiver`, `SnapshotHandler`, `cleanup_partial_snapshots`, `MAX_INFLIGHT_BYTES`, sub-protocol implementation.
