# Opcode Allocation

Reference for the InferaDB Ledger wire protocol's opcode allocation. Operators and SDK authors use this table to identify RPCs by their wire identifier; protocol implementers use it to confirm that new RPCs land in the correct service range.

## Overview

Opcodes are 16-bit unsigned integers identifying RPCs on the wire. The allocation is global across all services — there is no service-id field — so cross-service collisions are detected at compile time by Phase 0.0.D's `define_protocol!` macro. Each service receives a contiguous 256-opcode block (`SERVICE_RANGE_SIZE = 0x0100`); the reserved-control range occupies `0x0000`–`0x000F`; the global ceiling is `OPCODE_SPACE_END = 0x1000`. All constants live in [`crates/wire/src/opcode.rs`](../../crates/wire/src/opcode.rs).

## Allocation map

| Range               | Purpose                                                                         | Allocated      |
| ------------------- | ------------------------------------------------------------------------------- | -------------- |
| `0x0000`            | Reserved (zero is the null / unset opcode and is rejected by `is_valid_opcode`) | n/a            |
| `0x0001`–`0x000F`   | Reserved control opcodes (handshake, ping, auth, reflection)                    | yes (Phase 0.0.A.3) |
| `0x0010`–`0x00FF`   | Read service (shares the first 256-opcode block with the reserved range)        | base only      |
| `0x0100`–`0x01FF`   | Write service                                                                   | base only      |
| `0x0200`–`0x02FF`   | Organization service                                                            | base only      |
| `0x0300`–`0x03FF`   | Vault service                                                                   | base only      |
| `0x0400`–`0x04FF`   | Schema service                                                                  | base only      |
| `0x0500`–`0x05FF`   | User service                                                                    | base only      |
| `0x0600`–`0x06FF`   | App service                                                                     | base only      |
| `0x0700`–`0x07FF`   | Invitation service                                                              | base only      |
| `0x0800`–`0x08FF`   | Token service                                                                   | base only      |
| `0x0900`–`0x09FF`   | Events service                                                                  | base only      |
| `0x0A00`–`0x0AFF`   | Admin service                                                                   | base only      |
| `0x0B00`–`0x0BFF`   | Health service                                                                  | base only      |
| `0x0C00`–`0x0CFF`   | System Discovery service                                                        | base only      |
| `0x0D00`–`0x0DFF`   | Raft Consensus service                                                          | base only      |
| `0x0E00`–`0x0EFF`   | Maintenance service                                                             | base only      |
| `0x0F00`–`0x0FFF`   | Lease service                                                                   | base only      |
| `0x1000`–`0xFFFF`   | Out of range; rejected by `is_valid_opcode`                                     | n/a            |

`READ_SERVICE_BASE` (`0x0010`) is the only service base that is not a multiple of `SERVICE_RANGE_SIZE`: Read shares its first block with the reserved range. All other service bases align cleanly to 256-byte boundaries, which the unit test `service_bases_in_their_range` enforces.

## Reserved opcodes

The reserved range (`0x0001`–`0x000F`) is for protocol-level operations. Service RPCs MUST NOT use these opcodes. Five slots are allocated today:

| Constant                   | Value     | Purpose                                                                                 |
| -------------------------- | --------- | --------------------------------------------------------------------------------------- |
| `OPCODE_HANDSHAKE_REQUEST` | `0x0001`  | Connection-setup handshake. Phase 0.0.A.5 (ALPN-based version negotiation) likely supersedes the in-band handshake; the slot is reserved pending the ALPN design pass. |
| `OPCODE_PING`              | `0x0002`  | Connection-keepalive ping.                                                              |
| `OPCODE_AUTH_ESTABLISH`    | `0x0003`  | First frame on a new connection; carries the JWT. The server validates and caches caller identity per-connection. |
| `OPCODE_AUTH_REFRESH`      | `0x0004`  | Client-initiated JWT refresh mid-connection (when token expiry approaches). The old token remains valid until the response is received. |
| `OPCODE_REFLECT`           | `0x0005`  | Protocol-level reflection. Returns the opcode registry as `Vec<(opcode, name, request_type, response_type)>` for `wire-probe` and other introspection tools. Implementation in Phase 0.0.D. |

Slots `0x0006`–`0x000F` are intentionally unallocated and reserved for future protocol-level operations. They pass the syntactic `is_valid_opcode` check but currently have no handler.

## Per-service tables

<!-- Populated by Phase 0.0.D's define_protocol! macro output. See plan task 0.0.D.4. -->

The `define_protocol!` macro emits the per-service opcode → RPC-name → request-type → response-type table at compile time, using the per-service base constants in [`crates/wire/src/opcode.rs`](../../crates/wire/src/opcode.rs) as the anchor for each block. A future revision of this document will paste the generated tables here once the macro lands; until then, refer to the proto definitions in [`proto/ledger/v1/`](../../proto/ledger/v1/) for the authoritative RPC list per service.

## Opcode validation

`is_valid_opcode(opcode: u16) -> bool` in [`crates/wire/src/opcode.rs`](../../crates/wire/src/opcode.rs) is the syntactic check today. It accepts:

- Any reserved-range value `0x0001`–`0x000F`.
- Any value in any allocated service range, `0x0010`–`0x0FFF`.

It rejects `0x0000` (the null / unset opcode) and any value at or above `OPCODE_SPACE_END = 0x1000`. The function does NOT verify that the opcode corresponds to a registered RPC — that check requires the macro-generated registry from Phase 0.0.D and will replace this stub. Until then, frames with syntactically-valid but unregistered opcodes parse cleanly through `Frame::decode` and surface as `CodecError::UnknownOpcode` at the dispatch layer.

## Cross-references

- [`docs/architecture/wire-protocol.md`](../architecture/wire-protocol.md) — frame layout, decode safety contract, and the role of opcodes within a frame.
- [`docs/reference/wire-versioning.md`](wire-versioning.md) — when adding a new opcode is an extension and when it requires a v2 bump.
- [`crates/wire/src/opcode.rs`](../../crates/wire/src/opcode.rs) — canonical source for every constant referenced above.
