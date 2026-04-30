//! Wire frame layout for the InferaDB Ledger custom QUIC-based protocol.
//!
//! Replaces gRPC/HTTP/2 with a fixed-size 88-byte header plus length-prefixed
//! payload. Inspired by TigerBeetle's wire protocol.

/// Fixed-size wire frame header.
///
/// All multi-byte integers are native-endian in memory; codec tasks handle
/// on-wire byte-order conversion (big-endian / network byte order on the wire).
///
/// # Layout
///
/// `#[repr(C)]` ensures field order and natural alignment match the wire
/// definition. The field sizes in declaration order are: 16, 16, 16, 8, 8, 8,
/// 4, 4, 2, 1, 1, 1, 3 bytes — each field is naturally aligned at its size
/// boundary, so no implicit padding is inserted and the total is exactly 88
/// bytes. `#[repr(C, packed)]` is intentionally avoided: packed structs with
/// `u64`/`u128` fields require `unsafe` for any field reference on ARM64 (the
/// fields would be unaligned), which violates the workspace-wide
/// `unsafe_code = "deny"` rule.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    /// 128-bit request correlation ID stored as raw bytes. Server echoes on
    /// response. Used for concurrent-request correlation on a connection AND
    /// for structured-log correlation across the SDK retry loop, audit trail,
    /// and operator logs.
    ///
    /// Stored as `[u8; 16]` rather than `u128` so that the struct's alignment
    /// remains 8 (driven by the `u64` fields) and no trailing padding is
    /// inserted by `#[repr(C)]`. The `u128` alternative has alignment 16,
    /// which causes the compiler to add 8 bytes of tail-padding and yields a
    /// 96-byte struct instead of the required 88 bytes.
    pub request_id: [u8; 16],
    /// Phase 12 idempotency contract; zero = no idempotency.
    pub idempotency_token: [u8; 16],
    /// OpenTelemetry W3C trace ID.
    pub trace_id: [u8; 16],
    /// Phase 5 causality token; zero = no constraint.
    pub causality_commit_index: u64,
    /// Wall-clock deadline in nanoseconds since UNIX epoch; zero = no deadline.
    pub deadline_unix_nanos: u64,
    /// OpenTelemetry W3C span ID.
    pub span_id: [u8; 8],
    /// Variable payload size; server rejects > MAX_FRAME_SIZE before allocation.
    pub payload_length: u32,
    /// Non-zero = error frame; payload is always WireError.
    pub error_code: u32,
    /// RPC identifier; opcode registry is global across all services.
    pub opcode: u16,
    /// Protocol version (v1 = 0x01). Also negotiated via QUIC ALPN.
    pub version: u8,
    /// Bit 0: is_response. Bit 1: is_stream_chunk. Bit 2: is_stream_end.
    /// Bit 3: compressed. Bits 4-7 reserved.
    pub flags: u8,
    /// OpenTelemetry W3C trace flags.
    pub trace_flags: u8,
    /// Reserved; must be zero. Explicit padding to 8-byte alignment.
    _padding: [u8; 3],
}

impl Default for FrameHeader {
    /// Zero-fills every field.
    ///
    /// Useful with struct-update syntax for tests and constructors that only
    /// set a few fields:
    ///
    /// ```text
    /// // Within crates/wire (where _padding is visible):
    /// let header = FrameHeader { opcode: 7, version: 0x01, ..Default::default() };
    /// ```
    ///
    /// Callers MUST NOT set `_padding` to non-zero values — wire compatibility
    /// relies on it being zero.
    fn default() -> Self {
        Self {
            request_id: [0u8; 16],
            idempotency_token: [0u8; 16],
            trace_id: [0u8; 16],
            causality_commit_index: 0,
            deadline_unix_nanos: 0,
            span_id: [0u8; 8],
            payload_length: 0,
            error_code: 0,
            opcode: 0,
            version: 0,
            flags: 0,
            trace_flags: 0,
            _padding: [0u8; 3],
        }
    }
}

/// Header size on the wire (and in memory under `#[repr(C)]`).
pub const HEADER_SIZE: usize = 88;

/// Maximum payload size (16 MiB). Server rejects larger frames BEFORE
/// allocation as DoS prevention.
pub const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;

/// Compile-time invariant: header size must match the wire layout.
const _: () = assert!(std::mem::size_of::<FrameHeader>() == HEADER_SIZE);

/// A complete wire frame: fixed-size header + variable-size payload.
#[derive(Debug, Clone, Default)]
pub struct Frame {
    /// The fixed-size wire header.
    pub header: FrameHeader,
    /// The variable-length payload bytes.
    pub payload: bytes::Bytes,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::version::CURRENT_PROTOCOL_VERSION;

    #[test]
    fn frame_header_field_access() {
        let request_id = [
            0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
            0x12, 0x34,
        ];
        let header = FrameHeader {
            request_id,
            idempotency_token: [1u8; 16],
            trace_id: [2u8; 16],
            causality_commit_index: 42,
            deadline_unix_nanos: 1_000_000_000,
            span_id: [3u8; 8],
            payload_length: 256,
            opcode: 7,
            version: CURRENT_PROTOCOL_VERSION,
            flags: 0b0000_0001, // is_response bit set
            trace_flags: 0x01,
            ..Default::default()
        };

        assert_eq!(header.request_id, request_id);
        assert_eq!(header.idempotency_token, [1u8; 16]);
        assert_eq!(header.trace_id, [2u8; 16]);
        assert_eq!(header.causality_commit_index, 42);
        assert_eq!(header.deadline_unix_nanos, 1_000_000_000);
        assert_eq!(header.span_id, [3u8; 8]);
        assert_eq!(header.payload_length, 256);
        assert_eq!(header.error_code, 0);
        assert_eq!(header.opcode, 7);
        assert_eq!(header.version, CURRENT_PROTOCOL_VERSION);
        assert_eq!(header.flags & 0x01, 0x01); // is_response bit set
        assert_eq!(header.trace_flags, 0x01);
    }

    #[test]
    fn frame_struct_construction() {
        let mut req_id = [0u8; 16];
        req_id[15] = 1;
        let header = FrameHeader {
            request_id: req_id,
            payload_length: 4,
            opcode: 1,
            version: CURRENT_PROTOCOL_VERSION,
            ..Default::default()
        };
        let frame = Frame { header, payload: bytes::Bytes::from_static(b"test") };

        assert_eq!(frame.header.request_id, req_id);
        assert_eq!(frame.header.payload_length, 4);
        assert_eq!(frame.payload.len(), 4);
        assert_eq!(&frame.payload[..], b"test");
    }
}
