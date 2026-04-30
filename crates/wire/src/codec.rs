//! Frame codec: encode and decode [`Frame`] values over a byte stream.
//!
//! All multi-byte integer fields use **big-endian** (network byte order) on the
//! wire. Opaque byte arrays (`request_id`, `idempotency_token`, `trace_id`,
//! `span_id`) are copied verbatim with no endianness conversion.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

use crate::{
    frame::{Frame, FrameHeader, HEADER_SIZE, MAX_FRAME_SIZE},
    version::CURRENT_PROTOCOL_VERSION,
};

/// Peek the `payload_length` field from a header-sized byte slice.
///
/// The wire-transport crate uses this when reading frames off a QUIC stream:
/// it reads exactly [`HEADER_SIZE`] bytes first, calls `peek_payload_length`
/// to learn how many payload bytes follow, then reads the remaining bytes
/// before invoking [`Frame::decode`] on the full buffer.
///
/// Performs the same DoS-guard validation as [`Frame::decode`]:
///
/// 1. Buffer is at least [`HEADER_SIZE`] bytes long.
/// 2. Version byte at offset 82 equals [`CURRENT_PROTOCOL_VERSION`].
/// 3. Payload length at offsets 72..76 (u32 BE) is `<= MAX_FRAME_SIZE`.
///
/// On success returns the declared payload length so the caller can size the
/// next read exactly. The caller still passes the assembled buffer back
/// through [`Frame::decode`] to perform the full validation pass — this
/// helper is purely a length-peek.
///
/// # Errors
///
/// * [`CodecError::Truncated`] when `header` is shorter than [`HEADER_SIZE`].
/// * [`CodecError::UnsupportedVersion`] when the version byte is not [`CURRENT_PROTOCOL_VERSION`].
/// * [`CodecError::PayloadTooLarge`] when the declared length exceeds [`MAX_FRAME_SIZE`].
pub fn peek_payload_length(header: &[u8]) -> Result<u32, CodecError> {
    if header.len() < HEADER_SIZE {
        return Err(CodecError::Truncated { expected: HEADER_SIZE, actual: header.len() });
    }
    let version = header[82];
    if version != CURRENT_PROTOCOL_VERSION {
        return Err(CodecError::UnsupportedVersion(version));
    }
    let payload_length = u32::from_be_bytes([header[72], header[73], header[74], header[75]]);
    if payload_length > MAX_FRAME_SIZE {
        return Err(CodecError::PayloadTooLarge { payload_length, max: MAX_FRAME_SIZE });
    }
    Ok(payload_length)
}

/// Errors that can occur during frame encoding or decoding.
#[derive(Debug, Error)]
pub enum CodecError {
    /// The wire protocol version byte is not [`CURRENT_PROTOCOL_VERSION`].
    #[error("unsupported wire protocol version {0:#x}")]
    UnsupportedVersion(u8),

    /// The `payload_length` field exceeds [`MAX_FRAME_SIZE`].
    #[error("payload exceeds MAX_FRAME_SIZE: {payload_length} > {max}")]
    PayloadTooLarge {
        /// The `payload_length` value declared in the frame header.
        payload_length: u32,
        /// The [`MAX_FRAME_SIZE`] limit that was exceeded.
        max: u32,
    },

    /// Not enough bytes are available to complete the frame.
    #[error("frame truncated: expected {expected} bytes, only {actual} available")]
    Truncated {
        /// Number of bytes required to complete the frame.
        expected: usize,
        /// Number of bytes actually available.
        actual: usize,
    },

    /// An error frame (`error_code != 0`) carries an empty payload, which is
    /// invalid — error frames must always contain a `WireError` payload.
    #[error("error frame with empty payload (error_code={0:#x}, payload_length=0)")]
    ErrorFrameMissingPayload(u32),

    /// The opcode value is not recognised by this decoder.
    #[error("unknown opcode {0:#06x}")]
    UnknownOpcode(u16),

    /// A field-level invariant is violated.
    #[error("invalid frame: {0}")]
    Invalid(&'static str),
}

impl Frame {
    /// Encode this frame into `dst`, appending header then payload bytes.
    ///
    /// Returns `Err(CodecError::Invalid)` if `header.payload_length` does not
    /// equal `payload.len()` — the caller must keep these in sync.
    ///
    /// All multi-byte integer fields are written in **big-endian** order.
    /// The `_padding` bytes are always written as `0x00` regardless of what
    /// the in-memory struct holds.
    pub fn encode(&self, dst: &mut BytesMut) -> Result<(), CodecError> {
        if self.header.payload_length as usize != self.payload.len() {
            return Err(CodecError::Invalid("header.payload_length doesn't match payload bytes"));
        }

        dst.reserve(HEADER_SIZE + self.payload.len());

        // Offsets 0..16 — request_id (opaque bytes, no endianness conversion)
        dst.put_slice(&self.header.request_id);

        // Offsets 16..32 — idempotency_token (opaque bytes)
        dst.put_slice(&self.header.idempotency_token);

        // Offsets 32..48 — trace_id (opaque bytes)
        dst.put_slice(&self.header.trace_id);

        // Offsets 48..56 — causality_commit_index (u64 BE)
        dst.put_u64(self.header.causality_commit_index);

        // Offsets 56..64 — deadline_unix_nanos (u64 BE)
        dst.put_u64(self.header.deadline_unix_nanos);

        // Offsets 64..72 — span_id (opaque bytes)
        dst.put_slice(&self.header.span_id);

        // Offsets 72..76 — payload_length (u32 BE)
        dst.put_u32(self.header.payload_length);

        // Offsets 76..80 — error_code (u32 BE)
        dst.put_u32(self.header.error_code);

        // Offsets 80..82 — opcode (u16 BE)
        dst.put_u16(self.header.opcode);

        // Offset 82 — version (1 byte)
        dst.put_u8(self.header.version);

        // Offset 83 — flags (1 byte)
        dst.put_u8(self.header.flags);

        // Offset 84 — trace_flags (1 byte)
        dst.put_u8(self.header.trace_flags);

        // Offsets 85..88 — _padding (3 bytes, always zero on the wire)
        dst.put_slice(&[0u8; 3]);

        // Payload bytes
        dst.put_slice(&self.payload);

        Ok(())
    }

    /// Decode one frame from `src`, consuming exactly `HEADER_SIZE +
    /// payload_length` bytes on success.
    ///
    /// Validation is performed in order, with every invariant checked **before**
    /// any bytes are consumed from `src`:
    ///
    /// 1. Enough bytes for the header (`HEADER_SIZE`).
    /// 2. `version == CURRENT_PROTOCOL_VERSION`.
    /// 3. `payload_length <= MAX_FRAME_SIZE`.
    /// 4. Enough bytes for header + payload.
    /// 5. If `error_code != 0` then `payload_length != 0`.
    ///
    /// Opcode validation is deferred to the protocol layer (Task 0.0.D).
    pub fn decode(src: &mut Bytes) -> Result<Self, CodecError> {
        // ── Step 1: header bounds check ──────────────────────────────────────
        if src.remaining() < HEADER_SIZE {
            return Err(CodecError::Truncated { expected: HEADER_SIZE, actual: src.remaining() });
        }

        // ── Step 2–7: peek the header without consuming ───────────────────────
        // Copy the first HEADER_SIZE bytes to a stack buffer for validation.
        // `src.chunk()` returns a contiguous slice only up to an internal
        // segment boundary, so we use a stack copy to guarantee we see all 88
        // bytes at once.
        let mut header_buf = [0u8; HEADER_SIZE];
        {
            // `src` has at least HEADER_SIZE bytes (checked above); create a
            // temporary slice view to copy without advancing `src`.
            let chunk = src.chunk();
            if chunk.len() >= HEADER_SIZE {
                header_buf.copy_from_slice(&chunk[..HEADER_SIZE]);
            } else {
                // Bytes may be non-contiguous (e.g. chained). Clone the Bytes
                // handle and advance the clone to read all 88 bytes.
                let mut tmp = src.clone();
                tmp.copy_to_slice(&mut header_buf);
            }
        }

        // Byte offsets within header_buf:
        //   0..16   request_id
        //   16..32  idempotency_token
        //   32..48  trace_id
        //   48..56  causality_commit_index (u64 BE)
        //   56..64  deadline_unix_nanos (u64 BE)
        //   64..72  span_id
        //   72..76  payload_length (u32 BE)
        //   76..80  error_code (u32 BE)
        //   80..82  opcode (u16 BE)
        //   82      version
        //   83      flags
        //   84      trace_flags
        //   85..88  _padding

        // ── Step 3: version check (offset 82) ────────────────────────────────
        let version = header_buf[82];
        if version != CURRENT_PROTOCOL_VERSION {
            return Err(CodecError::UnsupportedVersion(version));
        }

        // ── Step 4: payload_length + DoS guard (offset 72, u32 BE) ───────────
        // MUST happen before the second bounds check so we never compute
        // HEADER_SIZE + an adversarially large payload_length.
        // Indexing into a [u8; HEADER_SIZE] with fixed offsets is infallible.
        let payload_length =
            u32::from_be_bytes([header_buf[72], header_buf[73], header_buf[74], header_buf[75]]);
        if payload_length > MAX_FRAME_SIZE {
            return Err(CodecError::PayloadTooLarge { payload_length, max: MAX_FRAME_SIZE });
        }

        // ── Step 5: full-frame bounds check ──────────────────────────────────
        let total = HEADER_SIZE + payload_length as usize;
        if src.remaining() < total {
            return Err(CodecError::Truncated { expected: total, actual: src.remaining() });
        }

        // ── Step 6: error_code + empty-payload guard (offset 76, u32 BE) ─────
        let error_code =
            u32::from_be_bytes([header_buf[76], header_buf[77], header_buf[78], header_buf[79]]);
        if error_code != 0 && payload_length == 0 {
            return Err(CodecError::ErrorFrameMissingPayload(error_code));
        }

        // ── Step 7: opcode (offset 80, u16 BE) — accepted as-is ─────────────
        // Validation happens in the protocol layer when the opcode registry is
        // available (Task 0.0.D / define_protocol! macro).
        let opcode = u16::from_be_bytes([header_buf[80], header_buf[81]]);

        // ── Step 8: consume bytes ─────────────────────────────────────────────
        // All checks passed; now advance `src` past the header then the payload.
        src.advance(HEADER_SIZE);
        let payload = src.copy_to_bytes(payload_length as usize);

        // ── Step 9: decode remaining header fields ────────────────────────────
        let mut request_id = [0u8; 16];
        request_id.copy_from_slice(&header_buf[0..16]);

        let mut idempotency_token = [0u8; 16];
        idempotency_token.copy_from_slice(&header_buf[16..32]);

        let mut trace_id = [0u8; 16];
        trace_id.copy_from_slice(&header_buf[32..48]);

        let causality_commit_index = u64::from_be_bytes([
            header_buf[48],
            header_buf[49],
            header_buf[50],
            header_buf[51],
            header_buf[52],
            header_buf[53],
            header_buf[54],
            header_buf[55],
        ]);
        let deadline_unix_nanos = u64::from_be_bytes([
            header_buf[56],
            header_buf[57],
            header_buf[58],
            header_buf[59],
            header_buf[60],
            header_buf[61],
            header_buf[62],
            header_buf[63],
        ]);

        let mut span_id = [0u8; 8];
        span_id.copy_from_slice(&header_buf[64..72]);

        let flags = header_buf[83];
        let trace_flags = header_buf[84];

        // `_padding` is private to the frame module; use Default::default() then
        // overwrite the public fields so the padding is guaranteed zero.
        let mut header = FrameHeader::default();
        header.request_id = request_id;
        header.idempotency_token = idempotency_token;
        header.trace_id = trace_id;
        header.causality_commit_index = causality_commit_index;
        header.deadline_unix_nanos = deadline_unix_nanos;
        header.span_id = span_id;
        header.payload_length = payload_length;
        header.error_code = error_code;
        header.opcode = opcode;
        header.version = version;
        header.flags = flags;
        header.trace_flags = trace_flags;

        Ok(Frame { header, payload })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;
    use crate::version::CURRENT_PROTOCOL_VERSION;

    /// Build a minimal valid header buffer with the given fields at the correct
    /// big-endian offsets.  All other fields are zero.
    fn make_header_buf(
        version: u8,
        payload_length: u32,
        error_code: u32,
        opcode: u16,
    ) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        // payload_length at offset 72 (u32 BE)
        buf[72..76].copy_from_slice(&payload_length.to_be_bytes());
        // error_code at offset 76 (u32 BE)
        buf[76..80].copy_from_slice(&error_code.to_be_bytes());
        // opcode at offset 80 (u16 BE)
        buf[80..82].copy_from_slice(&opcode.to_be_bytes());
        // version at offset 82
        buf[82] = version;
        buf
    }

    #[test]
    fn encode_decode_roundtrip_unary_frame() {
        let payload = bytes::Bytes::from_static(b"hello");
        // Construct via Default then overwrite — _padding is private to frame.rs.
        let mut header = FrameHeader::default();
        header.request_id = [1u8; 16];
        header.idempotency_token = [2u8; 16];
        header.trace_id = [3u8; 16];
        header.causality_commit_index = 99;
        header.deadline_unix_nanos = 1_000_000;
        header.span_id = [4u8; 8];
        header.payload_length = 5;
        header.error_code = 0;
        header.opcode = 42;
        header.version = CURRENT_PROTOCOL_VERSION;
        header.flags = 0b0000_0001;
        header.trace_flags = 0x02;
        let frame = Frame { header, payload };

        let mut buf = BytesMut::new();
        frame.encode(&mut buf).expect("encode should succeed");

        let mut bytes = buf.freeze();
        let decoded = Frame::decode(&mut bytes).expect("decode should succeed");

        assert_eq!(decoded.header.request_id, frame.header.request_id);
        assert_eq!(decoded.header.idempotency_token, frame.header.idempotency_token);
        assert_eq!(decoded.header.trace_id, frame.header.trace_id);
        assert_eq!(decoded.header.causality_commit_index, frame.header.causality_commit_index);
        assert_eq!(decoded.header.deadline_unix_nanos, frame.header.deadline_unix_nanos);
        assert_eq!(decoded.header.span_id, frame.header.span_id);
        assert_eq!(decoded.header.payload_length, frame.header.payload_length);
        assert_eq!(decoded.header.error_code, frame.header.error_code);
        assert_eq!(decoded.header.opcode, frame.header.opcode);
        assert_eq!(decoded.header.version, frame.header.version);
        assert_eq!(decoded.header.flags, frame.header.flags);
        assert_eq!(decoded.header.trace_flags, frame.header.trace_flags);
        assert_eq!(&decoded.payload[..], b"hello");
        // All input bytes consumed.
        assert_eq!(bytes.remaining(), 0);
    }

    #[test]
    fn decode_rejects_truncated_header() {
        // 50 bytes < HEADER_SIZE (88)
        let mut src = Bytes::from(vec![0u8; 50]);
        let err = Frame::decode(&mut src).expect_err("should fail");
        assert!(
            matches!(err, CodecError::Truncated { expected: 88, actual: 50 }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn decode_rejects_oversize_payload() {
        // Header with payload_length = MAX_FRAME_SIZE + 1, version = CURRENT_PROTOCOL_VERSION.
        // No payload bytes needed — the check fires before the bounds check.
        let oversize = MAX_FRAME_SIZE + 1;
        let buf = make_header_buf(CURRENT_PROTOCOL_VERSION, oversize, 0, 0);
        let mut src = Bytes::from(buf.to_vec());
        let err = Frame::decode(&mut src).expect_err("should fail");
        assert!(
            matches!(
                err,
                CodecError::PayloadTooLarge { payload_length, max }
                    if payload_length == oversize && max == MAX_FRAME_SIZE
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn decode_rejects_unsupported_version() {
        let buf = make_header_buf(0xFF, 0, 0, 0);
        let mut src = Bytes::from(buf.to_vec());
        let err = Frame::decode(&mut src).expect_err("should fail");
        assert!(matches!(err, CodecError::UnsupportedVersion(0xFF)), "unexpected error: {err}");
    }

    #[test]
    fn decode_rejects_error_frame_missing_payload() {
        // error_code = 1, payload_length = 0.
        let buf = make_header_buf(CURRENT_PROTOCOL_VERSION, 0, 1, 0);
        let mut src = Bytes::from(buf.to_vec());
        let err = Frame::decode(&mut src).expect_err("should fail");
        assert!(matches!(err, CodecError::ErrorFrameMissingPayload(1)), "unexpected error: {err}");
    }

    #[test]
    fn decode_truncated_payload() {
        // Header claims payload_length = 100 but we only append 50 payload bytes.
        let buf = make_header_buf(CURRENT_PROTOCOL_VERSION, 100, 0, 0);
        let mut bytes = Vec::with_capacity(HEADER_SIZE + 50);
        bytes.extend_from_slice(&buf);
        bytes.extend_from_slice(&[0u8; 50]); // only 50 of the 100 promised bytes
        let mut src = Bytes::from(bytes);
        let err = Frame::decode(&mut src).expect_err("should fail");
        assert!(
            matches!(
                err,
                CodecError::Truncated { expected, actual }
                    if expected == HEADER_SIZE + 100 && actual == HEADER_SIZE + 50
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn peek_payload_length_reads_header_field() {
        let buf = make_header_buf(CURRENT_PROTOCOL_VERSION, 12345, 0, 0);
        let len = peek_payload_length(&buf).expect("peek");
        assert_eq!(len, 12345);
    }

    #[test]
    fn peek_payload_length_rejects_truncated_header() {
        let short = [0u8; HEADER_SIZE - 1];
        let err = peek_payload_length(&short).expect_err("should fail");
        assert!(
            matches!(err, CodecError::Truncated { expected, actual }
                if expected == HEADER_SIZE && actual == HEADER_SIZE - 1),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn peek_payload_length_rejects_bad_version() {
        let buf = make_header_buf(0xFF, 0, 0, 0);
        let err = peek_payload_length(&buf).expect_err("should fail");
        assert!(matches!(err, CodecError::UnsupportedVersion(0xFF)), "unexpected error: {err}");
    }

    #[test]
    fn peek_payload_length_rejects_oversize() {
        let oversize = MAX_FRAME_SIZE + 1;
        let buf = make_header_buf(CURRENT_PROTOCOL_VERSION, oversize, 0, 0);
        let err = peek_payload_length(&buf).expect_err("should fail");
        assert!(
            matches!(err, CodecError::PayloadTooLarge { payload_length, max }
                if payload_length == oversize && max == MAX_FRAME_SIZE),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn encode_roundtrip_fills_padding_with_zeros() {
        let payload = bytes::Bytes::from_static(b"pad");
        // Construct via Default then overwrite — _padding is private to frame.rs.
        let mut hdr = FrameHeader::default();
        hdr.payload_length = 3;
        hdr.version = CURRENT_PROTOCOL_VERSION;
        hdr.opcode = 1;
        let frame = Frame { header: hdr, payload };

        let mut buf = BytesMut::new();
        frame.encode(&mut buf).expect("encode should succeed");

        let wire = buf.freeze();
        // Padding bytes are at offsets 85, 86, 87 in the header.
        assert_eq!(wire[85], 0x00, "padding byte at offset 85 must be zero");
        assert_eq!(wire[86], 0x00, "padding byte at offset 86 must be zero");
        assert_eq!(wire[87], 0x00, "padding byte at offset 87 must be zero");
    }
}
