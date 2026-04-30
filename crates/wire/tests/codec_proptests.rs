//! Property-based tests for the wire codec.
//!
//! These tests use `proptest` to exhaustively verify codec invariants over
//! random inputs. They live in `tests/` (separate test binary) so they can
//! consume only the public API of the crate, mirroring how downstream
//! callers will use the codec.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use bytes::{Bytes, BytesMut};
use inferadb_ledger_wire::{
    CURRENT_PROTOCOL_VERSION, CodecError, ErrorCode, Frame, HEADER_SIZE, MAX_FRAME_SIZE, WireError,
};
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Helper: build a raw header byte buffer with specific fields at the correct
// big-endian offsets. All other bytes are zero. Mirrors the make_header_buf
// helper in codec.rs unit tests but lives here for the integration test binary.
// ---------------------------------------------------------------------------
fn make_header_buf(version: u8, payload_length: u32, error_code: u32) -> Vec<u8> {
    let mut buf = vec![0u8; HEADER_SIZE];
    // payload_length at offset 72 (u32 BE)
    buf[72..76].copy_from_slice(&payload_length.to_be_bytes());
    // error_code at offset 76 (u32 BE)
    buf[76..80].copy_from_slice(&error_code.to_be_bytes());
    // version at offset 82
    buf[82] = version;
    buf
}

// ---------------------------------------------------------------------------
// Strategy: generate any of the 17 ErrorCode variants.
// ---------------------------------------------------------------------------
fn arb_error_code() -> impl Strategy<Value = ErrorCode> {
    prop_oneof![
        Just(ErrorCode::NotFound),
        Just(ErrorCode::AlreadyExists),
        Just(ErrorCode::FailedPrecondition),
        Just(ErrorCode::PermissionDenied),
        Just(ErrorCode::InvalidArgument),
        Just(ErrorCode::Internal),
        Just(ErrorCode::Unauthenticated),
        Just(ErrorCode::RateLimited),
        Just(ErrorCode::Expired),
        Just(ErrorCode::TooManyAttempts),
        Just(ErrorCode::InvitationRateLimited),
        Just(ErrorCode::InvitationAlreadyResolved),
        Just(ErrorCode::InvitationEmailMismatch),
        Just(ErrorCode::InvitationAlreadyMember),
        Just(ErrorCode::InvitationDuplicatePending),
        Just(ErrorCode::StaleRouting),
        Just(ErrorCode::Deprecated),
    ]
}

// ---------------------------------------------------------------------------
// Strategy: generate a WireError with random fields including BTreeMap context.
// ---------------------------------------------------------------------------
fn arb_wire_error() -> impl Strategy<Value = WireError> {
    (
        arb_error_code(),
        ".*",                                              // message
        any::<bool>(),                                     // retryable
        any::<u64>(),                                      // retry_after_ms
        proptest::collection::btree_map(".*", ".*", 0..8), // context
        ".*",                                              // suggested_action
    )
        .prop_map(|(code, message, retryable, retry_after_ms, context, suggested_action)| {
            WireError { code, message, retryable, retry_after_ms, context, suggested_action }
        })
}

// ---------------------------------------------------------------------------
// Property 1: every valid frame round-trips through encode → decode.
// ---------------------------------------------------------------------------
proptest! {
    #[test]
    fn frame_roundtrip(
        request_id in any::<[u8; 16]>(),
        idempotency_token in any::<[u8; 16]>(),
        trace_id in any::<[u8; 16]>(),
        causality_commit_index in any::<u64>(),
        deadline_unix_nanos in any::<u64>(),
        span_id in any::<[u8; 8]>(),
        opcode in any::<u16>(),
        flags in any::<u8>(),
        trace_flags in any::<u8>(),
        payload in proptest::collection::vec(any::<u8>(), 0..1024),
    ) {
        // Construct via Default + field assignment — _padding is private, so
        // struct literal syntax with `..Default::default()` is not available
        // from outside the crate.
        let mut frame = Frame::default();
        frame.header.request_id = request_id;
        frame.header.idempotency_token = idempotency_token;
        frame.header.trace_id = trace_id;
        frame.header.causality_commit_index = causality_commit_index;
        frame.header.deadline_unix_nanos = deadline_unix_nanos;
        frame.header.span_id = span_id;
        frame.header.payload_length = payload.len() as u32;
        // error_code = 0 (success); error frames require payload_length > 0
        // which is handled separately.
        frame.header.error_code = 0;
        frame.header.opcode = opcode;
        frame.header.version = CURRENT_PROTOCOL_VERSION;
        frame.header.flags = flags;
        frame.header.trace_flags = trace_flags;
        frame.payload = Bytes::from(payload);

        let mut buf = BytesMut::new();
        frame.encode(&mut buf).expect("encode of valid frame must succeed");
        let mut encoded = buf.freeze();
        let decoded = Frame::decode(&mut encoded).expect("decode of valid frame must succeed");

        prop_assert_eq!(decoded.header, frame.header);
        prop_assert_eq!(decoded.payload, frame.payload);
        // All bytes consumed.
        prop_assert_eq!(encoded.len(), 0);
    }
}

// ---------------------------------------------------------------------------
// Property 2: any payload_length > MAX_FRAME_SIZE → PayloadTooLarge.
// ---------------------------------------------------------------------------
proptest! {
    #[test]
    fn decode_rejects_oversize_payload_length(
        // Generate values strictly above MAX_FRAME_SIZE.
        oversize in (MAX_FRAME_SIZE + 1)..=u32::MAX,
    ) {
        // Build a raw header with the oversize payload_length field.
        // No payload bytes are appended — the check fires before the bounds check.
        let header_bytes = make_header_buf(CURRENT_PROTOCOL_VERSION, oversize, 0);
        let mut src = Bytes::from(header_bytes);
        let err = Frame::decode(&mut src).expect_err("must reject oversize payload_length");
        prop_assert!(
            matches!(
                err,
                CodecError::PayloadTooLarge { payload_length, max }
                    if payload_length == oversize && max == MAX_FRAME_SIZE
            ),
            "unexpected error variant: {err}"
        );
    }
}

// ---------------------------------------------------------------------------
// Property 3: any version != CURRENT_PROTOCOL_VERSION → UnsupportedVersion.
// ---------------------------------------------------------------------------
proptest! {
    #[test]
    fn decode_rejects_unsupported_version(
        bad_version in any::<u8>().prop_filter("must not equal current version",
            |v| *v != CURRENT_PROTOCOL_VERSION),
    ) {
        let header_bytes = make_header_buf(bad_version, 0, 0);
        let mut src = Bytes::from(header_bytes);
        let err = Frame::decode(&mut src).expect_err("must reject bad version");
        prop_assert!(
            matches!(err, CodecError::UnsupportedVersion(v) if v == bad_version),
            "unexpected error variant: {err}"
        );
    }
}

// ---------------------------------------------------------------------------
// Property 4a: any input shorter than HEADER_SIZE → Truncated.
// Decode must reject before attempting to parse header fields.
// ---------------------------------------------------------------------------
proptest! {
    #[test]
    fn decode_rejects_truncated_header(
        partial_header in proptest::collection::vec(any::<u8>(), 0..HEADER_SIZE),
    ) {
        let actual_len = partial_header.len();
        let mut src = Bytes::from(partial_header);
        let err = Frame::decode(&mut src).expect_err("must reject sub-header input");
        prop_assert!(
            matches!(
                err,
                CodecError::Truncated { expected, actual }
                    if expected == HEADER_SIZE && actual == actual_len
            ),
            "unexpected error variant: {err}"
        );
    }
}

// ---------------------------------------------------------------------------
// Property 4: truncated payload → Truncated.
// Construct a header claiming payload_length bytes but supply fewer actual bytes.
// ---------------------------------------------------------------------------
proptest! {
    #[test]
    fn decode_rejects_truncated_payload(
        // Generate (claimed, actual) where actual < claimed. Using prop_flat_map
        // makes `actual` dependent on `claimed`, avoiding the ~50% rejection rate
        // a `prop_assume!(actual < claimed)` filter on independent strategies would
        // produce (proptest warns at >10% rejection).
        (claimed_payload_len, actual_payload_len) in (1u32..=1024u32)
            .prop_flat_map(|claimed| (Just(claimed), 0u32..claimed)),
    ) {
        let header_bytes = make_header_buf(CURRENT_PROTOCOL_VERSION, claimed_payload_len, 0);
        let mut raw = header_bytes;
        raw.extend(std::iter::repeat_n(0u8, actual_payload_len as usize));

        let expected = HEADER_SIZE + claimed_payload_len as usize;
        let actual = HEADER_SIZE + actual_payload_len as usize;

        let mut src = Bytes::from(raw);
        let err = Frame::decode(&mut src).expect_err("must reject truncated frame");
        prop_assert!(
            matches!(
                err,
                CodecError::Truncated { expected: e, actual: a }
                    if e == expected && a == actual
            ),
            "unexpected error variant: {err}"
        );
    }
}

// ---------------------------------------------------------------------------
// Property 5: error_code != 0 with payload_length == 0 → ErrorFrameMissingPayload.
// ---------------------------------------------------------------------------
proptest! {
    #[test]
    fn decode_rejects_error_frame_missing_payload(
        // Non-zero error_code.
        error_code in 1u32..=u32::MAX,
    ) {
        // payload_length = 0, so the header claims an error frame but carries
        // no payload. A full 88-byte header (only) is provided — no payload bytes.
        let header_bytes = make_header_buf(CURRENT_PROTOCOL_VERSION, 0, error_code);
        let mut src = Bytes::from(header_bytes);
        let err = Frame::decode(&mut src).expect_err("must reject error frame with empty payload");
        prop_assert!(
            matches!(err, CodecError::ErrorFrameMissingPayload(ec) if ec == error_code),
            "unexpected error variant: {err}"
        );
    }
}

// ---------------------------------------------------------------------------
// Property 6: padding bytes (offsets 85-87) are always zero after encode.
// ---------------------------------------------------------------------------
proptest! {
    #[test]
    fn encoded_frame_padding_always_zero(
        payload in proptest::collection::vec(any::<u8>(), 0..1024),
        flags in any::<u8>(),
        trace_flags in any::<u8>(),
    ) {
        let mut frame = Frame::default();
        frame.header.payload_length = payload.len() as u32;
        frame.header.version = CURRENT_PROTOCOL_VERSION;
        frame.header.flags = flags;
        frame.header.trace_flags = trace_flags;
        frame.payload = Bytes::from(payload);

        let mut buf = BytesMut::new();
        frame.encode(&mut buf).expect("encode must succeed");

        // Bytes 85, 86, 87 are the _padding field at wire offsets 85..88.
        prop_assert_eq!(buf[85], 0x00, "padding byte at offset 85 must be zero");
        prop_assert_eq!(buf[86], 0x00, "padding byte at offset 86 must be zero");
        prop_assert_eq!(buf[87], 0x00, "padding byte at offset 87 must be zero");
    }
}

// ---------------------------------------------------------------------------
// Property 7: ErrorCode discriminant postcard round-trip.
// For every variant: postcard round-trip returns the same variant AND the
// encoded bytes equal what postcard produces for the corresponding u32 value
// (i.e., we're encoding the discriminant value, not the variant index).
// ---------------------------------------------------------------------------
proptest! {
    #[test]
    fn error_code_discriminant_postcard_roundtrip(code in arb_error_code()) {
        let discriminant = code.as_u32();

        // Encode the ErrorCode.
        let encoded = postcard::to_allocvec(&code).expect("ErrorCode serialization must succeed");

        // The on-wire bytes must equal the postcard encoding of the bare u32 value —
        // this verifies we serialize the discriminant, not the variant_index.
        let expected_u32_bytes =
            postcard::to_allocvec(&discriminant).expect("u32 serialization must succeed");
        prop_assert_eq!(
            &encoded,
            &expected_u32_bytes,
            "ErrorCode must serialize as its u32 discriminant, not variant_index"
        );

        // Round-trip: decoded variant must equal the original.
        let decoded: ErrorCode =
            postcard::from_bytes(&encoded).expect("ErrorCode deserialization must succeed");
        prop_assert_eq!(decoded, code);

        // Discriminant value must survive the round-trip unchanged.
        prop_assert_eq!(decoded.as_u32(), discriminant);
    }
}

// ---------------------------------------------------------------------------
// Property 8: WireError postcard round-trip preserves all fields.
// ---------------------------------------------------------------------------
proptest! {
    #[test]
    fn wire_error_postcard_roundtrip(wire_error in arb_wire_error()) {
        let encoded =
            postcard::to_allocvec(&wire_error).expect("WireError serialization must succeed");
        let decoded: WireError =
            postcard::from_bytes(&encoded).expect("WireError deserialization must succeed");

        prop_assert_eq!(decoded.code, wire_error.code);
        prop_assert_eq!(decoded.message, wire_error.message);
        prop_assert_eq!(decoded.retryable, wire_error.retryable);
        prop_assert_eq!(decoded.retry_after_ms, wire_error.retry_after_ms);
        prop_assert_eq!(decoded.context, wire_error.context);
        prop_assert_eq!(decoded.suggested_action, wire_error.suggested_action);
    }
}
