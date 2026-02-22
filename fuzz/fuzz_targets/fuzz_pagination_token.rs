//! Fuzz target for pagination token parsing.
//!
//! Tests that `PageTokenCodec::decode` never panics on arbitrary strings,
//! and that tokens from `encode` always roundtrip successfully.

#![no_main]

use libfuzzer_sys::fuzz_target;

use inferadb_ledger_raft::{PageToken, PageTokenCodec};

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    let selector = data[0] % 3;
    let payload = &data[1..];

    match selector {
        // Decode arbitrary strings — must never panic
        0 => fuzz_decode_arbitrary(payload),
        // Decode arbitrary base64 — must never panic
        1 => fuzz_decode_base64(payload),
        // Roundtrip: encode a valid token, then decode it
        _ => fuzz_roundtrip(payload),
    }
});

fn fuzz_decode_arbitrary(data: &[u8]) {
    // Try to interpret as UTF-8 string
    if let Ok(s) = std::str::from_utf8(data) {
        let codec = PageTokenCodec::new([0u8; 32]);
        let _ = codec.decode(s);
    }
}

fn fuzz_decode_base64(data: &[u8]) {
    use base64::Engine;
    // Encode raw bytes as base64, then try to decode as a pagination token.
    // This simulates a tampered but validly-encoded token.
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data);
    let codec = PageTokenCodec::new([0u8; 32]);
    let _ = codec.decode(&encoded);
}

fn fuzz_roundtrip(data: &[u8]) {
    // Use fuzzer data to construct a PageToken with varying fields
    if data.len() < 26 {
        return;
    }

    let organization_id = i64::from_le_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]);
    let vault_id = i64::from_le_bytes([
        data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
    ]);
    let at_height = u64::from_le_bytes([
        data[16], data[17], data[18], data[19], data[20], data[21], data[22], data[23],
    ]);
    let query_hash_bytes: [u8; 8] = [0u8; 8]; // Fixed for simplicity
    let last_key = data[24..].to_vec();

    let token = PageToken {
        version: 1,
        organization_id,
        vault_id,
        last_key,
        at_height,
        query_hash: query_hash_bytes,
    };

    let key = [42u8; 32];
    let codec = PageTokenCodec::new(key);
    let encoded = codec.encode(&token);
    let decoded = codec.decode(&encoded);

    // Roundtrip must succeed
    assert!(decoded.is_ok(), "roundtrip decode failed for valid token");
    let decoded = decoded.expect("already checked");
    assert_eq!(decoded, token, "roundtrip mismatch");

    // Different key must reject the token
    let other_codec = PageTokenCodec::new([99u8; 32]);
    let other_result = other_codec.decode(&encoded);
    assert!(other_result.is_err(), "different key should reject token");
}
