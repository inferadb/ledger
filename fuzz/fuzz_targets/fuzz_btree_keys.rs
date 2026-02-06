//! Fuzz target for B+ tree key encoding/decoding.
//!
//! Tests that `Key::decode` never panics on arbitrary input, and that
//! successfully decoded keys roundtrip correctly. Also tests varint
//! and length-prefixed decoding.

#![no_main]

use libfuzzer_sys::fuzz_target;

use inferadb_ledger_store::types::{decode_length_prefixed, decode_varint, Key, Value};

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    let selector = data[0] % 7;
    let payload = &data[1..];

    match selector {
        0 => fuzz_u64_key(payload),
        1 => fuzz_i64_key(payload),
        2 => fuzz_string_key(payload),
        3 => fuzz_vec_u8_key(payload),
        4 => fuzz_varint(payload),
        5 => fuzz_length_prefixed(payload),
        _ => fuzz_u64_value(payload),
    }
});

fn fuzz_u64_key(data: &[u8]) {
    if let Some(key) = <u64 as Key>::decode(data) {
        let mut buf = Vec::new();
        <u64 as Key>::encode(&key, &mut buf);
        let roundtrip = <u64 as Key>::decode(&buf);
        assert_eq!(roundtrip, Some(key), "u64 key roundtrip mismatch");
    }
}

fn fuzz_i64_key(data: &[u8]) {
    if let Some(key) = <i64 as Key>::decode(data) {
        let mut buf = Vec::new();
        <i64 as Key>::encode(&key, &mut buf);
        let roundtrip = <i64 as Key>::decode(&buf);
        assert_eq!(roundtrip, Some(key), "i64 key roundtrip mismatch");
    }
}

fn fuzz_string_key(data: &[u8]) {
    if let Some(key) = <String as Key>::decode(data) {
        let mut buf = Vec::new();
        <String as Key>::encode(&key, &mut buf);
        let roundtrip = <String as Key>::decode(&buf);
        assert_eq!(roundtrip, Some(key), "String key roundtrip mismatch");
    }
}

fn fuzz_vec_u8_key(data: &[u8]) {
    if let Some(key) = <Vec<u8> as Key>::decode(data) {
        let mut buf = Vec::new();
        <Vec<u8> as Key>::encode(&key, &mut buf);
        let roundtrip = <Vec<u8> as Key>::decode(&buf);
        assert_eq!(roundtrip, Some(key), "Vec<u8> key roundtrip mismatch");
    }
}

fn fuzz_varint(data: &[u8]) {
    if let Some((value, consumed)) = decode_varint(data) {
        // Consumed bytes must not exceed input length
        assert!(consumed <= data.len(), "varint consumed more than input");
        // Value must fit in u32
        assert!(value <= u32::MAX, "varint exceeded u32::MAX");
    }
}

fn fuzz_length_prefixed(data: &[u8]) {
    if let Some((decoded, total_size)) = decode_length_prefixed(data) {
        // Total size must not exceed input length
        assert!(total_size <= data.len(), "length-prefixed consumed more than input");
        // Decoded length must be consistent with total size
        assert!(decoded.len() <= total_size, "decoded data exceeds total size");
    }
}

fn fuzz_u64_value(data: &[u8]) {
    if let Some(val) = <u64 as Value>::decode(data) {
        let mut buf = Vec::new();
        <u64 as Value>::encode(&val, &mut buf);
        let roundtrip = <u64 as Value>::decode(&buf);
        assert_eq!(roundtrip, Some(val), "u64 value roundtrip mismatch");
    }
}
