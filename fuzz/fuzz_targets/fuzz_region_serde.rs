//! Fuzz target for Region enum serialization/deserialization.
//!
//! Tests that arbitrary bytes never cause panics when deserialized as a
//! Region, and that valid Region values survive JSON and postcard round-trips.

#![no_main]

use libfuzzer_sys::fuzz_target;

use inferadb_ledger_types::{Region, ALL_REGIONS, decode, encode};

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    let selector = data[0] % 4;
    let payload = &data[1..];

    match selector {
        // Attempt postcard deserialization of arbitrary bytes.
        // Must never panic.
        0 => {
            let _: Result<Region, _> = decode(payload);
        }
        // Postcard roundtrip: pick a valid region by index, encode, decode.
        1 => {
            if !payload.is_empty() {
                let idx = payload[0] as usize % ALL_REGIONS.len();
                let region = ALL_REGIONS[idx];
                let encoded = encode(&region).expect("encode must not fail for valid Region");
                let decoded: Region = decode(&encoded).expect("decode must not fail for valid encoding");
                assert_eq!(region, decoded, "postcard roundtrip mismatch");
            }
        }
        // JSON deserialization of arbitrary UTF-8.
        // Must never panic — only return Err for invalid input.
        2 => {
            if let Ok(s) = std::str::from_utf8(payload) {
                let _: Result<Region, _> = serde_json::from_str(s);
            }
        }
        // JSON roundtrip: pick a valid region, serialize, deserialize.
        _ => {
            if !payload.is_empty() {
                let idx = payload[0] as usize % ALL_REGIONS.len();
                let region = ALL_REGIONS[idx];
                let json = serde_json::to_string(&region).expect("JSON serialize must not fail");
                let decoded: Region =
                    serde_json::from_str(&json).expect("JSON deserialize must not fail");
                assert_eq!(region, decoded, "JSON roundtrip mismatch");
            }
        }
    }
});
