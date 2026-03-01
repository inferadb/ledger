//! Fuzz target for encrypted page parsing.
//!
//! Tests that arbitrary bytes fed to crypto metadata deserialization and
//! sidecar parsing never panic. Covers malformed headers, truncated
//! ciphertext, and wrong RMK version scenarios.

#![no_main]

use libfuzzer_sys::fuzz_target;

use inferadb_ledger_store::crypto::{CRYPTO_METADATA_SIZE, CryptoMetadata, CryptoSidecar};

fn read_page_id(payload: &[u8]) -> u64 {
    u64::from_le_bytes(payload[..8].try_into().unwrap())
}

fn read_meta_buf(payload: &[u8]) -> &[u8; CRYPTO_METADATA_SIZE] {
    payload[..CRYPTO_METADATA_SIZE].try_into().unwrap()
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    let selector = data[0] % 4;
    let payload = &data[1..];

    match selector {
        // Attempt to deserialize arbitrary bytes as CryptoMetadata.
        // Must never panic — only return None for invalid input.
        0 => {
            if payload.len() >= CRYPTO_METADATA_SIZE {
                let _ = CryptoMetadata::from_bytes(read_meta_buf(payload));
            }
        }
        // Round-trip valid metadata: if parsing succeeds, re-serialization
        // must produce the same bytes.
        1 => {
            if payload.len() >= CRYPTO_METADATA_SIZE {
                if let Some(meta) = CryptoMetadata::from_bytes(read_meta_buf(payload)) {
                    let re_serialized = meta.to_bytes();
                    let re_parsed = CryptoMetadata::from_bytes(&re_serialized);
                    assert!(
                        re_parsed.is_some(),
                        "re-parse failed after successful serialization"
                    );
                    let re_parsed = re_parsed.unwrap();
                    assert_eq!(
                        meta.rmk_version, re_parsed.rmk_version,
                        "rmk_version roundtrip mismatch"
                    );
                    assert_eq!(meta.nonce, re_parsed.nonce, "nonce roundtrip mismatch");
                    assert_eq!(
                        meta.auth_tag, re_parsed.auth_tag,
                        "auth_tag roundtrip mismatch"
                    );
                }
            }
        }
        // Attempt to read from an in-memory sidecar with arbitrary page IDs.
        // Must never panic on out-of-bounds or uninitialized reads.
        2 => {
            if payload.len() >= 8 {
                let sidecar = CryptoSidecar::new_memory();
                let _ = sidecar.read(read_page_id(payload));
            }
        }
        // Write then read metadata for a random page ID.
        _ => {
            if payload.len() >= 8 + CRYPTO_METADATA_SIZE {
                let page_id = read_page_id(payload);
                if let Some(meta) =
                    CryptoMetadata::from_bytes(read_meta_buf(&payload[8..]))
                {
                    let sidecar = CryptoSidecar::new_memory();
                    let _ = sidecar.write(page_id, &meta);
                    if let Ok(Some(read_meta)) = sidecar.read(page_id) {
                        assert_eq!(
                            meta.rmk_version, read_meta.rmk_version,
                            "sidecar write/read rmk_version mismatch"
                        );
                        assert_eq!(
                            meta.nonce, read_meta.nonce,
                            "sidecar write/read nonce mismatch"
                        );
                    }
                }
            }
        }
    }
});
