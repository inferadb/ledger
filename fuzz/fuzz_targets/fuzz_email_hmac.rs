//! Fuzz target for HMAC email hash.
//!
//! Tests that arbitrary email strings never produce HMAC collisions with random
//! inputs, and that normalization + hashing never panics.

#![no_main]

use libfuzzer_sys::fuzz_target;

use inferadb_ledger_types::email_hash::{EmailBlindingKey, compute_email_hmac, normalize_email};

fuzz_target!(|data: &[u8]| {
    if data.len() < 33 {
        return;
    }

    let selector = data[0] % 3;
    let payload = &data[1..];

    match selector {
        // Normalization must never panic on arbitrary UTF-8 or non-UTF-8.
        0 => {
            if let Ok(s) = std::str::from_utf8(payload) {
                let _ = normalize_email(s);
            }
        }
        // HMAC computation with a fixed key must never panic.
        // Two distinct normalized emails must produce distinct HMACs.
        1 => {
            if payload.len() >= 32 {
                let key_bytes: [u8; 32] = payload[..32].try_into().unwrap();
                let key = EmailBlindingKey::new(key_bytes, 1);
                let email_bytes = &payload[32..];
                if let Ok(email) = std::str::from_utf8(email_bytes) {
                    let normalized = normalize_email(email);
                    if !normalized.is_empty() {
                        let hmac = compute_email_hmac(&key, &normalized);
                        // HMAC output must be 64 hex characters.
                        assert_eq!(hmac.len(), 64, "HMAC output must be 64 hex chars");
                        // HMAC must be deterministic: same input → same output.
                        let hmac2 = compute_email_hmac(&key, &normalized);
                        assert_eq!(hmac, hmac2, "HMAC must be deterministic");
                    }
                }
            }
        }
        // Two different emails with the same key must produce different HMACs
        // (no false positives). Split payload into two email candidates.
        _ => {
            if payload.len() >= 32 + 2 {
                let key_bytes: [u8; 32] = payload[..32].try_into().unwrap();
                let key = EmailBlindingKey::new(key_bytes, 1);
                let rest = &payload[32..];
                // Split the remaining bytes at the midpoint.
                let mid = rest.len() / 2;
                let (a_bytes, b_bytes) = (&rest[..mid], &rest[mid..]);
                if let (Ok(a), Ok(b)) = (std::str::from_utf8(a_bytes), std::str::from_utf8(b_bytes))
                {
                    let na = normalize_email(a);
                    let nb = normalize_email(b);
                    if !na.is_empty() && !nb.is_empty() && na != nb {
                        let ha = compute_email_hmac(&key, &na);
                        let hb = compute_email_hmac(&key, &nb);
                        // Different normalized emails must never produce the same HMAC.
                        assert_ne!(ha, hb, "HMAC collision detected: {na:?} vs {nb:?}");
                    }
                }
            }
        }
    }
});
