//! Fuzz target for postcard codec roundtrip.
//!
//! Tests that arbitrary bytes fed to `postcard::from_bytes` for domain types
//! never panic, and that successfully decoded values roundtrip correctly.

#![no_main]

use libfuzzer_sys::fuzz_target;

use inferadb_ledger_types::codec::{decode, encode};
use inferadb_ledger_types::types::{
    BlockHeader, ChainCommitment, Entity, Operation, Relationship, SetCondition, ShardBlock,
    Transaction, VaultBlock, VaultEntry,
};

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    let selector = data[0] % 10;
    let payload = &data[1..];

    match selector {
        0 => try_roundtrip::<Operation>(payload),
        1 => try_roundtrip::<SetCondition>(payload),
        2 => try_roundtrip::<Entity>(payload),
        3 => try_roundtrip::<Relationship>(payload),
        4 => try_roundtrip::<Transaction>(payload),
        5 => try_roundtrip::<BlockHeader>(payload),
        6 => try_roundtrip::<VaultBlock>(payload),
        7 => try_roundtrip::<VaultEntry>(payload),
        8 => try_roundtrip::<ShardBlock>(payload),
        _ => try_roundtrip::<ChainCommitment>(payload),
    }
});

/// Attempt to decode arbitrary bytes as type T. If successful, re-encode
/// and verify the roundtrip produces the same value.
fn try_roundtrip<T>(data: &[u8])
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    if let Ok(value) = decode::<T>(data) {
        // If decoding succeeds, encoding must also succeed.
        let re_encoded = encode(&value);
        assert!(re_encoded.is_ok(), "encode failed after successful decode");

        // Re-decoding the re-encoded bytes must produce the same value.
        let re_decoded = decode::<T>(&re_encoded.expect("already checked"));
        assert!(re_decoded.is_ok(), "re-decode failed after successful encode");
        assert_eq!(
            value,
            re_decoded.expect("already checked"),
            "roundtrip mismatch"
        );
    }
    // Decode failure is expected for arbitrary bytes — no panic is the invariant.
}
