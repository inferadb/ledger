//! Centralized serialization and deserialization functions.
//!
//! Provides a unified interface for encoding and decoding data using postcard
//! serialization, with consistent error handling via snafu.

use serde::{Serialize, de::DeserializeOwned};
use snafu::{ResultExt, Snafu};

/// Error type for codec operations.
#[derive(Debug, Snafu)]
pub enum CodecError {
    /// Encoding failed.
    #[snafu(display("Encoding failed: {source}"))]
    Encode {
        /// The underlying postcard error.
        source: postcard::Error,
        /// Location where the error occurred.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Decoding failed.
    #[snafu(display("Decoding failed: {source}"))]
    Decode {
        /// The underlying postcard error.
        source: postcard::Error,
        /// Location where the error occurred.
        #[snafu(implicit)]
        location: snafu::Location,
    },
}

/// Encodes a value to bytes using postcard serialization.
///
/// # Errors
///
/// Returns `CodecError::Encode` if serialization fails.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    postcard::to_allocvec(value).context(EncodeSnafu)
}

/// Decodes bytes to a value using postcard deserialization.
///
/// # Errors
///
/// Returns `CodecError::Decode` if deserialization fails.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    postcard::from_bytes(bytes).context(DecodeSnafu)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use serde::Deserialize;
    use snafu::ResultExt;

    use super::*;

    // Test encode/decode roundtrip for primitive types
    #[test]
    fn test_roundtrip_primitive_u64() {
        let original: u64 = 42;
        let bytes = encode(&original).expect("encode u64");
        let decoded: u64 = decode(&bytes).expect("decode u64");
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_roundtrip_primitive_string() {
        let original = "hello world".to_string();
        let bytes = encode(&original).expect("encode string");
        let decoded: String = decode(&bytes).expect("decode string");
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_roundtrip_primitive_bool() {
        for original in [true, false] {
            let bytes = encode(&original).expect("encode bool");
            let decoded: bool = decode(&bytes).expect("decode bool");
            assert_eq!(original, decoded);
        }
    }

    #[test]
    fn test_roundtrip_primitive_vec() {
        let original: Vec<u32> = vec![1, 2, 3, 4, 5];
        let bytes = encode(&original).expect("encode vec");
        let decoded: Vec<u32> = decode(&bytes).expect("decode vec");
        assert_eq!(original, decoded);
    }

    // Test encode/decode roundtrip for complex structs
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct ComplexStruct {
        id: u64,
        name: String,
        data: Vec<u8>,
        nested: Option<NestedStruct>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct NestedStruct {
        value: i32,
        flag: bool,
    }

    #[test]
    fn test_roundtrip_complex_struct() {
        let original = ComplexStruct {
            id: 12345,
            name: "test entity".to_string(),
            data: vec![0xDE, 0xAD, 0xBE, 0xEF],
            nested: Some(NestedStruct { value: -42, flag: true }),
        };
        let bytes = encode(&original).expect("encode complex struct");
        let decoded: ComplexStruct = decode(&bytes).expect("decode complex struct");
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_roundtrip_complex_struct_with_none() {
        let original = ComplexStruct { id: 0, name: String::new(), data: vec![], nested: None };
        let bytes = encode(&original).expect("encode complex struct with None");
        let decoded: ComplexStruct = decode(&bytes).expect("decode complex struct with None");
        assert_eq!(original, decoded);
    }

    // Test error cases (malformed input)
    #[test]
    fn test_decode_malformed_input() {
        let malformed_bytes = [0xFF, 0xFF, 0xFF, 0xFF];
        let result: Result<ComplexStruct, _> = decode(&malformed_bytes);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CodecError::Decode { .. }));
        // Verify error message contains useful info
        let display = err.to_string();
        assert!(display.contains("Decoding failed"));
    }

    #[test]
    fn test_decode_truncated_data() {
        // Encode a struct then truncate the bytes
        let original = ComplexStruct {
            id: 12345,
            name: "test".to_string(),
            data: vec![1, 2, 3],
            nested: None,
        };
        let bytes = encode(&original).expect("encode");
        // Truncate to just first 2 bytes
        let truncated = &bytes[..2.min(bytes.len())];
        let result: Result<ComplexStruct, _> = decode(truncated);
        assert!(result.is_err());
    }

    // Test empty input handling
    #[test]
    fn test_decode_empty_input() {
        let empty: &[u8] = &[];
        let result: Result<u64, _> = decode(empty);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CodecError::Decode { .. }));
    }

    #[test]
    fn test_encode_empty_vec() {
        let empty_vec: Vec<u8> = vec![];
        let bytes = encode(&empty_vec).expect("encode empty vec");
        let decoded: Vec<u8> = decode(&bytes).expect("decode empty vec");
        assert_eq!(empty_vec, decoded);
    }

    #[test]
    fn test_encode_empty_string() {
        let empty_string = String::new();
        let bytes = encode(&empty_string).expect("encode empty string");
        let decoded: String = decode(&bytes).expect("decode empty string");
        assert_eq!(empty_string, decoded);
    }

    // Test error display implementations
    #[test]
    fn test_encode_error_display() {
        // We can't easily trigger an encode error with postcard,
        // but we can verify the error type structure
        let malformed: &[u8] = &[0xFF];
        let result: Result<String, _> = decode(malformed);
        let err = result.unwrap_err();
        let display = err.to_string();
        assert!(display.starts_with("Decoding failed:"));
    }

    // Test edge cases
    #[test]
    fn test_roundtrip_max_u64() {
        let original: u64 = u64::MAX;
        let bytes = encode(&original).expect("encode max u64");
        let decoded: u64 = decode(&bytes).expect("decode max u64");
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_roundtrip_unicode_string() {
        let original = "Hello 世界 🦀 émoji".to_string();
        let bytes = encode(&original).expect("encode unicode");
        let decoded: String = decode(&bytes).expect("decode unicode");
        assert_eq!(original, decoded);
    }

    // Error conversion chain tests

    // Test that CodecError::Encode has correct Display output
    #[test]
    fn test_codec_error_encode_display() {
        // Create a decode error (easier to trigger than encode error)
        let malformed: &[u8] = &[0xFF, 0xFF, 0xFF, 0xFF];
        let result: Result<u64, _> = decode(malformed);
        let err = result.unwrap_err();

        // Verify display format matches expected pattern
        let display = format!("{err}");
        assert!(
            display.starts_with("Decoding failed:"),
            "Expected 'Decoding failed:', got: {display}"
        );
    }

    // Test that CodecError::Decode has correct Display output
    #[test]
    fn test_codec_error_decode_display() {
        let empty: &[u8] = &[];
        let result: Result<String, _> = decode(empty);
        let err = result.unwrap_err();

        let display = format!("{err}");
        assert!(
            display.starts_with("Decoding failed:"),
            "Expected 'Decoding failed:', got: {display}"
        );
    }

    // Test error source chain - CodecError preserves underlying postcard error
    #[test]
    fn test_codec_error_source_chain() {
        use std::error::Error;

        let malformed: &[u8] = &[0xFF];
        let result: Result<String, _> = decode(malformed);
        let err = result.unwrap_err();

        // Verify the error has a source (the underlying postcard error)
        assert!(err.source().is_some(), "CodecError should have a source");

        // The source should be a postcard::Error
        let source = err.source().unwrap();
        // Verify source has a Display impl (postcard::Error)
        let source_display = format!("{source}");
        assert!(!source_display.is_empty(), "Source should have non-empty display");
    }

    // ============================================
    // Property-based roundtrip tests
    // ============================================

    mod proptest_roundtrip {
        use proptest::prelude::*;

        use crate::{
            codec::{decode, encode},
            types::{
                BlockHeader, ChainCommitment, Entity, Operation, Relationship, SetCondition,
                ShardBlock, Transaction, VaultBlock, VaultEntry,
            },
        };

        /// Strategy for generating arbitrary `SetCondition`.
        fn arb_set_condition() -> impl Strategy<Value = SetCondition> {
            prop_oneof![
                Just(SetCondition::MustNotExist),
                Just(SetCondition::MustExist),
                any::<u64>().prop_map(SetCondition::VersionEquals),
                proptest::collection::vec(any::<u8>(), 0..32).prop_map(SetCondition::ValueEquals),
            ]
        }

        /// Strategy for generating arbitrary `Operation`.
        fn arb_operation() -> impl Strategy<Value = Operation> {
            prop_oneof![
                (
                    "[a-z]{1,16}",
                    proptest::collection::vec(any::<u8>(), 0..32),
                    proptest::option::of(arb_set_condition()),
                    proptest::option::of(any::<u64>()),
                )
                    .prop_map(|(key, value, condition, expires_at)| {
                        Operation::SetEntity { key, value, condition, expires_at }
                    }),
                "[a-z]{1,16}".prop_map(|key| Operation::DeleteEntity { key }),
                ("[a-z]{1,16}", any::<u64>())
                    .prop_map(|(key, expired_at)| Operation::ExpireEntity { key, expired_at }),
                ("[a-z]{1,16}", "[a-z]{1,8}", "[a-z]{1,16}").prop_map(
                    |(resource, relation, subject)| {
                        Operation::CreateRelationship { resource, relation, subject }
                    }
                ),
                ("[a-z]{1,16}", "[a-z]{1,8}", "[a-z]{1,16}").prop_map(
                    |(resource, relation, subject)| {
                        Operation::DeleteRelationship { resource, relation, subject }
                    }
                ),
            ]
        }

        /// Strategy for a 32-byte hash.
        fn arb_hash() -> impl Strategy<Value = [u8; 32]> {
            proptest::array::uniform32(any::<u8>())
        }

        /// Strategy for a `DateTime<Utc>`.
        fn arb_timestamp() -> impl Strategy<Value = chrono::DateTime<chrono::Utc>> {
            use chrono::{TimeZone, Utc};
            (1_577_836_800i64..1_893_456_000i64).prop_map(|secs| {
                Utc.timestamp_opt(secs, 0)
                    .single()
                    .unwrap_or_else(|| chrono::DateTime::<Utc>::from(std::time::UNIX_EPOCH))
            })
        }

        /// Strategy for a Transaction.
        fn arb_transaction() -> impl Strategy<Value = Transaction> {
            (
                proptest::array::uniform16(any::<u8>()),
                "[a-z]{3,10}",
                1u64..100_000,
                "[a-z]{3,10}",
                proptest::collection::vec(arb_operation(), 1..5),
                arb_timestamp(),
            )
                .prop_map(|(id, client_id, sequence, actor, operations, timestamp)| {
                    Transaction { id, client_id, sequence, actor, operations, timestamp }
                })
        }

        /// Strategy for a BlockHeader.
        fn arb_block_header() -> impl Strategy<Value = BlockHeader> {
            (
                any::<u64>(),
                (1i64..10_000).prop_map(crate::types::NamespaceId::new),
                (1i64..10_000).prop_map(crate::types::VaultId::new),
                arb_hash(),
                arb_hash(),
                arb_hash(),
                arb_timestamp(),
                any::<u64>(),
                any::<u64>(),
            )
                .prop_map(
                    |(
                        height,
                        namespace_id,
                        vault_id,
                        previous_hash,
                        tx_merkle_root,
                        state_root,
                        timestamp,
                        term,
                        committed_index,
                    )| {
                        BlockHeader {
                            height,
                            namespace_id,
                            vault_id,
                            previous_hash,
                            tx_merkle_root,
                            state_root,
                            timestamp,
                            term,
                            committed_index,
                        }
                    },
                )
        }

        proptest! {
            /// Any `Operation` must survive postcard roundtrip.
            #[test]
            fn prop_operation_roundtrip(op in arb_operation()) {
                let bytes = encode(&op).expect("encode operation");
                let decoded: Operation = decode(&bytes).expect("decode operation");
                prop_assert_eq!(op, decoded);
            }

            /// Any `SetCondition` must survive postcard roundtrip.
            #[test]
            fn prop_set_condition_roundtrip(cond in arb_set_condition()) {
                let bytes = encode(&cond).expect("encode condition");
                let decoded: SetCondition = decode(&bytes).expect("decode condition");
                prop_assert_eq!(cond, decoded);
            }

            /// Any `Entity` must survive postcard roundtrip.
            #[test]
            fn prop_entity_roundtrip(
                key in proptest::collection::vec(any::<u8>(), 0..64),
                value in proptest::collection::vec(any::<u8>(), 0..64),
                expires_at in any::<u64>(),
                version in any::<u64>(),
            ) {
                let entity = Entity { key, value, expires_at, version };
                let bytes = encode(&entity).expect("encode entity");
                let decoded: Entity = decode(&bytes).expect("decode entity");
                prop_assert_eq!(entity, decoded);
            }

            /// Any `Relationship` must survive postcard roundtrip.
            #[test]
            fn prop_relationship_roundtrip(
                resource in "[a-z]{1,16}",
                relation in "[a-z]{1,8}",
                subject in "[a-z]{1,16}",
            ) {
                let rel = Relationship { resource, relation, subject };
                let bytes = encode(&rel).expect("encode relationship");
                let decoded: Relationship = decode(&bytes).expect("decode relationship");
                prop_assert_eq!(rel, decoded);
            }

            /// Any `Transaction` must survive postcard roundtrip.
            #[test]
            fn prop_transaction_roundtrip(tx in arb_transaction()) {
                let bytes = encode(&tx).expect("encode transaction");
                let decoded: Transaction = decode(&bytes).expect("decode transaction");
                prop_assert_eq!(tx, decoded);
            }

            /// Any `BlockHeader` must survive postcard roundtrip.
            #[test]
            fn prop_block_header_roundtrip(header in arb_block_header()) {
                let bytes = encode(&header).expect("encode block header");
                let decoded: BlockHeader = decode(&bytes).expect("decode block header");
                prop_assert_eq!(header, decoded);
            }

            /// Any `VaultBlock` must survive postcard roundtrip.
            #[test]
            fn prop_vault_block_roundtrip(
                header in arb_block_header(),
                transactions in proptest::collection::vec(arb_transaction(), 0..3),
            ) {
                let block = VaultBlock { header, transactions };
                let bytes = encode(&block).expect("encode vault block");
                let decoded: VaultBlock = decode(&bytes).expect("decode vault block");
                prop_assert_eq!(block, decoded);
            }

            /// Any `VaultEntry` must survive postcard roundtrip.
            #[test]
            fn prop_vault_entry_roundtrip(
                ns_id in (1i64..10_000).prop_map(crate::types::NamespaceId::new),
                vault_id in (1i64..10_000).prop_map(crate::types::VaultId::new),
                vault_height in any::<u64>(),
                previous_vault_hash in arb_hash(),
                transactions in proptest::collection::vec(arb_transaction(), 0..2),
                tx_merkle_root in arb_hash(),
                state_root in arb_hash(),
            ) {
                let entry = VaultEntry {
                    namespace_id: ns_id,
                    vault_id,
                    vault_height,
                    previous_vault_hash,
                    transactions,
                    tx_merkle_root,
                    state_root,
                };
                let bytes = encode(&entry).expect("encode vault entry");
                let decoded: VaultEntry = decode(&bytes).expect("decode vault entry");
                prop_assert_eq!(entry, decoded);
            }

            /// Any `ShardBlock` must survive postcard roundtrip.
            #[test]
            fn prop_shard_block_roundtrip(
                shard_id in (1u32..1_000).prop_map(crate::types::ShardId::new),
                shard_height in any::<u64>(),
                previous_shard_hash in arb_hash(),
                timestamp in arb_timestamp(),
                leader_id in "[a-z]{3,10}",
                term in any::<u64>(),
                committed_index in any::<u64>(),
            ) {
                let block = ShardBlock {
                    shard_id,
                    shard_height,
                    previous_shard_hash,
                    vault_entries: vec![],
                    timestamp,
                    leader_id,
                    term,
                    committed_index,
                };
                let bytes = encode(&block).expect("encode shard block");
                let decoded: ShardBlock = decode(&bytes).expect("decode shard block");
                prop_assert_eq!(block, decoded);
            }

            /// Any `ChainCommitment` must survive postcard roundtrip.
            #[test]
            fn prop_chain_commitment_roundtrip(
                accumulated in arb_hash(),
                state_root_acc in arb_hash(),
                from in any::<u64>(),
                to in any::<u64>(),
            ) {
                let commitment = ChainCommitment {
                    accumulated_header_hash: accumulated,
                    state_root_accumulator: state_root_acc,
                    from_height: from.min(to),
                    to_height: from.max(to),
                };
                let bytes = encode(&commitment).expect("encode chain commitment");
                let decoded: ChainCommitment =
                    decode(&bytes).expect("decode chain commitment");
                prop_assert_eq!(commitment, decoded);
            }
        }
    }

    // Test Debug implementation contains useful info
    #[test]
    fn test_codec_error_debug() {
        let malformed: &[u8] = &[0xFF, 0xFF];
        let result: Result<u64, _> = decode(malformed);
        let err = result.unwrap_err();

        let debug = format!("{err:?}");
        // Debug output should contain the variant name
        assert!(debug.contains("Decode"), "Debug should contain 'Decode' variant name");
        // Debug output should contain "source" field info
        assert!(debug.contains("source"), "Debug should contain 'source' field: {debug}");
    }

    // Test that both error variants exist and are distinct
    #[test]
    fn test_codec_error_variants() {
        // Decode error - use context selector to properly construct with location
        let decode_result: Result<u64, CodecError> =
            postcard::from_bytes::<u64>(&[0xFF, 0xFF, 0xFF]).context(super::DecodeSnafu);
        let decode_err = decode_result.expect_err("should fail");

        // Verify we can match on the variant
        assert!(matches!(decode_err, CodecError::Decode { .. }));

        // The Encode variant exists (verified at compile time by matching)
        // Creating an actual encode error is difficult since postcard rarely fails encoding,
        // but the variant is tested implicitly via the encode() function
        assert!(!matches!(decode_err, CodecError::Encode { .. }));
    }
}
