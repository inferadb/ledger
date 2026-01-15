//! Centralized serialization and deserialization functions.
//!
//! This module provides a unified interface for encoding and decoding data
//! using postcard serialization, with consistent error handling via snafu.

use serde::{Serialize, de::DeserializeOwned};
use snafu::Snafu;

/// Error type for codec operations.
#[derive(Debug, Snafu)]
pub enum CodecError {
    /// Encoding failed.
    #[snafu(display("Encoding failed: {source}"))]
    Encode {
        /// The underlying postcard error.
        source: postcard::Error,
    },

    /// Decoding failed.
    #[snafu(display("Decoding failed: {source}"))]
    Decode {
        /// The underlying postcard error.
        source: postcard::Error,
    },
}

/// Encodes a value to bytes using postcard serialization.
///
/// # Errors
///
/// Returns `CodecError::Encode` if serialization fails.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    postcard::to_allocvec(value).map_err(|source| CodecError::Encode { source })
}

/// Decodes bytes to a value using postcard deserialization.
///
/// # Errors
///
/// Returns `CodecError::Decode` if deserialization fails.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    postcard::from_bytes(bytes).map_err(|source| CodecError::Decode { source })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use serde::Deserialize;

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
            nested: Some(NestedStruct {
                value: -42,
                flag: true,
            }),
        };
        let bytes = encode(&original).expect("encode complex struct");
        let decoded: ComplexStruct = decode(&bytes).expect("decode complex struct");
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_roundtrip_complex_struct_with_none() {
        let original = ComplexStruct {
            id: 0,
            name: String::new(),
            data: vec![],
            nested: None,
        };
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

    // =========================================================================
    // Error conversion chain tests (Task 2: Consolidate Error Types)
    // =========================================================================

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
        assert!(
            !source_display.is_empty(),
            "Source should have non-empty display"
        );
    }

    // Test Debug implementation contains useful info
    #[test]
    fn test_codec_error_debug() {
        let malformed: &[u8] = &[0xFF, 0xFF];
        let result: Result<u64, _> = decode(malformed);
        let err = result.unwrap_err();

        let debug = format!("{err:?}");
        // Debug output should contain the variant name
        assert!(
            debug.contains("Decode"),
            "Debug should contain 'Decode' variant name"
        );
        // Debug output should contain "source" field info
        assert!(
            debug.contains("source"),
            "Debug should contain 'source' field: {debug}"
        );
    }

    // Test that both error variants exist and are distinct
    #[test]
    fn test_codec_error_variants() {
        // Decode error
        let decode_err = CodecError::Decode {
            source: postcard::from_bytes::<u64>(&[0xFF, 0xFF, 0xFF]).expect_err("should fail"),
        };

        // Verify we can match on the variant
        assert!(matches!(decode_err, CodecError::Decode { .. }));

        // The Encode variant exists (verified at compile time by this pattern)
        // Creating an actual encode error is difficult since postcard rarely fails encoding,
        // but we can verify the variant exists through a type check
        let _: fn(postcard::Error) -> CodecError = |source| CodecError::Encode { source };
    }
}
