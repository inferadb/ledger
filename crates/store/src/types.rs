//! Key and value type encoding for the store engine.
//!
//! The store supports three key types:
//! - `u64` / `i64`: 8-byte fixed-width integers (big-endian for lexicographic ordering)
//! - `&str`: UTF-8 strings with length prefix
//! - `&[u8]`: Arbitrary byte slices with length prefix
//!
//! Values are always `&[u8]` (arbitrary bytes).

use std::cmp::Ordering;

use byteorder::{BigEndian, ByteOrder, WriteBytesExt};

/// Key type discriminant for compile-time table definitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    /// Unsigned 64-bit integer (big-endian for lexicographic ordering).
    U64,
    /// Signed 64-bit integer (transformed for lexicographic ordering).
    I64,
    /// UTF-8 string (length-prefixed).
    Str,
    /// Arbitrary bytes (length-prefixed).
    Bytes,
}

/// Trait for types that can be used as keys in store tables.
pub trait Key: Sized {
    /// The key type discriminant.
    const KEY_TYPE: KeyType;

    /// Encode the key into a byte buffer.
    fn encode(&self, buf: &mut Vec<u8>);

    /// Decode a key from a byte slice.
    fn decode(buf: &[u8]) -> Option<Self>;

    /// Compare two encoded keys lexicographically.
    fn compare_encoded(a: &[u8], b: &[u8]) -> Ordering;

    /// Returns the encoded size of this key.
    fn encoded_size(&self) -> usize;
}

/// Trait for types that can be used as values in store tables.
pub trait Value: Sized {
    /// Encode the value into a byte buffer.
    fn encode(&self, buf: &mut Vec<u8>);

    /// Decode a value from a byte slice.
    fn decode(buf: &[u8]) -> Option<Self>;

    /// Returns the encoded size of this value.
    fn encoded_size(&self) -> usize;
}

// ============================================================================
// u64 Key Implementation
// ============================================================================

impl Key for u64 {
    const KEY_TYPE: KeyType = KeyType::U64;

    fn encode(&self, buf: &mut Vec<u8>) {
        buf.write_u64::<BigEndian>(*self).unwrap();
    }

    fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() >= 8 { Some(BigEndian::read_u64(buf)) } else { None }
    }

    fn compare_encoded(a: &[u8], b: &[u8]) -> Ordering {
        // Big-endian encoding preserves numeric ordering
        a.cmp(b)
    }

    fn encoded_size(&self) -> usize {
        8
    }
}

// ============================================================================
// i64 Key Implementation
// ============================================================================

impl Key for i64 {
    const KEY_TYPE: KeyType = KeyType::I64;

    fn encode(&self, buf: &mut Vec<u8>) {
        // Flip sign bit for lexicographic ordering:
        // -9223372036854775808 -> 0x0000000000000000
        // -1                   -> 0x7FFFFFFFFFFFFFFF
        // 0                    -> 0x8000000000000000
        // 9223372036854775807  -> 0xFFFFFFFFFFFFFFFF
        let transformed = (*self as u64) ^ (1u64 << 63);
        buf.write_u64::<BigEndian>(transformed).unwrap();
    }

    fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() >= 8 {
            let transformed = BigEndian::read_u64(buf);
            Some((transformed ^ (1u64 << 63)) as i64)
        } else {
            None
        }
    }

    fn compare_encoded(a: &[u8], b: &[u8]) -> Ordering {
        // Transformed encoding preserves numeric ordering
        a.cmp(b)
    }

    fn encoded_size(&self) -> usize {
        8
    }
}

// ============================================================================
// String Key Implementation
// ============================================================================

impl Key for String {
    const KEY_TYPE: KeyType = KeyType::Str;

    fn encode(&self, buf: &mut Vec<u8>) {
        // Write bytes directly (no length prefix needed when stored inline)
        buf.extend_from_slice(self.as_bytes());
    }

    fn decode(buf: &[u8]) -> Option<Self> {
        std::str::from_utf8(buf).ok().map(|s| s.to_string())
    }

    fn compare_encoded(a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    fn encoded_size(&self) -> usize {
        self.len()
    }
}

impl Key for &str {
    const KEY_TYPE: KeyType = KeyType::Str;

    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_bytes());
    }

    fn decode(_buf: &[u8]) -> Option<Self> {
        // Can't return borrowed reference from owned data
        None
    }

    fn compare_encoded(a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    fn encoded_size(&self) -> usize {
        self.len()
    }
}

// ============================================================================
// Bytes Key Implementation
// ============================================================================

impl Key for Vec<u8> {
    const KEY_TYPE: KeyType = KeyType::Bytes;

    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self);
    }

    fn decode(buf: &[u8]) -> Option<Self> {
        Some(buf.to_vec())
    }

    fn compare_encoded(a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    fn encoded_size(&self) -> usize {
        self.len()
    }
}

impl Key for &[u8] {
    const KEY_TYPE: KeyType = KeyType::Bytes;

    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self);
    }

    fn decode(_buf: &[u8]) -> Option<Self> {
        // Can't return borrowed reference from owned data
        None
    }

    fn compare_encoded(a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    fn encoded_size(&self) -> usize {
        self.len()
    }
}

// ============================================================================
// Value Implementations
// ============================================================================

impl Value for Vec<u8> {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self);
    }

    fn decode(buf: &[u8]) -> Option<Self> {
        Some(buf.to_vec())
    }

    fn encoded_size(&self) -> usize {
        self.len()
    }
}

impl Value for &[u8] {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self);
    }

    fn decode(_buf: &[u8]) -> Option<Self> {
        None
    }

    fn encoded_size(&self) -> usize {
        self.len()
    }
}

impl Value for u64 {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.write_u64::<BigEndian>(*self).unwrap();
    }

    fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() >= 8 { Some(BigEndian::read_u64(buf)) } else { None }
    }

    fn encoded_size(&self) -> usize {
        8
    }
}

// ============================================================================
// Encoding Utilities
// ============================================================================

/// Encode a length-prefixed byte slice (used for variable-length fields).
pub fn encode_length_prefixed(data: &[u8], buf: &mut Vec<u8>) {
    // Use varint encoding for length (1-5 bytes for lengths up to 4GB)
    encode_varint(data.len() as u32, buf);
    buf.extend_from_slice(data);
}

/// Decode a length-prefixed byte slice.
pub fn decode_length_prefixed(buf: &[u8]) -> Option<(&[u8], usize)> {
    let (len, varint_size) = decode_varint(buf)?;
    let total_size = varint_size + len as usize;
    if buf.len() >= total_size { Some((&buf[varint_size..total_size], total_size)) } else { None }
}

/// Encode a u32 as a varint (1-5 bytes).
pub fn encode_varint(mut value: u32, buf: &mut Vec<u8>) {
    loop {
        if value < 0x80 {
            buf.push(value as u8);
            return;
        }
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
}

/// Decode a varint from a byte slice. Returns (value, bytes_consumed).
pub fn decode_varint(buf: &[u8]) -> Option<(u32, usize)> {
    let mut value: u32 = 0;
    let mut shift = 0;

    for (i, &byte) in buf.iter().enumerate() {
        if i >= 5 {
            return None; // Varint too long
        }

        value |= ((byte & 0x7F) as u32) << shift;

        if byte & 0x80 == 0 {
            return Some((value, i + 1));
        }

        shift += 7;
    }

    None // Incomplete varint
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u64_encoding() {
        let mut buf = Vec::new();
        Key::encode(&42u64, &mut buf);
        assert_eq!(buf.len(), 8);
        assert_eq!(<u64 as Key>::decode(&buf), Some(42));
    }

    #[test]
    fn test_i64_encoding_ordering() {
        let values = [-1000i64, -1, 0, 1, 1000];
        let mut encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|v| {
                let mut buf = Vec::new();
                v.encode(&mut buf);
                buf
            })
            .collect();

        // Verify encoded order matches numeric order
        let sorted_encoded = encoded.clone();
        encoded.sort();
        assert_eq!(encoded, sorted_encoded);
    }

    #[test]
    fn test_varint_encoding() {
        for value in [0u32, 1, 127, 128, 255, 16383, 16384, 0x7FFFFFFF] {
            let mut buf = Vec::new();
            encode_varint(value, &mut buf);
            let (decoded, _) = decode_varint(&buf).unwrap();
            assert_eq!(decoded, value);
        }
    }
}
