//! Key and value type encoding for the store engine.
//!
//! The store supports three key types:
//! - `u64` / `i64`: 8-byte fixed-width integers (big-endian for lexicographic ordering)
//! - `String` / `&str`: UTF-8 strings (stored inline without length prefix)
//! - `Vec<u8>` / `&[u8]`: Arbitrary byte slices (stored inline without length prefix)
//!
//! Values are `&[u8]` (arbitrary bytes) or `u64` (8-byte big-endian).

use std::cmp::Ordering;

use byteorder::{BigEndian, ByteOrder, WriteBytesExt};

/// Key type discriminant for compile-time table definitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    /// Unsigned 64-bit integer (big-endian for lexicographic ordering).
    U64,
    /// Signed 64-bit integer (transformed for lexicographic ordering).
    I64,
    /// UTF-8 string (stored inline without length prefix).
    Str,
    /// Arbitrary bytes (stored inline without length prefix).
    Bytes,
}

/// Trait for types that can be used as keys in store tables.
pub trait Key: Sized {
    /// The key type discriminant.
    const KEY_TYPE: KeyType;

    /// Encodes the key into a byte buffer.
    fn encode(&self, buf: &mut Vec<u8>);

    /// Decodes a key from a byte slice.
    fn decode(buf: &[u8]) -> Option<Self>;

    /// Compares two encoded keys lexicographically.
    fn compare_encoded(a: &[u8], b: &[u8]) -> Ordering;

    /// Returns the encoded size of this key.
    fn encoded_size(&self) -> usize;
}

/// Trait for types that can be used as values in store tables.
pub trait Value: Sized {
    /// Encodes the value into a byte buffer.
    fn encode(&self, buf: &mut Vec<u8>);

    /// Decodes a value from a byte slice.
    fn decode(buf: &[u8]) -> Option<Self>;

    /// Returns the encoded size of this value.
    fn encoded_size(&self) -> usize;
}

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

/// Encodes a length-prefixed byte slice (used for variable-length fields).
pub fn encode_length_prefixed(data: &[u8], buf: &mut Vec<u8>) {
    // Use varint encoding for length (1-5 bytes for lengths up to 4GB)
    encode_varint(data.len() as u32, buf);
    buf.extend_from_slice(data);
}

/// Decodes a length-prefixed byte slice.
pub fn decode_length_prefixed(buf: &[u8]) -> Option<(&[u8], usize)> {
    let (len, varint_size) = decode_varint(buf)?;
    let total_size = varint_size + len as usize;
    if buf.len() >= total_size { Some((&buf[varint_size..total_size], total_size)) } else { None }
}

/// Encodes a u32 as a varint (1-5 bytes).
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

/// Decodes a varint from a byte slice. Returns (value, bytes_consumed).
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

    // ─── u64 Key roundtrip and edge cases ──────────────────────

    #[test]
    fn test_u64_key_roundtrip_zero() {
        let mut buf = Vec::new();
        Key::encode(&0u64, &mut buf);
        assert_eq!(<u64 as Key>::decode(&buf), Some(0u64));
    }

    #[test]
    fn test_u64_key_roundtrip_max() {
        let mut buf = Vec::new();
        Key::encode(&u64::MAX, &mut buf);
        assert_eq!(<u64 as Key>::decode(&buf), Some(u64::MAX));
    }

    #[test]
    fn test_u64_key_decode_too_short() {
        assert_eq!(<u64 as Key>::decode(&[0u8; 7]), None);
        assert_eq!(<u64 as Key>::decode(&[]), None);
    }

    #[test]
    fn test_u64_key_encoded_size() {
        assert_eq!(Key::encoded_size(&42u64), 8);
    }

    #[test]
    fn test_u64_key_compare_encoded_ordering() {
        let pairs: Vec<(u64, u64)> = vec![(0, 1), (1, 100), (0, u64::MAX)];
        for (a, b) in pairs {
            let mut buf_a = Vec::new();
            let mut buf_b = Vec::new();
            Key::encode(&a, &mut buf_a);
            Key::encode(&b, &mut buf_b);
            assert_eq!(<u64 as Key>::compare_encoded(&buf_a, &buf_b), Ordering::Less);
            assert_eq!(<u64 as Key>::compare_encoded(&buf_b, &buf_a), Ordering::Greater);
            assert_eq!(<u64 as Key>::compare_encoded(&buf_a, &buf_a), Ordering::Equal);
        }
    }

    #[test]
    fn test_u64_key_type() {
        assert_eq!(<u64 as Key>::KEY_TYPE, KeyType::U64);
    }

    // ─── i64 Key roundtrip and edge cases ──────────────────────

    #[test]
    fn test_i64_key_roundtrip_boundaries() {
        for val in [i64::MIN, -1, 0, 1, i64::MAX] {
            let mut buf = Vec::new();
            val.encode(&mut buf);
            assert_eq!(<i64 as Key>::decode(&buf), Some(val));
        }
    }

    #[test]
    fn test_i64_key_decode_too_short() {
        assert_eq!(<i64 as Key>::decode(&[0u8; 7]), None);
        assert_eq!(<i64 as Key>::decode(&[]), None);
    }

    #[test]
    fn test_i64_key_encoded_size() {
        assert_eq!(Key::encoded_size(&-42i64), 8);
    }

    #[test]
    fn test_i64_key_compare_encoded_preserves_sign_order() {
        // The XOR transform should make negative < 0 < positive in byte order
        let values = [i64::MIN, -100, -1, 0, 1, 100, i64::MAX];
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|v| {
                let mut buf = Vec::new();
                v.encode(&mut buf);
                buf
            })
            .collect();

        for i in 0..encoded.len() - 1 {
            assert_eq!(
                <i64 as Key>::compare_encoded(&encoded[i], &encoded[i + 1]),
                Ordering::Less,
                "Expected {:?} < {:?}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_i64_key_type() {
        assert_eq!(<i64 as Key>::KEY_TYPE, KeyType::I64);
    }

    // ─── String Key roundtrip and edge cases ───────────────────

    #[test]
    fn test_string_key_roundtrip() {
        let s = "hello world".to_string();
        let mut buf = Vec::new();
        s.encode(&mut buf);
        assert_eq!(<String as Key>::decode(&buf), Some(s));
    }

    #[test]
    fn test_string_key_empty() {
        let s = String::new();
        let mut buf = Vec::new();
        s.encode(&mut buf);
        assert!(buf.is_empty());
        assert_eq!(<String as Key>::decode(&buf), Some(String::new()));
    }

    #[test]
    fn test_string_key_encoded_size() {
        assert_eq!(Key::encoded_size(&"hello".to_string()), 5);
        assert_eq!(Key::encoded_size(&String::new()), 0);
    }

    #[test]
    fn test_string_key_decode_invalid_utf8() {
        let invalid_utf8 = [0xFF, 0xFE, 0xFD];
        assert_eq!(<String as Key>::decode(&invalid_utf8), None);
    }

    #[test]
    fn test_string_key_compare_encoded() {
        let mut buf_a = Vec::new();
        let mut buf_b = Vec::new();
        "apple".to_string().encode(&mut buf_a);
        "banana".to_string().encode(&mut buf_b);
        assert_eq!(<String as Key>::compare_encoded(&buf_a, &buf_b), Ordering::Less);
    }

    #[test]
    fn test_string_key_type() {
        assert_eq!(<String as Key>::KEY_TYPE, KeyType::Str);
    }

    // ─── &str Key ──────────────────────────────────────────────

    #[test]
    fn test_str_ref_key_encode() {
        let mut buf = Vec::new();
        Key::encode(&"hello", &mut buf);
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn test_str_ref_key_decode_always_none() {
        // Can't return borrowed reference from owned data
        assert_eq!(<&str as Key>::decode(b"hello"), None);
    }

    #[test]
    fn test_str_ref_key_encoded_size() {
        assert_eq!(Key::encoded_size(&"test"), 4);
    }

    #[test]
    fn test_str_ref_key_type() {
        assert_eq!(<&str as Key>::KEY_TYPE, KeyType::Str);
    }

    // ─── Vec<u8> Key roundtrip and edge cases ──────────────────

    #[test]
    fn test_vec_u8_key_roundtrip() {
        let v = vec![1u8, 2, 3, 4];
        let mut buf = Vec::new();
        Key::encode(&v, &mut buf);
        assert_eq!(<Vec<u8> as Key>::decode(&buf), Some(v));
    }

    #[test]
    fn test_vec_u8_key_empty() {
        let v: Vec<u8> = Vec::new();
        let mut buf = Vec::new();
        Key::encode(&v, &mut buf);
        assert!(buf.is_empty());
        assert_eq!(<Vec<u8> as Key>::decode(&buf), Some(Vec::new()));
    }

    #[test]
    fn test_vec_u8_key_encoded_size() {
        assert_eq!(Key::encoded_size(&vec![0u8; 10]), 10);
    }

    #[test]
    fn test_vec_u8_key_type() {
        assert_eq!(<Vec<u8> as Key>::KEY_TYPE, KeyType::Bytes);
    }

    // ─── &[u8] Key ─────────────────────────────────────────────

    #[test]
    fn test_slice_u8_key_encode() {
        let data: &[u8] = &[0xAA, 0xBB];
        let mut buf = Vec::new();
        Key::encode(&data, &mut buf);
        assert_eq!(buf, vec![0xAA, 0xBB]);
    }

    #[test]
    fn test_slice_u8_key_decode_always_none() {
        assert_eq!(<&[u8] as Key>::decode(&[1, 2, 3]), None);
    }

    #[test]
    fn test_slice_u8_key_encoded_size() {
        let data: &[u8] = &[1, 2, 3];
        assert_eq!(Key::encoded_size(&data), 3);
    }

    #[test]
    fn test_slice_u8_key_type() {
        assert_eq!(<&[u8] as Key>::KEY_TYPE, KeyType::Bytes);
    }

    // ─── Vec<u8> Value ─────────────────────────────────────────

    #[test]
    fn test_vec_u8_value_roundtrip() {
        let v = vec![10u8, 20, 30];
        let mut buf = Vec::new();
        <Vec<u8> as Value>::encode(&v, &mut buf);
        assert_eq!(<Vec<u8> as Value>::decode(&buf), Some(v));
    }

    #[test]
    fn test_vec_u8_value_empty() {
        let v: Vec<u8> = Vec::new();
        let mut buf = Vec::new();
        <Vec<u8> as Value>::encode(&v, &mut buf);
        assert_eq!(<Vec<u8> as Value>::decode(&buf), Some(Vec::new()));
    }

    #[test]
    fn test_vec_u8_value_encoded_size() {
        assert_eq!(Value::encoded_size(&vec![0u8; 7]), 7);
    }

    // ─── &[u8] Value ───────────────────────────────────────────

    #[test]
    fn test_slice_u8_value_encode() {
        let data: &[u8] = &[0xCC, 0xDD];
        let mut buf = Vec::new();
        Value::encode(&data, &mut buf);
        assert_eq!(buf, vec![0xCC, 0xDD]);
    }

    #[test]
    fn test_slice_u8_value_decode_always_none() {
        assert_eq!(<&[u8] as Value>::decode(&[5, 6]), None);
    }

    #[test]
    fn test_slice_u8_value_encoded_size() {
        let data: &[u8] = &[1, 2, 3, 4, 5];
        assert_eq!(Value::encoded_size(&data), 5);
    }

    // ─── u64 Value ─────────────────────────────────────────────

    #[test]
    fn test_u64_value_roundtrip() {
        let v = 12345u64;
        let mut buf = Vec::new();
        Value::encode(&v, &mut buf);
        assert_eq!(buf.len(), 8);
        assert_eq!(<u64 as Value>::decode(&buf), Some(12345u64));
    }

    #[test]
    fn test_u64_value_zero() {
        let mut buf = Vec::new();
        <u64 as Value>::encode(&0u64, &mut buf);
        assert_eq!(<u64 as Value>::decode(&buf), Some(0u64));
    }

    #[test]
    fn test_u64_value_max() {
        let mut buf = Vec::new();
        <u64 as Value>::encode(&u64::MAX, &mut buf);
        assert_eq!(<u64 as Value>::decode(&buf), Some(u64::MAX));
    }

    #[test]
    fn test_u64_value_decode_too_short() {
        assert_eq!(<u64 as Value>::decode(&[0u8; 7]), None);
        assert_eq!(<u64 as Value>::decode(&[]), None);
    }

    #[test]
    fn test_u64_value_encoded_size() {
        assert_eq!(Value::encoded_size(&42u64), 8);
    }

    // ─── Length-prefixed encoding ──────────────────────────────

    #[test]
    fn test_length_prefixed_roundtrip() {
        let data = b"hello world";
        let mut buf = Vec::new();
        encode_length_prefixed(data, &mut buf);
        let (decoded, total_size) = decode_length_prefixed(&buf).unwrap();
        assert_eq!(decoded, data);
        assert_eq!(total_size, buf.len());
    }

    #[test]
    fn test_length_prefixed_empty() {
        let data: &[u8] = &[];
        let mut buf = Vec::new();
        encode_length_prefixed(data, &mut buf);
        let (decoded, total_size) = decode_length_prefixed(&buf).unwrap();
        assert!(decoded.is_empty());
        assert_eq!(total_size, 1); // 1-byte varint for length 0
    }

    #[test]
    fn test_length_prefixed_truncated_data() {
        // Encode a valid length-prefixed slice then truncate the data portion
        let data = b"hello";
        let mut buf = Vec::new();
        encode_length_prefixed(data, &mut buf);
        // Truncate so the data portion is incomplete
        buf.truncate(2);
        assert!(decode_length_prefixed(&buf).is_none());
    }

    // ─── Varint edge cases ─────────────────────────────────────

    #[test]
    fn test_varint_single_byte() {
        let mut buf = Vec::new();
        encode_varint(0, &mut buf);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], 0);

        buf.clear();
        encode_varint(127, &mut buf);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], 127);
    }

    #[test]
    fn test_varint_multi_byte() {
        let mut buf = Vec::new();
        encode_varint(128, &mut buf);
        assert!(buf.len() > 1);
        let (decoded, _) = decode_varint(&buf).unwrap();
        assert_eq!(decoded, 128);
    }

    #[test]
    fn test_varint_decode_empty() {
        assert!(decode_varint(&[]).is_none());
    }

    #[test]
    fn test_varint_decode_too_long() {
        // 5 continuation bytes (all with high bit set) means varint is too long
        let buf = [0x80, 0x80, 0x80, 0x80, 0x80];
        assert!(decode_varint(&buf).is_none());
    }

    #[test]
    fn test_varint_decode_incomplete() {
        // Single byte with continuation bit but no following byte
        let buf = [0x80];
        assert!(decode_varint(&buf).is_none());
    }

    #[test]
    fn test_varint_max_u32() {
        let mut buf = Vec::new();
        encode_varint(u32::MAX, &mut buf);
        let (decoded, _) = decode_varint(&buf).unwrap();
        assert_eq!(decoded, u32::MAX);
    }

    // ─── KeyType discriminant coverage ─────────────────────────

    #[test]
    fn test_key_type_equality() {
        assert_eq!(KeyType::U64, KeyType::U64);
        assert_ne!(KeyType::U64, KeyType::I64);
        assert_ne!(KeyType::Str, KeyType::Bytes);
    }

    #[test]
    fn test_key_type_debug() {
        // Ensure Debug is derived
        let s = format!("{:?}", KeyType::Bytes);
        assert_eq!(s, "Bytes");
    }

    #[test]
    fn test_key_type_clone() {
        let kt = KeyType::Str;
        let cloned = kt;
        assert_eq!(kt, cloned);
    }
}
