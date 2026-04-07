//! Binary key encoding for relationship storage.
//!
//! Provides compact, lexicographically sortable binary key formats for
//! relationship tuples and their indexes. Type IDs use 2-byte big-endian
//! encoding to preserve sort order; length fields use 2-byte little-endian
//! (framing only, not for ordering).
//!
//! # Key formats
//!
//! **Relationship / object-index local key:**
//! `[res_type:2BE][res_id_len:2LE][res_id][relation:2BE][subj_type:2BE][subj_id_len:2LE][subj_id]`
//!
//! **Object-index prefix:**
//! `[res_type:2BE][res_id_len:2LE][res_id][relation:2BE]`
//!
//! **Subject-index local key:**
//! `[subj_type:2BE][subj_id_len:2LE][subj_id][res_type:2BE][res_id_len:2LE][res_id][relation:2BE]`
//!
//! **Subject-index prefix:**
//! `[subj_type:2BE][subj_id_len:2LE][subj_id]`

/// Category of interned string for the dictionary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum InternCategory {
    /// Relation names (e.g. "owner", "viewer").
    Relation = 0,
    /// Resource type names (e.g. "doc", "folder").
    ResourceType = 1,
    /// Subject type names (e.g. "user", "group").
    SubjectType = 2,
}

/// Interned identifier assigned by the dictionary to a string within a category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InternId(pub u16);

/// Encode a relationship local key.
///
/// Format: `[res_type:2BE][res_id_len:2LE][res_id][relation:2BE][subj_type:2BE][subj_id_len:
/// 2LE][subj_id]`
pub fn encode_relationship_local_key(
    resource_type: InternId,
    resource_id: &[u8],
    relation: InternId,
    subject_type: InternId,
    subject_id: &[u8],
) -> Vec<u8> {
    let res_id_len = resource_id.len() as u16;
    let subj_id_len = subject_id.len() as u16;
    let capacity = 2 + 2 + resource_id.len() + 2 + 2 + 2 + subject_id.len();
    let mut buf = Vec::with_capacity(capacity);
    buf.extend_from_slice(&resource_type.0.to_be_bytes());
    buf.extend_from_slice(&res_id_len.to_le_bytes());
    buf.extend_from_slice(resource_id);
    buf.extend_from_slice(&relation.0.to_be_bytes());
    buf.extend_from_slice(&subject_type.0.to_be_bytes());
    buf.extend_from_slice(&subj_id_len.to_le_bytes());
    buf.extend_from_slice(subject_id);
    buf
}

/// Decode a relationship local key.
///
/// Returns `(resource_type, resource_id, relation, subject_type, subject_id)`,
/// or `None` if the key is truncated or malformed.
pub fn decode_relationship_local_key(
    key: &[u8],
) -> Option<(InternId, &[u8], InternId, InternId, &[u8])> {
    if key.len() < 2 {
        return None;
    }
    let resource_type = InternId(u16::from_be_bytes([key[0], key[1]]));
    let key = &key[2..];

    if key.len() < 2 {
        return None;
    }
    let res_id_len = u16::from_le_bytes([key[0], key[1]]) as usize;
    let key = &key[2..];

    if key.len() < res_id_len {
        return None;
    }
    let resource_id = &key[..res_id_len];
    let key = &key[res_id_len..];

    if key.len() < 2 {
        return None;
    }
    let relation = InternId(u16::from_be_bytes([key[0], key[1]]));
    let key = &key[2..];

    if key.len() < 2 {
        return None;
    }
    let subject_type = InternId(u16::from_be_bytes([key[0], key[1]]));
    let key = &key[2..];

    if key.len() < 2 {
        return None;
    }
    let subj_id_len = u16::from_le_bytes([key[0], key[1]]) as usize;
    let key = &key[2..];

    if key.len() < subj_id_len {
        return None;
    }
    let subject_id = &key[..subj_id_len];

    // Reject trailing bytes — exact match only.
    if key.len() != subj_id_len {
        return None;
    }

    Some((resource_type, resource_id, relation, subject_type, subject_id))
}

/// Encode an object-index local key.
///
/// The object index uses the same binary layout as the relationship local key.
#[inline]
pub fn encode_obj_index_local_key(
    resource_type: InternId,
    resource_id: &[u8],
    relation: InternId,
    subject_type: InternId,
    subject_id: &[u8],
) -> Vec<u8> {
    encode_relationship_local_key(resource_type, resource_id, relation, subject_type, subject_id)
}

/// Encode an object-index prefix for scanning.
///
/// Format: `[res_type:2BE][res_id_len:2LE][res_id][relation:2BE]`
pub fn encode_obj_index_prefix(
    resource_type: InternId,
    resource_id: &[u8],
    relation: InternId,
) -> Vec<u8> {
    let res_id_len = resource_id.len() as u16;
    let capacity = 2 + 2 + resource_id.len() + 2;
    let mut buf = Vec::with_capacity(capacity);
    buf.extend_from_slice(&resource_type.0.to_be_bytes());
    buf.extend_from_slice(&res_id_len.to_le_bytes());
    buf.extend_from_slice(resource_id);
    buf.extend_from_slice(&relation.0.to_be_bytes());
    buf
}

/// Encode a subject-index local key.
///
/// Format: `[subj_type:2BE][subj_id_len:2LE][subj_id][res_type:2BE][res_id_len:
/// 2LE][res_id][relation:2BE]`
pub fn encode_subj_index_local_key(
    subject_type: InternId,
    subject_id: &[u8],
    resource_type: InternId,
    resource_id: &[u8],
    relation: InternId,
) -> Vec<u8> {
    let subj_id_len = subject_id.len() as u16;
    let res_id_len = resource_id.len() as u16;
    let capacity = 2 + 2 + subject_id.len() + 2 + 2 + resource_id.len() + 2;
    let mut buf = Vec::with_capacity(capacity);
    buf.extend_from_slice(&subject_type.0.to_be_bytes());
    buf.extend_from_slice(&subj_id_len.to_le_bytes());
    buf.extend_from_slice(subject_id);
    buf.extend_from_slice(&resource_type.0.to_be_bytes());
    buf.extend_from_slice(&res_id_len.to_le_bytes());
    buf.extend_from_slice(resource_id);
    buf.extend_from_slice(&relation.0.to_be_bytes());
    buf
}

/// Decode a subject-index local key.
///
/// Returns `(subject_type, subject_id, resource_type, resource_id, relation)`,
/// or `None` if the key is truncated or malformed.
pub fn decode_subj_index_local_key(
    key: &[u8],
) -> Option<(InternId, &[u8], InternId, &[u8], InternId)> {
    if key.len() < 2 {
        return None;
    }
    let subject_type = InternId(u16::from_be_bytes([key[0], key[1]]));
    let key = &key[2..];

    if key.len() < 2 {
        return None;
    }
    let subj_id_len = u16::from_le_bytes([key[0], key[1]]) as usize;
    let key = &key[2..];

    if key.len() < subj_id_len {
        return None;
    }
    let subject_id = &key[..subj_id_len];
    let key = &key[subj_id_len..];

    if key.len() < 2 {
        return None;
    }
    let resource_type = InternId(u16::from_be_bytes([key[0], key[1]]));
    let key = &key[2..];

    if key.len() < 2 {
        return None;
    }
    let res_id_len = u16::from_le_bytes([key[0], key[1]]) as usize;
    let key = &key[2..];

    if key.len() < res_id_len {
        return None;
    }
    let resource_id = &key[..res_id_len];
    let key = &key[res_id_len..];

    if key.len() < 2 {
        return None;
    }
    let relation = InternId(u16::from_be_bytes([key[0], key[1]]));
    let key = &key[2..];

    // Reject trailing bytes.
    if !key.is_empty() {
        return None;
    }

    Some((subject_type, subject_id, resource_type, resource_id, relation))
}

/// Encode a subject-index prefix for scanning.
///
/// Format: `[subj_type:2BE][subj_id_len:2LE][subj_id]`
pub fn encode_subj_index_prefix(subject_type: InternId, subject_id: &[u8]) -> Vec<u8> {
    let subj_id_len = subject_id.len() as u16;
    let capacity = 2 + 2 + subject_id.len();
    let mut buf = Vec::with_capacity(capacity);
    buf.extend_from_slice(&subject_type.0.to_be_bytes());
    buf.extend_from_slice(&subj_id_len.to_le_bytes());
    buf.extend_from_slice(subject_id);
    buf
}

/// Split a typed ID string on the first colon.
///
/// ```no_run
/// # use inferadb_ledger_state::binary_keys::split_typed_id;
/// assert_eq!(split_typed_id("doc:123"), Some(("doc", "123")));
/// assert_eq!(split_typed_id("no_colon"), None);
/// ```
pub fn split_typed_id(s: &str) -> Option<(&str, &str)> {
    let idx = s.find(':')?;
    Some((&s[..idx], &s[idx + 1..]))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_relationship_key_roundtrip() {
        let res_type = InternId(1);
        let res_id = b"abc";
        let relation = InternId(5);
        let subj_type = InternId(2);
        let subj_id = b"xyz";

        let key = encode_relationship_local_key(res_type, res_id, relation, subj_type, subj_id);
        let (rt, rid, rel, st, sid) = decode_relationship_local_key(&key).expect("decode failed");

        assert_eq!(rt, res_type);
        assert_eq!(rid, &res_id[..]);
        assert_eq!(rel, relation);
        assert_eq!(st, subj_type);
        assert_eq!(sid, &subj_id[..]);
    }

    #[test]
    fn lexicographic_ordering_by_resource_type() {
        let key_a =
            encode_relationship_local_key(InternId(1), b"x", InternId(0), InternId(0), b"y");
        let key_b =
            encode_relationship_local_key(InternId(2), b"x", InternId(0), InternId(0), b"y");
        assert!(key_a < key_b);
    }

    #[test]
    fn lexicographic_ordering_by_resource_id() {
        // Same resource type, different resource IDs — lexicographic order
        // of the full key should reflect resource_id order when lengths match.
        let key_a =
            encode_relationship_local_key(InternId(1), b"aaa", InternId(0), InternId(0), b"z");
        let key_b =
            encode_relationship_local_key(InternId(1), b"bbb", InternId(0), InternId(0), b"z");
        assert!(key_a < key_b);
    }

    #[test]
    fn obj_index_key_shares_prefix_with_relationship_key() {
        let res_type = InternId(3);
        let res_id = b"doc-42";
        let relation = InternId(7);
        let subj_type = InternId(9);
        let subj_id = b"user-1";

        let rel_key = encode_relationship_local_key(res_type, res_id, relation, subj_type, subj_id);
        let obj_key = encode_obj_index_local_key(res_type, res_id, relation, subj_type, subj_id);
        assert_eq!(rel_key, obj_key);
    }

    #[test]
    fn obj_index_prefix_matches_full_key() {
        let res_type = InternId(3);
        let res_id = b"doc-42";
        let relation = InternId(7);
        let subj_type = InternId(9);
        let subj_id = b"user-1";

        let full_key = encode_obj_index_local_key(res_type, res_id, relation, subj_type, subj_id);
        let prefix = encode_obj_index_prefix(res_type, res_id, relation);
        assert!(full_key.starts_with(&prefix));
    }

    #[test]
    fn subj_index_prefix_matches_full_key() {
        let subj_type = InternId(2);
        let subj_id = b"user-1";
        let res_type = InternId(3);
        let res_id = b"doc-42";
        let relation = InternId(7);

        let full_key = encode_subj_index_local_key(subj_type, subj_id, res_type, res_id, relation);
        let prefix = encode_subj_index_prefix(subj_type, subj_id);
        assert!(full_key.starts_with(&prefix));
    }

    #[test]
    fn decode_subj_index_key_roundtrip() {
        let subj_type = InternId(10);
        let subj_id = b"group-7";
        let res_type = InternId(4);
        let res_id = b"folder-99";
        let relation = InternId(12);

        let key = encode_subj_index_local_key(subj_type, subj_id, res_type, res_id, relation);
        let (st, sid, rt, rid, rel) = decode_subj_index_local_key(&key).expect("decode failed");

        assert_eq!(st, subj_type);
        assert_eq!(sid, &subj_id[..]);
        assert_eq!(rt, res_type);
        assert_eq!(rid, &res_id[..]);
        assert_eq!(rel, relation);
    }

    #[test]
    fn split_typed_id_basic() {
        assert_eq!(split_typed_id("doc:123"), Some(("doc", "123")));
    }

    #[test]
    fn split_typed_id_no_colon() {
        assert_eq!(split_typed_id("nocolon"), None);
    }

    #[test]
    fn split_typed_id_multiple_colons() {
        assert_eq!(split_typed_id("a:b:c"), Some(("a", "b:c")));
    }

    #[test]
    fn split_typed_id_empty_local_id() {
        assert_eq!(split_typed_id("type:"), Some(("type", "")));
    }

    #[test]
    fn empty_local_id_encodes() {
        let key = encode_relationship_local_key(InternId(1), b"", InternId(2), InternId(3), b"");
        let (rt, rid, rel, st, sid) = decode_relationship_local_key(&key).expect("decode failed");
        assert_eq!(rt, InternId(1));
        assert!(rid.is_empty());
        assert_eq!(rel, InternId(2));
        assert_eq!(st, InternId(3));
        assert!(sid.is_empty());
    }

    #[test]
    fn max_length_local_id() {
        let big_id = vec![0xAB_u8; 65535];
        let key =
            encode_relationship_local_key(InternId(1), &big_id, InternId(2), InternId(3), &big_id);
        let (rt, rid, rel, st, sid) = decode_relationship_local_key(&key).expect("decode failed");
        assert_eq!(rt, InternId(1));
        assert_eq!(rid.len(), 65535);
        assert_eq!(rel, InternId(2));
        assert_eq!(st, InternId(3));
        assert_eq!(sid.len(), 65535);
    }

    #[test]
    fn decode_truncated_key_returns_none() {
        // A valid key with content, then truncated at various points.
        let full =
            encode_relationship_local_key(InternId(1), b"abc", InternId(2), InternId(3), b"xyz");
        // Truncate at every position shorter than the full key.
        for len in 0..full.len() {
            assert!(
                decode_relationship_local_key(&full[..len]).is_none(),
                "expected None for truncated key of length {len}"
            );
        }
        // Also test subject index decode with truncation.
        let full_subj =
            encode_subj_index_local_key(InternId(1), b"abc", InternId(2), b"xyz", InternId(3));
        for len in 0..full_subj.len() {
            assert!(
                decode_subj_index_local_key(&full_subj[..len]).is_none(),
                "expected None for truncated subj key of length {len}"
            );
        }
    }
}
