//! Key encoding for storage layer.
//!
//! All keys are prefixed with vault_id and bucket_id to enable:
//! - Efficient vault-scoped queries (prefix scan by vault_id)
//! - Bucket-based state hashing (prefix scan by vault_id + bucket_id)
//!
//! Key format: {vault_id:8BE}{bucket_id:1}{local_key:var}

use inferadb_ledger_types::{VaultId, bucket_id};

/// Decoded storage key components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageKey {
    /// Vault containing this key.
    pub vault_id: VaultId,
    /// Bucket assignment (0-255).
    pub bucket_id: u8,
    /// Local key within the vault.
    pub local_key: Vec<u8>,
}

/// Encodes a storage key with vault and bucket prefixes.
///
/// * `vault` - Internal vault identifier (`VaultId`).
///
/// Format: {vault_id:8BE}{bucket_id:1}{local_key:var}
///
/// The bucket_id is computed from local_key using seahash % 256.
/// Using big-endian for vault_id ensures lexicographic ordering by vault.
pub fn encode_storage_key(vault: VaultId, local_key: &[u8]) -> Vec<u8> {
    let bucket = bucket_id(local_key);
    let mut key = Vec::with_capacity(9 + local_key.len());
    key.extend_from_slice(&vault.value().to_be_bytes());
    key.push(bucket);
    key.extend_from_slice(local_key);
    key
}

/// Creates a prefix for scanning all keys in a vault.
///
/// * `vault` - Internal vault identifier (`VaultId`).
pub fn vault_prefix(vault: VaultId) -> [u8; 8] {
    vault.value().to_be_bytes()
}

/// Creates a prefix for scanning all keys in a specific bucket within a vault.
///
/// * `vault` - Internal vault identifier (`VaultId`).
pub fn bucket_prefix(vault: VaultId, bucket_id: u8) -> [u8; 9] {
    let mut prefix = [0u8; 9];
    prefix[..8].copy_from_slice(&vault.value().to_be_bytes());
    prefix[8] = bucket_id;
    prefix
}

/// Decodes a storage key into its components.
///
/// Returns None if the key is too short.
pub fn decode_storage_key(key: &[u8]) -> Option<StorageKey> {
    if key.len() < 9 {
        return None;
    }

    let vault_id = i64::from_be_bytes(key[..8].try_into().ok()?);
    let bucket_id = key[8];
    let local_key = key[9..].to_vec();

    Some(StorageKey { vault_id: VaultId::new(vault_id), bucket_id, local_key })
}

/// Encodes an object index key.
///
/// Format: obj_idx:{resource}#{relation}
pub fn encode_obj_index_key(resource: &str, relation: &str) -> Vec<u8> {
    format!("obj_idx:{}#{}", resource, relation).into_bytes()
}

/// Encodes a subject index key.
///
/// Format: subj_idx:{subject}
pub fn encode_subj_index_key(subject: &str) -> Vec<u8> {
    format!("subj_idx:{}", subject).into_bytes()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let vault_id = VaultId::new(12345);
        let local_key = b"user:alice";

        let encoded = encode_storage_key(vault_id, local_key);
        let decoded = decode_storage_key(&encoded).expect("should decode");

        assert_eq!(decoded.vault_id, vault_id);
        assert_eq!(decoded.bucket_id, bucket_id(local_key));
        assert_eq!(decoded.local_key, local_key);
    }

    #[test]
    fn test_key_ordering() {
        // Keys should be ordered by vault_id first (big-endian)
        let key1 = encode_storage_key(VaultId::new(1), b"z");
        let key2 = encode_storage_key(VaultId::new(2), b"a");

        assert!(key1 < key2, "vault 1 < vault 2");
    }

    #[test]
    fn test_index_key_formats() {
        let obj_key = encode_obj_index_key("doc:123", "viewer");
        assert_eq!(obj_key, b"obj_idx:doc:123#viewer");

        let subj_key = encode_subj_index_key("user:alice");
        assert_eq!(subj_key, b"subj_idx:user:alice");
    }

    #[test]
    fn test_decode_too_short() {
        assert!(decode_storage_key(&[0u8; 8]).is_none());
        assert!(decode_storage_key(&[0u8; 7]).is_none());
    }

    #[test]
    fn test_deterministic_bucket_assignment() {
        // Same key should always get same bucket
        let key = b"consistent_key";
        let b1 = bucket_id(key);
        let b2 = bucket_id(key);
        assert_eq!(b1, b2);
    }
}
