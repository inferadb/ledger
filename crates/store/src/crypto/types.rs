//! Cryptographic types for envelope encryption.

use zeroize::{Zeroize, ZeroizeOnDrop};

/// Size of a single [`CryptoMetadata`] entry in bytes.
///
/// Layout: `rmk_version(4) + wrapped_dek(40) + nonce(12) + auth_tag(16) = 72`.
pub const CRYPTO_METADATA_SIZE: usize = 4 + 40 + 12 + 16;

/// 32-byte AES-256 Data Encryption Key.
///
/// Automatically zeroized on drop. Each DEK encrypts exactly one artifact
/// (one-DEK-one-encrypt invariant).
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct DataEncryptionKey {
    key: [u8; 32],
}

impl DataEncryptionKey {
    /// Creates a DEK from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self { key: bytes }
    }

    /// Returns the raw key bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.key
    }
}

impl std::fmt::Debug for DataEncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataEncryptionKey").field("key", &"[REDACTED]").finish()
    }
}

/// AES-KWP wrapped DEK (32-byte key + 8-byte integrity = 40 bytes).
///
/// Nonce-free wrapping eliminates nonce-reuse risks at the key
/// wrapping layer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WrappedDek {
    ciphertext: [u8; 40],
}

impl WrappedDek {
    /// Creates a wrapped DEK from raw bytes.
    pub fn from_bytes(bytes: [u8; 40]) -> Self {
        Self { ciphertext: bytes }
    }

    /// Returns the raw wrapped bytes.
    pub fn as_bytes(&self) -> &[u8; 40] {
        &self.ciphertext
    }
}

/// Region Master Key for wrapping/unwrapping DEKs.
///
/// Automatically zeroized on drop. Multiple RMK versions coexist
/// at runtime for rotation without downtime.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct RegionMasterKey {
    /// RMK version number (monotonically increasing).
    #[zeroize(skip)]
    pub version: u32,
    /// Raw 256-bit key material.
    key: [u8; 32],
}

impl RegionMasterKey {
    /// Creates an RMK from version and raw bytes.
    pub fn new(version: u32, key: [u8; 32]) -> Self {
        Self { version, key }
    }

    /// Returns the raw key bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.key
    }
}

impl std::fmt::Debug for RegionMasterKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionMasterKey")
            .field("version", &self.version)
            .field("key", &"[REDACTED]")
            .finish()
    }
}

/// Per-page cryptographic metadata stored in the sidecar.
///
/// Contains everything needed to decrypt a page: the RMK version
/// (to look up the correct RMK), the wrapped DEK, the GCM nonce,
/// and the GCM authentication tag.
#[derive(Debug, Clone)]
pub struct CryptoMetadata {
    /// RMK version used to wrap the DEK.
    pub rmk_version: u32,
    /// AES-KWP wrapped DEK.
    pub wrapped_dek: WrappedDek,
    /// 12-byte random nonce for AES-256-GCM.
    pub nonce: [u8; 12],
    /// 16-byte GCM authentication tag.
    pub auth_tag: [u8; 16],
}

impl CryptoMetadata {
    /// Serializes to a fixed-size byte array for sidecar storage.
    pub fn to_bytes(&self) -> [u8; CRYPTO_METADATA_SIZE] {
        let mut buf = [0u8; CRYPTO_METADATA_SIZE];
        buf[0..4].copy_from_slice(&self.rmk_version.to_le_bytes());
        buf[4..44].copy_from_slice(self.wrapped_dek.as_bytes());
        buf[44..56].copy_from_slice(&self.nonce);
        buf[56..72].copy_from_slice(&self.auth_tag);
        buf
    }

    /// Deserializes from a fixed-size byte array.
    ///
    /// Returns `None` if the buffer is all zeros (unwritten page).
    pub fn from_bytes(buf: &[u8; CRYPTO_METADATA_SIZE]) -> Option<Self> {
        // All-zero entry means unencrypted/unwritten page
        if buf.iter().all(|&b| b == 0) {
            return None;
        }
        let rmk_version = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let mut wrapped_dek_bytes = [0u8; 40];
        wrapped_dek_bytes.copy_from_slice(&buf[4..44]);
        let mut nonce = [0u8; 12];
        nonce.copy_from_slice(&buf[44..56]);
        let mut auth_tag = [0u8; 16];
        auth_tag.copy_from_slice(&buf[56..72]);
        Some(Self {
            rmk_version,
            wrapped_dek: WrappedDek::from_bytes(wrapped_dek_bytes),
            nonce,
            auth_tag,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_dek_debug_redacts() {
        let dek = DataEncryptionKey::from_bytes([0x42; 32]);
        let debug = format!("{dek:?}");
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains("42"));
    }

    #[test]
    fn test_rmk_debug_redacts() {
        let rmk = RegionMasterKey::new(1, [0x42; 32]);
        let debug = format!("{rmk:?}");
        assert!(debug.contains("REDACTED"));
        assert!(debug.contains("version: 1"));
        assert!(!debug.contains("42"));
    }

    #[test]
    fn test_wrapped_dek_equality() {
        let a = WrappedDek::from_bytes([0xAA; 40]);
        let b = WrappedDek::from_bytes([0xAA; 40]);
        let c = WrappedDek::from_bytes([0xBB; 40]);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_crypto_metadata_roundtrip() {
        let meta = CryptoMetadata {
            rmk_version: 42,
            wrapped_dek: WrappedDek::from_bytes([0xAA; 40]),
            nonce: [0xBB; 12],
            auth_tag: [0xCC; 16],
        };

        let bytes = meta.to_bytes();
        assert_eq!(bytes.len(), CRYPTO_METADATA_SIZE);

        let restored = CryptoMetadata::from_bytes(&bytes).unwrap();
        assert_eq!(restored.rmk_version, 42);
        assert_eq!(restored.wrapped_dek, WrappedDek::from_bytes([0xAA; 40]));
        assert_eq!(restored.nonce, [0xBB; 12]);
        assert_eq!(restored.auth_tag, [0xCC; 16]);
    }

    #[test]
    fn test_crypto_metadata_all_zeros_returns_none() {
        let buf = [0u8; CRYPTO_METADATA_SIZE];
        assert!(CryptoMetadata::from_bytes(&buf).is_none());
    }

    #[test]
    fn test_crypto_metadata_size_constant() {
        // 4 (rmk_version) + 40 (wrapped_dek) + 12 (nonce) + 16 (auth_tag) = 72
        assert_eq!(CRYPTO_METADATA_SIZE, 72);
    }
}
