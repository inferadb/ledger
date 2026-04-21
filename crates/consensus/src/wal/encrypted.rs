//! AES-256-GCM encrypted WAL backend wrapper.
//!
//! [`EncryptedWalBackend`] wraps any [`WalBackend`] implementation, encrypting
//! frame payloads on write and decrypting on read. Per-vault data encryption
//! keys (DEKs) are obtained from a [`VaultKeyProvider`], enabling
//! crypto-shredding: destroying a vault's key makes its WAL frames
//! permanently unrecoverable.
//!
//! ## Encrypted frame payload format
//!
//! ```text
//! [vault_id:8LE][dek_version:2LE][nonce:12][ciphertext+tag:variable]
//! ```
//!
//! - `vault_id` (8 bytes, little-endian) — identifies which vault owns this frame
//! - `dek_version` (2 bytes, little-endian) — which version of the vault's DEK was used
//! - `nonce` (12 bytes) — random nonce generated via `OsRng`
//! - `ciphertext+tag` — AES-256-GCM output (encrypted plaintext + 16-byte authentication tag)

use aes_gcm::{
    AeadCore, Aes256Gcm, KeyInit,
    aead::{Aead, OsRng},
};

use crate::{
    crypto::VaultKeyProvider,
    wal_backend::{WalBackend, WalError, WalFrame},
};

/// Size of the vault_id field in the encrypted payload.
const VAULT_ID_SIZE: usize = 8;
/// Size of the dek_version field in the encrypted payload.
const DEK_VERSION_SIZE: usize = 2;
/// Size of the AES-256-GCM nonce.
const NONCE_SIZE: usize = 12;
/// Minimum size of an encrypted payload (header + nonce, no ciphertext).
const MIN_PAYLOAD_SIZE: usize = VAULT_ID_SIZE + DEK_VERSION_SIZE + NONCE_SIZE;

/// A WAL backend wrapper that encrypts frame data with AES-256-GCM.
///
/// Wraps an inner [`WalBackend`] (`W`) and uses a [`VaultKeyProvider`] (`K`)
/// to obtain per-vault encryption keys. The `shard_id` field on each frame
/// is preserved unencrypted so the inner backend can route frames correctly;
/// only the `data` payload is encrypted.
pub struct EncryptedWalBackend<W, K> {
    inner: W,
    key_provider: K,
}

impl<W, K> EncryptedWalBackend<W, K>
where
    W: WalBackend,
    K: VaultKeyProvider,
{
    /// Creates a new encrypted WAL backend wrapping `inner` with keys from
    /// `key_provider`.
    pub fn new(inner: W, key_provider: K) -> Self {
        Self { inner, key_provider }
    }

    /// Encrypts a single frame's data, returning the encrypted payload.
    fn encrypt_frame(&self, frame: &WalFrame) -> Result<Vec<u8>, WalError> {
        let vault_id = frame.shard_id.0;
        let dek_version = self.key_provider.current_version(vault_id);
        let key_bytes = self.key_provider.vault_key(vault_id, dek_version).ok_or(WalError::Io {
            kind: std::io::ErrorKind::NotFound,
            message: format!("no encryption key for vault_id={vault_id} dek_version={dek_version}"),
        })?;

        let cipher = Aes256Gcm::new_from_slice(&key_bytes).map_err(|e| WalError::Io {
            kind: std::io::ErrorKind::InvalidData,
            message: format!("failed to create AES-256-GCM cipher: {e}"),
        })?;

        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = cipher.encrypt(&nonce, &*frame.data).map_err(|e| WalError::Io {
            kind: std::io::ErrorKind::Other,
            message: format!("AES-256-GCM encryption failed: {e}"),
        })?;

        let mut payload =
            Vec::with_capacity(VAULT_ID_SIZE + DEK_VERSION_SIZE + NONCE_SIZE + ciphertext.len());
        payload.extend_from_slice(&vault_id.to_le_bytes());
        payload.extend_from_slice(&dek_version.to_le_bytes());
        payload.extend_from_slice(&nonce);
        payload.extend_from_slice(&ciphertext);

        Ok(payload)
    }

    /// Decrypts a single frame's encrypted payload, returning the plaintext data.
    fn decrypt_frame(&self, encrypted_data: &[u8]) -> Result<Vec<u8>, WalError> {
        if encrypted_data.len() < MIN_PAYLOAD_SIZE {
            return Err(WalError::Io {
                kind: std::io::ErrorKind::UnexpectedEof,
                message: format!(
                    "encrypted payload too short: {} bytes (minimum {MIN_PAYLOAD_SIZE})",
                    encrypted_data.len()
                ),
            });
        }

        let vault_id =
            u64::from_le_bytes(encrypted_data[..VAULT_ID_SIZE].try_into().map_err(|_| {
                WalError::Io {
                    kind: std::io::ErrorKind::InvalidData,
                    message: "failed to read vault_id".to_string(),
                }
            })?);
        let dek_version = u16::from_le_bytes(
            encrypted_data[VAULT_ID_SIZE..VAULT_ID_SIZE + DEK_VERSION_SIZE].try_into().map_err(
                |_| WalError::Io {
                    kind: std::io::ErrorKind::InvalidData,
                    message: "failed to read dek_version".to_string(),
                },
            )?,
        );
        let nonce_start = VAULT_ID_SIZE + DEK_VERSION_SIZE;
        let nonce_bytes = &encrypted_data[nonce_start..nonce_start + NONCE_SIZE];
        let ciphertext = &encrypted_data[nonce_start + NONCE_SIZE..];

        let key_bytes = self.key_provider.vault_key(vault_id, dek_version).ok_or(WalError::Io {
            kind: std::io::ErrorKind::NotFound,
            message: format!(
                "decryption key destroyed or missing for vault_id={vault_id} dek_version={dek_version}"
            ),
        })?;

        let cipher = Aes256Gcm::new_from_slice(&key_bytes).map_err(|e| WalError::Io {
            kind: std::io::ErrorKind::InvalidData,
            message: format!("failed to create AES-256-GCM cipher: {e}"),
        })?;

        let nonce = aes_gcm::Nonce::from_slice(nonce_bytes);
        cipher.decrypt(nonce, ciphertext).map_err(|e| WalError::Io {
            kind: std::io::ErrorKind::InvalidData,
            message: format!("AES-256-GCM decryption failed: {e}"),
        })
    }
}

impl<W, K> WalBackend for EncryptedWalBackend<W, K>
where
    W: WalBackend,
    K: VaultKeyProvider + 'static,
{
    fn append(&mut self, frames: &[WalFrame]) -> Result<(), WalError> {
        let encrypted_frames: Vec<WalFrame> = frames
            .iter()
            .map(|frame| {
                let encrypted_data = self.encrypt_frame(frame)?;
                Ok(WalFrame {
                    shard_id: frame.shard_id,
                    index: frame.index,
                    term: frame.term,
                    data: encrypted_data.into(),
                })
            })
            .collect::<Result<Vec<_>, WalError>>()?;

        self.inner.append(&encrypted_frames)
    }

    fn sync(&mut self) -> Result<(), WalError> {
        self.inner.sync()
    }

    fn read_frames(&self, from_offset: u64) -> Result<Vec<WalFrame>, WalError> {
        let encrypted_frames = self.inner.read_frames(from_offset)?;

        encrypted_frames
            .iter()
            .map(|frame| {
                let plaintext = self.decrypt_frame(&frame.data)?;
                Ok(WalFrame {
                    shard_id: frame.shard_id,
                    index: frame.index,
                    term: frame.term,
                    data: plaintext.into(),
                })
            })
            .collect()
    }

    fn truncate_before(&mut self, offset: u64) -> Result<(), WalError> {
        self.inner.truncate_before(offset)
    }

    fn shred_frames(&mut self, shard_id: crate::types::ConsensusStateId) -> Result<u64, WalError> {
        self.inner.shred_frames(shard_id)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        crypto::InMemoryKeyProvider,
        types::ConsensusStateId,
        wal::{InMemoryWalBackend, SegmentedWalBackend},
    };

    fn frame(shard: u64, data: &[u8]) -> WalFrame {
        WalFrame { shard_id: ConsensusStateId(shard), index: 0, term: 0, data: Arc::from(data) }
    }

    fn setup_provider(vault_id: u64, dek_version: u16) -> Arc<InMemoryKeyProvider> {
        let provider = Arc::new(InMemoryKeyProvider::new());
        let key = [0xAA; 32];
        provider.set_key(vault_id, dek_version, key);
        provider
    }

    // --- encrypt/decrypt roundtrip ---

    #[test]
    fn test_encrypt_decrypt_roundtrip_preserves_data() {
        let provider = setup_provider(1, 1);
        let inner = InMemoryWalBackend::new();
        let mut wal = EncryptedWalBackend::new(inner, Arc::clone(&provider));

        let original_frames =
            vec![frame(1, b"hello world"), frame(1, b"second entry"), frame(1, b"")];

        wal.append(&original_frames).unwrap();
        wal.sync().unwrap();

        let read_back = wal.read_frames(0).unwrap();
        assert_eq!(read_back.len(), 3);
        assert_eq!(&*read_back[0].data, b"hello world");
        assert_eq!(read_back[0].shard_id, ConsensusStateId(1));
        assert_eq!(&*read_back[1].data, b"second entry");
        assert_eq!(&*read_back[2].data, b"");
    }

    #[test]
    fn test_encrypt_different_vaults_use_different_keys() {
        let provider = Arc::new(InMemoryKeyProvider::new());
        provider.set_key(1, 1, [0x11; 32]);
        provider.set_key(2, 1, [0x22; 32]);

        let inner = InMemoryWalBackend::new();
        let mut wal = EncryptedWalBackend::new(inner, Arc::clone(&provider));

        wal.append(&[frame(1, b"vault-one"), frame(2, b"vault-two")]).unwrap();
        wal.sync().unwrap();

        let read_back = wal.read_frames(0).unwrap();
        assert_eq!(read_back.len(), 2);
        assert_eq!(&*read_back[0].data, b"vault-one");
        assert_eq!(&*read_back[1].data, b"vault-two");
    }

    // --- crypto-shredding ---

    #[test]
    fn test_decrypt_destroyed_key_returns_error() {
        let provider = Arc::new(InMemoryKeyProvider::new());
        provider.set_key(1, 1, [0xBB; 32]);

        let inner = InMemoryWalBackend::new();
        let mut wal = EncryptedWalBackend::new(inner, Arc::clone(&provider));

        wal.append(&[frame(1, b"sensitive data")]).unwrap();
        wal.sync().unwrap();

        provider.destroy_key(1, 1);

        let err = wal.read_frames(0).unwrap_err();
        assert!(err.to_string().contains("destroyed or missing"));
    }

    // --- missing key on encrypt ---

    #[test]
    fn test_encrypt_missing_key_returns_error() {
        let provider = Arc::new(InMemoryKeyProvider::new());
        // No key registered for vault 1.
        let inner = InMemoryWalBackend::new();
        let mut wal = EncryptedWalBackend::new(inner, Arc::clone(&provider));

        let err = wal.append(&[frame(1, b"no key")]).unwrap_err();
        assert!(err.to_string().contains("no encryption key"));
    }

    // --- inner data is encrypted, not plaintext ---

    #[test]
    fn test_inner_backend_stores_encrypted_not_plaintext() {
        let provider = setup_provider(1, 1);
        let inner = InMemoryWalBackend::new();
        let mut wal = EncryptedWalBackend::new(inner, Arc::clone(&provider));

        wal.append(&[frame(1, b"plaintext secret")]).unwrap();
        wal.sync().unwrap();

        // Read from the inner backend directly — data must differ from plaintext.
        let inner_frames = wal.inner.read_frames(0).unwrap();
        assert_eq!(inner_frames.len(), 1);
        assert_ne!(&*inner_frames[0].data, b"plaintext secret");
        // Encrypted payload is larger than plaintext (header + nonce + tag overhead).
        assert!(inner_frames[0].data.len() > b"plaintext secret".len());
    }

    // --- segmented backend integration ---

    #[test]
    fn test_encrypt_with_segmented_backend_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let provider = setup_provider(1, 1);
        let inner = SegmentedWalBackend::open(dir.path()).unwrap();
        let mut wal = EncryptedWalBackend::new(inner, Arc::clone(&provider));

        wal.append(&[frame(1, b"persistent encrypted"), frame(1, b"another frame")]).unwrap();
        wal.sync().unwrap();

        let read_back = wal.read_frames(0).unwrap();
        assert_eq!(read_back.len(), 2);
        assert_eq!(&*read_back[0].data, b"persistent encrypted");
        assert_eq!(&*read_back[1].data, b"another frame");
    }

    // --- truncated encrypted payload ---

    #[test]
    fn test_decrypt_truncated_payload_returns_error() {
        let provider = setup_provider(1, 1);
        let mut inner = InMemoryWalBackend::new();

        // Manually insert a frame with data shorter than MIN_PAYLOAD_SIZE.
        let short_payload = vec![0u8; 5];
        let bad_frame =
            WalFrame { shard_id: ConsensusStateId(1), index: 0, term: 0, data: short_payload.into() };
        inner.append(&[bad_frame]).unwrap();
        inner.sync().unwrap();

        let wal = EncryptedWalBackend::new(inner, Arc::clone(&provider));
        let err = wal.read_frames(0).unwrap_err();
        assert!(err.to_string().contains("too short"));
    }
}
