//! Snapshot file encryption and decryption using AES-256-GCM.
//!
//! Snapshots are encrypted with a per-region key before writing to disk
//! or transferring to followers, protecting multi-tenant data at rest.
//!
//! ## Wire format
//!
//! ```text
//! [nonce:12][ciphertext+tag:variable]
//! ```
//!
//! The nonce is randomly generated per encryption. The ciphertext includes
//! the AES-256-GCM authentication tag (16 bytes appended).

use aes_gcm::{Aes256Gcm, KeyInit, Nonce, aead::Aead};

/// Encrypts snapshot data using AES-256-GCM.
///
/// Returns the nonce (12 bytes) prepended to the ciphertext. The nonce is
/// randomly generated for each call via the system CSPRNG.
///
/// # Errors
///
/// Returns [`SnapshotCryptoError::Encryption`] if encryption fails.
pub fn encrypt_snapshot(key: &[u8; 32], plaintext: &[u8]) -> Result<Vec<u8>, SnapshotCryptoError> {
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| SnapshotCryptoError::Encryption { message: e.to_string() })?;

    let nonce_bytes: [u8; 12] = rand::random();
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| SnapshotCryptoError::Encryption { message: e.to_string() })?;

    let mut result = Vec::with_capacity(12 + ciphertext.len());
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&ciphertext);
    Ok(result)
}

/// Decrypts snapshot data encrypted by [`encrypt_snapshot`].
///
/// The input must be at least 12 bytes (nonce) plus the ciphertext.
///
/// # Errors
///
/// Returns [`SnapshotCryptoError::Decryption`] if the data is too short,
/// the key is wrong, or the authentication tag does not match.
pub fn decrypt_snapshot(key: &[u8; 32], encrypted: &[u8]) -> Result<Vec<u8>, SnapshotCryptoError> {
    if encrypted.len() < 12 {
        return Err(SnapshotCryptoError::Decryption {
            message: "encrypted data too short for nonce".to_string(),
        });
    }

    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| SnapshotCryptoError::Decryption { message: e.to_string() })?;

    let nonce = Nonce::from_slice(&encrypted[..12]);
    let ciphertext = &encrypted[12..];

    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| SnapshotCryptoError::Decryption { message: e.to_string() })
}

/// Errors from snapshot encryption or decryption.
#[derive(Debug, snafu::Snafu)]
pub enum SnapshotCryptoError {
    /// Snapshot encryption failed.
    #[snafu(display("Snapshot encryption failed: {message}"))]
    Encryption {
        /// Description of the failure.
        message: String,
    },
    /// Snapshot decryption failed.
    #[snafu(display("Snapshot decryption failed: {message}"))]
    Decryption {
        /// Description of the failure.
        message: String,
    },
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let key = [0xAB; 32];
        let plaintext = b"snapshot data for testing";

        let encrypted = encrypt_snapshot(&key, plaintext).unwrap();
        let decrypted = decrypt_snapshot(&key, &encrypted).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn empty_plaintext_roundtrip() {
        let key = [0x01; 32];
        let encrypted = encrypt_snapshot(&key, b"").unwrap();
        let decrypted = decrypt_snapshot(&key, &encrypted).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn wrong_key_fails_decryption() {
        let key = [0xAA; 32];
        let wrong_key = [0xBB; 32];

        let encrypted = encrypt_snapshot(&key, b"sensitive snapshot data").unwrap();
        let err = decrypt_snapshot(&wrong_key, &encrypted).unwrap_err();

        assert!(err.to_string().contains("decryption failed"));
    }

    #[test]
    fn decryption_rejects_inputs_shorter_than_nonce() {
        let key = [0xCC; 32];
        let short_inputs: &[&[u8]] = &[&[], &[0u8; 1], &[0u8; 11]];
        for input in short_inputs {
            let err = decrypt_snapshot(&key, input).unwrap_err();
            assert!(
                err.to_string().contains("too short for nonce"),
                "input len={}: {err}",
                input.len()
            );
        }
    }

    #[test]
    fn exactly_twelve_bytes_fails_as_empty_ciphertext() {
        let key = [0xDD; 32];
        // 12 bytes is just a nonce with no ciphertext — GCM needs at least the tag.
        let result = decrypt_snapshot(&key, &[0u8; 12]);
        assert!(result.is_err());
    }

    #[test]
    fn each_encryption_produces_unique_nonce() {
        let key = [0xDE; 32];
        let plaintext = b"same plaintext";

        let enc1 = encrypt_snapshot(&key, plaintext).unwrap();
        let enc2 = encrypt_snapshot(&key, plaintext).unwrap();

        // Nonces (first 12 bytes) must differ.
        assert_ne!(&enc1[..12], &enc2[..12]);

        // Both must decrypt to the original.
        assert_eq!(decrypt_snapshot(&key, &enc1).unwrap(), plaintext);
        assert_eq!(decrypt_snapshot(&key, &enc2).unwrap(), plaintext);
    }

    #[test]
    fn encrypted_output_size_is_nonce_plus_plaintext_plus_tag() {
        let key = [0xFF; 32];
        let plaintext = b"test data";

        let encrypted = encrypt_snapshot(&key, plaintext).unwrap();
        assert_eq!(encrypted.len(), 12 + plaintext.len() + 16);
    }

    #[test]
    fn tampered_ciphertext_fails_authentication() {
        let key = [0xEE; 32];
        let mut encrypted = encrypt_snapshot(&key, b"authentic data").unwrap();

        // Flip a bit in the ciphertext (after the 12-byte nonce).
        let idx = 12 + encrypted.len().saturating_sub(13).min(1);
        encrypted[idx] ^= 0x01;

        let result = decrypt_snapshot(&key, &encrypted);
        assert!(result.is_err(), "tampered ciphertext must fail authentication");
    }
}
