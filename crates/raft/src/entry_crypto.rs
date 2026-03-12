//! Per-entry encryption for PII-bearing Raft log entries.
//!
//! Provides AES-256-GCM encryption of [`SystemRequest`] payloads using
//! per-user `SubjectKey` material. When a user is erased, their
//! `SubjectKey` is destroyed — rendering all historical Raft log entries
//! containing that user's PII cryptographically unrecoverable (crypto-shredding).
//!
//! Only user-scoped REGIONAL requests are encrypted. Organization, team,
//! and app profile writes are protected by regional Raft isolation and
//! do not carry user PII.

use aes_gcm::{
    Aes256Gcm, KeyInit, Nonce,
    aead::{Aead, Payload},
};
use inferadb_ledger_types::UserId;
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::types::SystemRequest;

/// Encrypted form of a [`SystemRequest`] for at-rest PII protection.
///
/// The plaintext `SystemRequest` is postcard-serialized, then encrypted with
/// AES-256-GCM using the user's `SubjectKey` as the encryption key. The
/// `user_id` is stored in plaintext so the decryption path can look up
/// the correct key.
///
/// AAD (additional authenticated data) binds the ciphertext to the specific
/// user, preventing key-substitution attacks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncryptedSystemRequest {
    /// AES-256-GCM sealed output: `ciphertext || auth_tag` (16-byte tag appended).
    pub sealed: Vec<u8>,
    /// 12-byte random nonce (unique per encryption).
    pub nonce: [u8; 12],
    /// User whose `SubjectKey` encrypts this entry.
    ///
    /// Stored in plaintext for key lookup during decryption.
    /// Not sensitive — user IDs are internal sequential identifiers.
    pub user_id: UserId,
}

/// Encrypts a [`SystemRequest`] using a user's `SubjectKey` material.
///
/// The request is postcard-serialized, then encrypted with AES-256-GCM.
/// The user ID is bound as AAD to prevent cross-user key substitution.
///
/// # Errors
///
/// Returns an error if serialization or encryption fails.
pub fn encrypt_system_request(
    request: &SystemRequest,
    subject_key_bytes: &[u8; 32],
    user_id: UserId,
) -> Result<EncryptedSystemRequest, EncryptionError> {
    let plaintext = postcard::to_allocvec(request)
        .map_err(|e| EncryptionError::Serialization(e.to_string()))?;

    let cipher = Aes256Gcm::new(subject_key_bytes.into());

    let mut nonce_bytes = [0u8; 12];
    rand::rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    // Bind ciphertext to user_id via AAD
    let aad = user_id.value().to_le_bytes();
    let payload = Payload { msg: &plaintext, aad: &aad };

    let sealed = cipher.encrypt(nonce, payload).map_err(|_| EncryptionError::Encryption)?;

    Ok(EncryptedSystemRequest { sealed, nonce: nonce_bytes, user_id })
}

/// Decrypts an [`EncryptedSystemRequest`] back to a [`SystemRequest`].
///
/// Uses the user's `SubjectKey` material and verifies the AAD binding.
///
/// # Errors
///
/// Returns [`DecryptionError::Decryption`] if the key is wrong or data is
/// tampered. Returns [`DecryptionError::Deserialization`] if the decrypted
/// bytes aren't a valid `SystemRequest`.
pub fn decrypt_system_request(
    encrypted: &EncryptedSystemRequest,
    subject_key_bytes: &[u8; 32],
) -> Result<SystemRequest, DecryptionError> {
    let cipher = Aes256Gcm::new(subject_key_bytes.into());
    let nonce = Nonce::from_slice(&encrypted.nonce);

    let aad = encrypted.user_id.value().to_le_bytes();
    let payload = Payload { msg: &encrypted.sealed, aad: &aad };

    let plaintext = cipher.decrypt(nonce, payload).map_err(|_| DecryptionError::Decryption)?;

    postcard::from_bytes(&plaintext).map_err(|e| DecryptionError::Deserialization(e.to_string()))
}

/// Error during Raft entry encryption.
#[derive(Debug)]
pub enum EncryptionError {
    /// Postcard serialization of the `SystemRequest` failed.
    Serialization(String),
    /// AES-256-GCM encryption failed.
    Encryption,
}

impl std::fmt::Display for EncryptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialization(e) => write!(f, "failed to serialize SystemRequest: {e}"),
            Self::Encryption => write!(f, "AES-256-GCM encryption failed"),
        }
    }
}

/// Error during Raft entry decryption.
#[derive(Debug)]
pub enum DecryptionError {
    /// AES-256-GCM decryption failed (wrong key or tampered data).
    Decryption,
    /// Postcard deserialization of the decrypted bytes failed.
    Deserialization(String),
}

impl std::fmt::Display for DecryptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Decryption => write!(f, "AES-256-GCM decryption failed"),
            Self::Deserialization(e) => write!(f, "failed to deserialize SystemRequest: {e}"),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    fn test_key() -> [u8; 32] {
        [0xAA; 32]
    }

    fn test_user_id() -> UserId {
        UserId::new(42)
    }

    #[test]
    fn round_trip_encrypt_decrypt() {
        let request = SystemRequest::UpdateUserProfile {
            user_id: UserId::new(42),
            name: "Alice Smith".to_string(),
        };

        let key = test_key();
        let encrypted = encrypt_system_request(&request, &key, test_user_id()).unwrap();

        assert_eq!(encrypted.user_id, test_user_id());
        // Sealed bytes should differ from plaintext serialization
        assert_ne!(encrypted.sealed, postcard::to_allocvec(&request).unwrap());

        let decrypted = decrypt_system_request(&encrypted, &key).unwrap();
        assert_eq!(decrypted, request);
    }

    #[test]
    fn wrong_key_fails_decryption() {
        let request = SystemRequest::UpdateUserProfile {
            user_id: UserId::new(42),
            name: "Alice Smith".to_string(),
        };

        let key = test_key();
        let encrypted = encrypt_system_request(&request, &key, test_user_id()).unwrap();

        let wrong_key = [0xBB; 32];
        let result = decrypt_system_request(&encrypted, &wrong_key);
        assert!(matches!(result, Err(DecryptionError::Decryption)));
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let request = SystemRequest::UpdateUserProfile {
            user_id: UserId::new(42),
            name: "Alice Smith".to_string(),
        };

        let key = test_key();
        let mut encrypted = encrypt_system_request(&request, &key, test_user_id()).unwrap();
        encrypted.sealed[0] ^= 0xFF;

        let result = decrypt_system_request(&encrypted, &key);
        assert!(matches!(result, Err(DecryptionError::Decryption)));
    }

    #[test]
    fn different_nonces_produce_different_ciphertext() {
        let request = SystemRequest::UpdateUserProfile {
            user_id: UserId::new(42),
            name: "Alice Smith".to_string(),
        };

        let key = test_key();
        let enc1 = encrypt_system_request(&request, &key, test_user_id()).unwrap();
        let enc2 = encrypt_system_request(&request, &key, test_user_id()).unwrap();

        // Same plaintext, different nonces → different ciphertext
        assert_ne!(enc1.nonce, enc2.nonce);
        assert_ne!(enc1.sealed, enc2.sealed);

        // Both decrypt to the same request
        let dec1 = decrypt_system_request(&enc1, &key).unwrap();
        let dec2 = decrypt_system_request(&enc2, &key).unwrap();
        assert_eq!(dec1, dec2);
    }

    #[test]
    fn user_id_aad_prevents_cross_user_substitution() {
        let request = SystemRequest::UpdateUserProfile {
            user_id: UserId::new(42),
            name: "Alice Smith".to_string(),
        };

        let key = test_key();
        let mut encrypted = encrypt_system_request(&request, &key, test_user_id()).unwrap();

        // Change user_id (simulating cross-user key substitution attack)
        encrypted.user_id = UserId::new(99);

        // Decryption fails because AAD no longer matches
        let result = decrypt_system_request(&encrypted, &key);
        assert!(matches!(result, Err(DecryptionError::Decryption)));
    }
}
