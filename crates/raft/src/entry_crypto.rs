//! Per-entry encryption for PII-bearing Raft log entries.
//!
//! Provides AES-256-GCM encryption of [`SystemRequest`] payloads using
//! per-subject or per-organization key material. When a user is erased or
//! an organization is purged, the key is destroyed — rendering all historical
//! Raft log entries containing that entity's PII cryptographically
//! unrecoverable (crypto-shredding).
//!
//! User-scoped REGIONAL requests are encrypted via [`encrypt_user_system_request`].
//! Organization-scoped REGIONAL requests (org/team/app profile writes) are
//! encrypted via [`encrypt_org_system_request`].
//! Onboarding PII is sealed via [`seal`] before entering the Raft log.

use aes_gcm::{
    Aes256Gcm, KeyInit, Nonce,
    aead::{Aead, Payload},
};
use inferadb_ledger_types::{OrganizationId, UserId};
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
pub struct EncryptedUserSystemRequest {
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
pub fn encrypt_user_system_request(
    request: &SystemRequest,
    subject_key_bytes: &[u8; 32],
    user_id: UserId,
) -> Result<EncryptedUserSystemRequest, EncryptionError> {
    let plaintext = postcard::to_allocvec(request)
        .map_err(|e| EncryptionError::Serialization(e.to_string()))?;
    let aad = user_id.value().to_le_bytes();
    let (sealed, nonce) = seal(&plaintext, subject_key_bytes, &aad)?;
    Ok(EncryptedUserSystemRequest { sealed, nonce, user_id })
}

/// Decrypts an [`EncryptedUserSystemRequest`] back to a [`SystemRequest`].
///
/// Uses the user's `SubjectKey` material and verifies the AAD binding.
///
/// # Errors
///
/// Returns [`DecryptionError::Decryption`] if the key is wrong or data is
/// tampered. Returns [`DecryptionError::Deserialization`] if the decrypted
/// bytes aren't a valid `SystemRequest`.
pub fn decrypt_user_system_request(
    encrypted: &EncryptedUserSystemRequest,
    subject_key_bytes: &[u8; 32],
) -> Result<SystemRequest, DecryptionError> {
    let aad = encrypted.user_id.value().to_le_bytes();
    let plaintext = unseal(&encrypted.sealed, &encrypted.nonce, subject_key_bytes, &aad)?;
    postcard::from_bytes(&plaintext).map_err(|e| DecryptionError::Deserialization(e.to_string()))
}

/// Encrypted form of a [`SystemRequest`] for at-rest organization PII protection.
///
/// Mirrors [`EncryptedUserSystemRequest`] but keyed on `OrganizationId` instead of
/// `UserId`. Organization-scoped PII (org names, team names, app names) is
/// encrypted with the organization's `OrgKey`. When the organization is purged
/// and the `OrgKey` destroyed, historical log entries become unrecoverable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncryptedOrgSystemRequest {
    /// AES-256-GCM sealed output: `ciphertext || auth_tag` (16-byte tag appended).
    pub sealed: Vec<u8>,
    /// 12-byte random nonce (unique per encryption).
    pub nonce: [u8; 12],
    /// Organization whose `OrgKey` encrypts this entry.
    ///
    /// Stored in plaintext for key lookup during decryption.
    pub organization: OrganizationId,
}

/// Encrypts a [`SystemRequest`] using an organization's `OrgKey` material.
///
/// The request is postcard-serialized, then encrypted with AES-256-GCM.
/// The organization ID is bound as AAD to prevent cross-org key substitution.
///
/// # Errors
///
/// Returns an error if serialization or encryption fails.
pub fn encrypt_org_system_request(
    request: &SystemRequest,
    org_key_bytes: &[u8; 32],
    organization: OrganizationId,
) -> Result<EncryptedOrgSystemRequest, EncryptionError> {
    let plaintext = postcard::to_allocvec(request)
        .map_err(|e| EncryptionError::Serialization(e.to_string()))?;
    let aad = organization.value().to_le_bytes();
    let (sealed, nonce) = seal(&plaintext, org_key_bytes, &aad)?;
    Ok(EncryptedOrgSystemRequest { sealed, nonce, organization })
}

/// Decrypts an [`EncryptedOrgSystemRequest`] back to a [`SystemRequest`].
///
/// Uses the organization's `OrgKey` material and verifies the AAD binding.
///
/// # Errors
///
/// Returns [`DecryptionError::Decryption`] if the key is wrong or data is
/// tampered. Returns [`DecryptionError::Deserialization`] if the decrypted
/// bytes aren't a valid `SystemRequest`.
pub fn decrypt_org_system_request(
    encrypted: &EncryptedOrgSystemRequest,
    org_key_bytes: &[u8; 32],
) -> Result<SystemRequest, DecryptionError> {
    let aad = encrypted.organization.value().to_le_bytes();
    let plaintext = unseal(&encrypted.sealed, &encrypted.nonce, org_key_bytes, &aad)?;
    postcard::from_bytes(&plaintext).map_err(|e| DecryptionError::Deserialization(e.to_string()))
}

/// PII fields sealed during onboarding before entering the Raft log.
///
/// Serialized with postcard, then AES-256-GCM encrypted using the user's
/// `SubjectKey`. The `SubjectKey` itself travels alongside this sealed blob
/// in the same Raft entry (bootstrapping). At-rest protection comes from
/// the `EncryptedBackend`; per-entry sealing protects against log replay
/// after user erasure (the apply handler checks the erasure tombstone).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingPii {
    /// Plaintext email address.
    pub email: String,
    /// User display name.
    pub name: String,
    /// Organization name.
    pub organization_name: String,
}

/// Seals arbitrary bytes with AES-256-GCM.
///
/// Returns `(ciphertext || tag, nonce)`. The `aad` parameter binds the
/// ciphertext to a specific context (e.g., user ID bytes).
pub fn seal(
    plaintext: &[u8],
    key: &[u8; 32],
    aad: &[u8],
) -> Result<(Vec<u8>, [u8; 12]), EncryptionError> {
    let cipher = Aes256Gcm::new(key.into());

    let mut nonce_bytes = [0u8; 12];
    rand::rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let payload = Payload { msg: plaintext, aad };
    let sealed = cipher.encrypt(nonce, payload).map_err(|_| EncryptionError::Encryption)?;

    Ok((sealed, nonce_bytes))
}

/// Unseals AES-256-GCM encrypted bytes.
///
/// Returns the plaintext. Fails if the key is wrong, data is tampered,
/// or the AAD does not match.
pub fn unseal(
    sealed: &[u8],
    nonce: &[u8; 12],
    key: &[u8; 32],
    aad: &[u8],
) -> Result<Vec<u8>, DecryptionError> {
    let cipher = Aes256Gcm::new(key.into());
    let nonce = Nonce::from_slice(nonce);

    let payload = Payload { msg: sealed, aad };
    cipher.decrypt(nonce, payload).map_err(|_| DecryptionError::Decryption)
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
        let encrypted = encrypt_user_system_request(&request, &key, test_user_id()).unwrap();

        assert_eq!(encrypted.user_id, test_user_id());
        // Sealed bytes should differ from plaintext serialization
        assert_ne!(encrypted.sealed, postcard::to_allocvec(&request).unwrap());

        let decrypted = decrypt_user_system_request(&encrypted, &key).unwrap();
        assert_eq!(decrypted, request);
    }

    #[test]
    fn wrong_key_fails_decryption() {
        let request = SystemRequest::UpdateUserProfile {
            user_id: UserId::new(42),
            name: "Alice Smith".to_string(),
        };

        let key = test_key();
        let encrypted = encrypt_user_system_request(&request, &key, test_user_id()).unwrap();

        let wrong_key = [0xBB; 32];
        let result = decrypt_user_system_request(&encrypted, &wrong_key);
        assert!(matches!(result, Err(DecryptionError::Decryption)));
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let request = SystemRequest::UpdateUserProfile {
            user_id: UserId::new(42),
            name: "Alice Smith".to_string(),
        };

        let key = test_key();
        let mut encrypted = encrypt_user_system_request(&request, &key, test_user_id()).unwrap();
        encrypted.sealed[0] ^= 0xFF;

        let result = decrypt_user_system_request(&encrypted, &key);
        assert!(matches!(result, Err(DecryptionError::Decryption)));
    }

    #[test]
    fn different_nonces_produce_different_ciphertext() {
        let request = SystemRequest::UpdateUserProfile {
            user_id: UserId::new(42),
            name: "Alice Smith".to_string(),
        };

        let key = test_key();
        let enc1 = encrypt_user_system_request(&request, &key, test_user_id()).unwrap();
        let enc2 = encrypt_user_system_request(&request, &key, test_user_id()).unwrap();

        // Same plaintext, different nonces → different ciphertext
        assert_ne!(enc1.nonce, enc2.nonce);
        assert_ne!(enc1.sealed, enc2.sealed);

        // Both decrypt to the same request
        let dec1 = decrypt_user_system_request(&enc1, &key).unwrap();
        let dec2 = decrypt_user_system_request(&enc2, &key).unwrap();
        assert_eq!(dec1, dec2);
    }

    #[test]
    fn user_id_aad_prevents_cross_user_substitution() {
        let request = SystemRequest::UpdateUserProfile {
            user_id: UserId::new(42),
            name: "Alice Smith".to_string(),
        };

        let key = test_key();
        let mut encrypted = encrypt_user_system_request(&request, &key, test_user_id()).unwrap();

        // Change user_id (simulating cross-user key substitution attack)
        encrypted.user_id = UserId::new(99);

        // Decryption fails because AAD no longer matches
        let result = decrypt_user_system_request(&encrypted, &key);
        assert!(matches!(result, Err(DecryptionError::Decryption)));
    }

    #[test]
    fn seal_unseal_round_trip() {
        let key = test_key();
        let aad = 42i64.to_le_bytes();
        let plaintext = b"sensitive PII data";

        let (sealed, nonce) = seal(plaintext, &key, &aad).unwrap();
        assert_ne!(sealed, plaintext);

        let recovered = unseal(&sealed, &nonce, &key, &aad).unwrap();
        assert_eq!(recovered, plaintext);
    }

    #[test]
    fn seal_wrong_key_fails() {
        let key = test_key();
        let aad = 42i64.to_le_bytes();
        let (sealed, nonce) = seal(b"data", &key, &aad).unwrap();

        let wrong_key = [0xBB; 32];
        let result = unseal(&sealed, &nonce, &wrong_key, &aad);
        assert!(matches!(result, Err(DecryptionError::Decryption)));
    }

    #[test]
    fn seal_wrong_aad_fails() {
        let key = test_key();
        let aad = 42i64.to_le_bytes();
        let (sealed, nonce) = seal(b"data", &key, &aad).unwrap();

        let wrong_aad = 99i64.to_le_bytes();
        let result = unseal(&sealed, &nonce, &key, &wrong_aad);
        assert!(matches!(result, Err(DecryptionError::Decryption)));
    }

    fn test_org_id() -> OrganizationId {
        OrganizationId::new(7)
    }

    #[test]
    fn org_round_trip_encrypt_decrypt() {
        let request = SystemRequest::UpdateOrganizationProfile {
            organization: test_org_id(),
            name: "Acme Corp".to_string(),
        };

        let key = test_key();
        let encrypted = encrypt_org_system_request(&request, &key, test_org_id()).unwrap();

        assert_eq!(encrypted.organization, test_org_id());
        assert_ne!(encrypted.sealed, postcard::to_allocvec(&request).unwrap());

        let decrypted = decrypt_org_system_request(&encrypted, &key).unwrap();
        assert_eq!(decrypted, request);
    }

    #[test]
    fn org_wrong_key_fails_decryption() {
        let request = SystemRequest::UpdateOrganizationProfile {
            organization: test_org_id(),
            name: "Acme Corp".to_string(),
        };

        let key = test_key();
        let encrypted = encrypt_org_system_request(&request, &key, test_org_id()).unwrap();

        let wrong_key = [0xBB; 32];
        let result = decrypt_org_system_request(&encrypted, &wrong_key);
        assert!(matches!(result, Err(DecryptionError::Decryption)));
    }

    #[test]
    fn org_id_aad_prevents_cross_org_substitution() {
        let request = SystemRequest::UpdateOrganizationProfile {
            organization: test_org_id(),
            name: "Acme Corp".to_string(),
        };

        let key = test_key();
        let mut encrypted = encrypt_org_system_request(&request, &key, test_org_id()).unwrap();

        encrypted.organization = OrganizationId::new(99);

        let result = decrypt_org_system_request(&encrypted, &key);
        assert!(matches!(result, Err(DecryptionError::Decryption)));
    }

    #[test]
    fn onboarding_pii_seal_round_trip() {
        let key = test_key();
        let user_id = test_user_id();
        let aad = user_id.value().to_le_bytes();

        let pii = OnboardingPii {
            email: "alice@example.com".to_string(),
            name: "Alice".to_string(),
            organization_name: "Acme Corp".to_string(),
        };
        let plaintext = postcard::to_allocvec(&pii).unwrap();
        let (sealed, nonce) = seal(&plaintext, &key, &aad).unwrap();

        let recovered_bytes = unseal(&sealed, &nonce, &key, &aad).unwrap();
        let recovered: OnboardingPii = postcard::from_bytes(&recovered_bytes).unwrap();
        assert_eq!(recovered, pii);
    }
}
