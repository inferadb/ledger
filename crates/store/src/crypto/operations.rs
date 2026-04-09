//! Core cryptographic operations: DEK generation, wrapping, page encryption.

use aes_gcm::{
    Aes256Gcm, KeyInit, Nonce,
    aead::{Aead, Payload},
};
use aes_kw::KekAes256;
use rand::Rng;

use super::types::{DataEncryptionKey, RegionMasterKey, WrappedDek};
use crate::error::{Error, Result};

/// Generates a fresh random 256-bit DEK.
pub fn generate_dek() -> DataEncryptionKey {
    let mut key = [0u8; 32];
    rand::rng().fill_bytes(&mut key);
    DataEncryptionKey::from_bytes(key)
}

/// Wraps a DEK with an RMK using AES-KWP (RFC 5649).
///
/// The wrapped output is 40 bytes (32-byte DEK + 8-byte integrity check).
/// AES-KWP is nonce-free, eliminating nonce-reuse risks at the wrapping layer.
pub fn wrap_dek(dek: &DataEncryptionKey, rmk: &RegionMasterKey) -> Result<WrappedDek> {
    let kek = KekAes256::new(rmk.as_bytes().into());
    let mut output = [0u8; 40];
    kek.wrap(dek.as_bytes(), &mut output)
        .map_err(|_| Error::Encryption { reason: "AES-KWP wrap failed".into() })?;
    Ok(WrappedDek::from_bytes(output))
}

/// Unwraps a DEK from its AES-KWP wrapped form using the RMK.
///
/// Returns the plaintext DEK. Fails if the RMK is wrong or the
/// wrapped data has been tampered with.
pub fn unwrap_dek(wrapped: &WrappedDek, rmk: &RegionMasterKey) -> Result<DataEncryptionKey> {
    let kek = KekAes256::new(rmk.as_bytes().into());
    let mut output = [0u8; 32];
    kek.unwrap(wrapped.as_bytes(), &mut output).map_err(|_| Error::Encryption {
        reason: "AES-KWP unwrap failed (wrong RMK or tampered wrapped DEK)".into(),
    })?;
    Ok(DataEncryptionKey::from_bytes(output))
}

/// Encrypts a page body with AES-256-GCM.
///
/// - `body`: plaintext page content (everything after the 16-byte page header)
/// - `aad`: additional authenticated data (the 16-byte page header)
/// - `dek`: the data encryption key
///
/// Returns `(ciphertext, nonce, auth_tag)` where ciphertext is the same
/// length as the input body.
pub fn encrypt_page_body(
    body: &[u8],
    aad: &[u8],
    dek: &DataEncryptionKey,
) -> Result<(Vec<u8>, [u8; 12], [u8; 16])> {
    let cipher = Aes256Gcm::new(dek.as_bytes().into());

    // Generate random 12-byte nonce
    let mut nonce_bytes = [0u8; 12];
    rand::rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let payload = Payload { msg: body, aad };
    let ciphertext_with_tag = cipher
        .encrypt(nonce, payload)
        .map_err(|_| Error::Encryption { reason: "AES-256-GCM encryption failed".into() })?;

    // aes-gcm appends the 16-byte tag to the ciphertext
    let tag_offset = ciphertext_with_tag.len() - 16;
    let ciphertext = ciphertext_with_tag[..tag_offset].to_vec();
    let mut auth_tag = [0u8; 16];
    auth_tag.copy_from_slice(&ciphertext_with_tag[tag_offset..]);

    Ok((ciphertext, nonce_bytes, auth_tag))
}

/// Decrypts a page body with AES-256-GCM.
///
/// - `ciphertext`: encrypted page content
/// - `nonce`: the 12-byte nonce used during encryption
/// - `aad`: additional authenticated data (the 16-byte page header)
/// - `auth_tag`: the 16-byte GCM authentication tag
/// - `dek`: the data encryption key
///
/// Returns the decrypted plaintext body. Fails if the key is wrong,
/// the data has been tampered with, or the AAD doesn't match.
pub fn decrypt_page_body(
    ciphertext: &[u8],
    nonce: &[u8; 12],
    aad: &[u8],
    auth_tag: &[u8; 16],
    dek: &DataEncryptionKey,
) -> Result<Vec<u8>> {
    let cipher = Aes256Gcm::new(dek.as_bytes().into());
    let nonce = Nonce::from_slice(nonce);

    // Reconstruct ciphertext+tag for aes-gcm (it expects them concatenated)
    let mut ct_with_tag = Vec::with_capacity(ciphertext.len() + 16);
    ct_with_tag.extend_from_slice(ciphertext);
    ct_with_tag.extend_from_slice(auth_tag);

    let payload = Payload { msg: &ct_with_tag, aad };
    cipher.decrypt(nonce, payload).map_err(|_| Error::Encryption {
        reason: "AES-256-GCM decryption failed (wrong key or tampered data)".into(),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn test_rmk() -> RegionMasterKey {
        RegionMasterKey::new(1, [0xAA; 32])
    }

    #[test]
    fn test_generate_dek_produces_random_keys() {
        let dek1 = generate_dek();
        let dek2 = generate_dek();
        // Two random DEKs should never be identical
        assert_ne!(dek1.as_bytes(), dek2.as_bytes());
    }

    #[test]
    fn test_wrap_unwrap_roundtrip() {
        let dek = generate_dek();
        let rmk = test_rmk();

        let wrapped = wrap_dek(&dek, &rmk).unwrap();
        let unwrapped = unwrap_dek(&wrapped, &rmk).unwrap();

        assert_eq!(dek.as_bytes(), unwrapped.as_bytes());
    }

    #[test]
    fn test_unwrap_with_wrong_rmk_fails() {
        let dek = generate_dek();
        let rmk1 = RegionMasterKey::new(1, [0xAA; 32]);
        let rmk2 = RegionMasterKey::new(2, [0xBB; 32]);

        let wrapped = wrap_dek(&dek, &rmk1).unwrap();
        let result = unwrap_dek(&wrapped, &rmk2);

        assert!(result.is_err());
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let dek = generate_dek();
        let body = b"Hello, encrypted world!";
        let aad = b"page-header-16b!"; // 16-byte AAD

        let (ciphertext, nonce, auth_tag) = encrypt_page_body(body, aad, &dek).unwrap();

        // Ciphertext should be same length as plaintext
        assert_eq!(ciphertext.len(), body.len());
        // Ciphertext should differ from plaintext
        assert_ne!(&ciphertext, &body[..]);

        let plaintext = decrypt_page_body(&ciphertext, &nonce, aad, &auth_tag, &dek).unwrap();
        assert_eq!(&plaintext, &body[..]);
    }

    #[test]
    fn test_decrypt_with_wrong_key_fails() {
        let dek1 = generate_dek();
        let dek2 = generate_dek();
        let body = b"sensitive data";
        let aad = b"page-header-16b!";

        let (ciphertext, nonce, auth_tag) = encrypt_page_body(body, aad, &dek1).unwrap();
        let result = decrypt_page_body(&ciphertext, &nonce, aad, &auth_tag, &dek2);

        assert!(result.is_err());
    }

    #[test]
    fn test_decrypt_with_tampered_ciphertext_fails() {
        let dek = generate_dek();
        let body = b"sensitive data here";
        let aad = b"page-header-16b!";

        let (mut ciphertext, nonce, auth_tag) = encrypt_page_body(body, aad, &dek).unwrap();
        ciphertext[0] ^= 0xFF; // Tamper with ciphertext

        let result = decrypt_page_body(&ciphertext, &nonce, aad, &auth_tag, &dek);
        assert!(result.is_err());
    }

    #[test]
    fn test_decrypt_with_tampered_aad_fails() {
        let dek = generate_dek();
        let body = b"sensitive data here";
        let aad = b"page-header-16b!";

        let (ciphertext, nonce, auth_tag) = encrypt_page_body(body, aad, &dek).unwrap();

        // Tampered AAD should cause authentication failure
        let bad_aad = b"tampered-header!";
        let result = decrypt_page_body(&ciphertext, &nonce, bad_aad, &auth_tag, &dek);
        assert!(result.is_err());
    }

    #[test]
    fn test_encrypt_empty_body() {
        let dek = generate_dek();
        let body = b"";
        let aad = b"page-header-16b!";

        let (ciphertext, nonce, auth_tag) = encrypt_page_body(body, aad, &dek).unwrap();
        assert_eq!(ciphertext.len(), 0);

        let plaintext = decrypt_page_body(&ciphertext, &nonce, aad, &auth_tag, &dek).unwrap();
        assert_eq!(&plaintext, &body[..]);
    }

    #[test]
    fn test_each_encryption_uses_unique_nonce() {
        let dek = generate_dek();
        let body = b"same data";
        let aad = b"page-header-16b!";

        let (_, nonce1, _) = encrypt_page_body(body, aad, &dek).unwrap();
        let (_, nonce2, _) = encrypt_page_body(body, aad, &dek).unwrap();

        // Random nonces should differ
        assert_ne!(nonce1, nonce2);
    }

    proptest! {
        /// For any DEK and RMK key material, wrap→unwrap recovers the original DEK.
        #[test]
        fn prop_wrap_unwrap_roundtrip(
            dek_bytes in proptest::array::uniform32(any::<u8>()),
            rmk_bytes in proptest::array::uniform32(any::<u8>()),
            rmk_version in any::<u32>(),
        ) {
            let dek = DataEncryptionKey::from_bytes(dek_bytes);
            let rmk = RegionMasterKey::new(rmk_version, rmk_bytes);

            let wrapped = wrap_dek(&dek, &rmk).unwrap();
            let unwrapped = unwrap_dek(&wrapped, &rmk).unwrap();
            prop_assert_eq!(dek.as_bytes(), unwrapped.as_bytes());
        }

        /// For any plaintext body and AAD, encrypt→decrypt recovers the original body.
        #[test]
        fn prop_encrypt_decrypt_roundtrip(
            body in proptest::collection::vec(any::<u8>(), 0..4096),
            aad in proptest::collection::vec(any::<u8>(), 0..64),
        ) {
            let dek = generate_dek();
            let (ciphertext, nonce, auth_tag) = encrypt_page_body(&body, &aad, &dek).unwrap();
            prop_assert_eq!(ciphertext.len(), body.len());

            let plaintext = decrypt_page_body(&ciphertext, &nonce, &aad, &auth_tag, &dek).unwrap();
            prop_assert_eq!(&plaintext, &body);
        }
    }
}
