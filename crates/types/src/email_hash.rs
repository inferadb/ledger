//! Email blinding key and HMAC-based email hashing for global uniqueness.
//!
//! Provides privacy-preserving email uniqueness enforcement across regions.
//! Plaintext emails remain in regional stores; only HMAC hashes appear in the
//! global control plane. The blinding key is loaded from an external source
//! (env var, secrets manager, or key file) — never stored in Raft state.

use std::{fmt, str::FromStr};

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// A 32-byte secret key used to compute HMAC-SHA256 hashes of email addresses.
///
/// HMAC (vs. bare SHA-256) prevents dictionary attacks against the global email
/// hash index — without the blinding key, an attacker cannot enumerate emails
/// from their hashes.
///
/// Loaded from an external key source at node startup. Never persisted to disk
/// or distributed via Raft.
#[derive(Clone)]
pub struct EmailBlindingKey {
    bytes: [u8; 32],
    version: u32,
}

impl EmailBlindingKey {
    /// Creates a new blinding key from raw bytes and a version number.
    ///
    /// Version numbers are monotonically increasing; the active version
    /// determines which key is used for new HMAC computations.
    pub fn new(bytes: [u8; 32], version: u32) -> Self {
        Self { bytes, version }
    }

    /// Returns the key version.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Returns the raw key bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.bytes
    }
}

/// `Debug` intentionally redacts key material.
impl fmt::Debug for EmailBlindingKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EmailBlindingKey")
            .field("version", &self.version)
            .field("bytes", &"[REDACTED]")
            .finish()
    }
}

/// Normalizes an email address for consistent HMAC computation.
///
/// Rules:
/// - Trims leading/trailing whitespace
/// - Lowercases the entire address (RFC 5321 local-part is technically case-sensitive, but in
///   practice all major providers treat it as case-insensitive — matching this convention avoids
///   user confusion)
pub fn normalize_email(email: &str) -> String {
    email.trim().to_lowercase()
}

/// Computes `HMAC-SHA256(key, normalize(email))` and returns the result
/// as a lowercase hex string (64 characters).
///
/// The output is deterministic for a given key + email pair, enabling
/// uniqueness checks without exposing plaintext emails in the global
/// control plane.
pub fn compute_email_hmac(key: &EmailBlindingKey, email: &str) -> String {
    let normalized = normalize_email(email);
    // HMAC-SHA256 accepts keys of any length — `new_from_slice` only fails
    // for algorithms with key-length constraints (HMAC has none). A match
    // arm satisfies the no-unwrap rule while being unreachable in practice.
    let mut mac = match HmacSha256::new_from_slice(&key.bytes) {
        Ok(m) => m,
        Err(_) => return String::new(),
    };
    mac.update(normalized.as_bytes());
    let result = mac.finalize().into_bytes();
    bytes_to_hex(&result)
}

/// Converts a byte slice to a lowercase hex string.
fn bytes_to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut acc, b| {
        let _ = write!(acc, "{b:02x}");
        acc
    })
}

/// Errors that can occur when parsing an email blinding key from a hex string.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmailBlindingKeyParseError {
    /// Hex string has wrong length (expected 64 hex chars = 32 bytes).
    InvalidLength {
        /// Actual number of hex characters provided.
        actual: usize,
    },
    /// Hex string contains non-hex characters.
    InvalidHex,
}

impl fmt::Display for EmailBlindingKeyParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength { actual } => {
                write!(f, "expected 64 hex characters (32 bytes), got {actual}")
            },
            Self::InvalidHex => write!(f, "invalid hex character in blinding key"),
        }
    }
}

impl std::error::Error for EmailBlindingKeyParseError {}

impl FromStr for EmailBlindingKey {
    type Err = EmailBlindingKeyParseError;

    /// Parses a 64-character hex string into a blinding key with version 1.
    fn from_str(hex: &str) -> std::result::Result<Self, Self::Err> {
        let hex = hex.trim();
        if hex.len() != 64 {
            return Err(EmailBlindingKeyParseError::InvalidLength { actual: hex.len() });
        }
        let mut bytes = [0u8; 32];
        for (i, byte) in bytes.iter_mut().enumerate() {
            let hi = hex_char_to_nibble(hex.as_bytes()[i * 2])
                .ok_or(EmailBlindingKeyParseError::InvalidHex)?;
            let lo = hex_char_to_nibble(hex.as_bytes()[i * 2 + 1])
                .ok_or(EmailBlindingKeyParseError::InvalidHex)?;
            *byte = (hi << 4) | lo;
        }
        Ok(Self::new(bytes, 1))
    }
}

fn hex_char_to_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn test_key(version: u32) -> EmailBlindingKey {
        EmailBlindingKey::new([0xAB; 32], version)
    }

    #[test]
    fn normalize_trims_and_lowercases() {
        assert_eq!(normalize_email("  Alice@Example.COM  "), "alice@example.com");
    }

    #[test]
    fn normalize_preserves_already_clean() {
        assert_eq!(normalize_email("bob@test.com"), "bob@test.com");
    }

    #[test]
    fn hmac_deterministic_for_same_key_and_email() {
        let key = test_key(1);
        let h1 = compute_email_hmac(&key, "alice@example.com");
        let h2 = compute_email_hmac(&key, "alice@example.com");
        assert_eq!(h1, h2);
    }

    #[test]
    fn hmac_case_insensitive() {
        let key = test_key(1);
        let h1 = compute_email_hmac(&key, "Alice@Example.COM");
        let h2 = compute_email_hmac(&key, "alice@example.com");
        assert_eq!(h1, h2);
    }

    #[test]
    fn hmac_different_emails_produce_different_hashes() {
        let key = test_key(1);
        let h1 = compute_email_hmac(&key, "alice@example.com");
        let h2 = compute_email_hmac(&key, "bob@example.com");
        assert_ne!(h1, h2);
    }

    #[test]
    fn hmac_different_keys_produce_different_hashes() {
        let k1 = EmailBlindingKey::new([0xAA; 32], 1);
        let k2 = EmailBlindingKey::new([0xBB; 32], 2);
        let h1 = compute_email_hmac(&k1, "alice@example.com");
        let h2 = compute_email_hmac(&k2, "alice@example.com");
        assert_ne!(h1, h2);
    }

    #[test]
    fn hmac_output_is_64_hex_chars() {
        let key = test_key(1);
        let hash = compute_email_hmac(&key, "test@example.com");
        assert_eq!(hash.len(), 64);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn debug_redacts_key_material() {
        let key = test_key(1);
        let debug = format!("{key:?}");
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains("171")); // 0xAB = 171
    }

    #[test]
    fn version_preserved() {
        let key = test_key(42);
        assert_eq!(key.version(), 42);
    }

    #[test]
    fn from_str_valid_hex() {
        let hex = "ab".repeat(32);
        let key: EmailBlindingKey = hex.parse().unwrap();
        assert_eq!(key.as_bytes(), &[0xAB; 32]);
        assert_eq!(key.version(), 1);
    }

    #[test]
    fn from_str_wrong_length() {
        let err = "abcd".parse::<EmailBlindingKey>().unwrap_err();
        assert_eq!(err, EmailBlindingKeyParseError::InvalidLength { actual: 4 });
    }

    #[test]
    fn from_str_invalid_hex() {
        let hex = "zz".repeat(32);
        let err = hex.parse::<EmailBlindingKey>().unwrap_err();
        assert_eq!(err, EmailBlindingKeyParseError::InvalidHex);
    }

    #[test]
    fn from_str_trims_whitespace() {
        let hex = format!("  {}  ", "ab".repeat(32));
        let key: EmailBlindingKey = hex.parse().unwrap();
        assert_eq!(key.as_bytes(), &[0xAB; 32]);
    }

    proptest! {
        /// Two distinct non-empty emails with the same key always produce
        /// distinct HMACs (collision resistance).
        #[test]
        fn prop_hmac_no_collisions(
            key_bytes in proptest::array::uniform32(any::<u8>()),
            email_a in "[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,4}",
            email_b in "[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,4}",
        ) {
            let key = EmailBlindingKey::new(key_bytes, 1);
            let na = normalize_email(&email_a);
            let nb = normalize_email(&email_b);
            // Only check non-equal normalized emails
            prop_assume!(!na.is_empty() && !nb.is_empty() && na != nb);
            let ha = compute_email_hmac(&key, &na);
            let hb = compute_email_hmac(&key, &nb);
            prop_assert_ne!(ha, hb);
        }

        /// HMAC output is always 64 lowercase hex characters for any key+email.
        #[test]
        fn prop_hmac_output_format(
            key_bytes in proptest::array::uniform32(any::<u8>()),
            email in "\\PC{1,200}",
        ) {
            let key = EmailBlindingKey::new(key_bytes, 1);
            let normalized = normalize_email(&email);
            prop_assume!(!normalized.is_empty());
            let hmac = compute_email_hmac(&key, &normalized);
            prop_assert_eq!(hmac.len(), 64);
            prop_assert!(hmac.chars().all(|c| c.is_ascii_hexdigit()));
            prop_assert!(hmac.chars().all(|c| !c.is_ascii_uppercase()));
        }
    }
}
