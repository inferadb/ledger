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
use zeroize::{Zeroize, ZeroizeOnDrop};

type HmacSha256 = Hmac<Sha256>;

/// A 32-byte secret key used to compute HMAC-SHA256 hashes of email addresses.
///
/// HMAC (vs. bare SHA-256) prevents dictionary attacks against the global email
/// hash index — without the blinding key, an attacker cannot enumerate emails
/// from their hashes.
///
/// Loaded from an external key source at node startup. Never persisted to disk
/// or distributed via Raft.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct EmailBlindingKey {
    bytes: [u8; 32],
    #[zeroize(skip)]
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
/// Rules (applied in order):
/// 1. Trims leading/trailing whitespace and lowercases
/// 2. Strips plus-addressing: `user+tag@example.com` → `user@example.com`
/// 3. Removes dots from Gmail/Googlemail local parts: `u.ser@gmail.com` → `user@gmail.com`
///
/// The plaintext email stored in REGIONAL is never modified. This normalization
/// is only used for HMAC computation (rate limiting, dedup, uniqueness checks).
pub fn normalize_email(email: &str) -> String {
    let email = email.trim().to_lowercase();
    let Some((local, domain)) = email.rsplit_once('@') else {
        return email;
    };
    // Strip plus-addressing: user+tag → user
    let local = local.split('+').next().unwrap_or(local);
    // Gmail dot-insensitivity: u.ser → user (Gmail/Googlemail only)
    let local = if domain == "gmail.com" || domain == "googlemail.com" {
        local.replace('.', "")
    } else {
        local.to_string()
    };
    format!("{local}@{domain}")
}

/// Computes `HMAC-SHA256(key, "email:" || normalize(email))` and returns the
/// result as a lowercase hex string (64 characters).
///
/// The `"email:"` domain prefix provides cryptographic separation from
/// [`compute_code_hash`], which uses `"code:"` — even if a verification code
/// happens to look like an email address, the HMAC outputs will differ.
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
    mac.update(b"email:");
    mac.update(normalized.as_bytes());
    let result = mac.finalize().into_bytes();
    bytes_to_hex(&result)
}

/// Computes `HMAC-SHA256(key, "code:" || uppercase(code))` and returns the
/// 32-byte raw hash.
///
/// Used for verification code hashing during onboarding. The `"code:"` domain
/// prefix provides cryptographic separation from [`compute_email_hmac`], which
/// uses `"email:"` — even if a verification code happens to look like an email,
/// the outputs will differ.
///
/// The code is uppercased before hashing to make verification case-insensitive
/// (users may type `abc123` or `ABC123` interchangeably).
pub fn compute_code_hash(key: &EmailBlindingKey, code: &str) -> [u8; 32] {
    let uppercased = code.trim().to_uppercase();
    let mut mac = match HmacSha256::new_from_slice(&key.bytes) {
        Ok(m) => m,
        Err(_) => return [0u8; 32],
    };
    mac.update(b"code:");
    mac.update(uppercased.as_bytes());
    mac.finalize().into_bytes().into()
}

/// Generates a random verification code and its HMAC hash.
///
/// Returns `(code, hash)` where:
/// - `code` is a [`CODE_LENGTH`](crate::onboarding::CODE_LENGTH)-character string from the `A-Z0-9`
///   charset (36 characters, 6 positions = 36^6 ≈ 2.18 billion combinations)
/// - `hash` is `compute_code_hash(key, &code)` (32-byte HMAC-SHA256)
///
/// The code is generated using a CSPRNG (`rand::rng()`). Only the hash
/// is stored in Raft state; the plaintext code is sent to the user via
/// email and discarded.
pub fn generate_verification_code(key: &EmailBlindingKey) -> (String, [u8; 32]) {
    use rand::RngExt;

    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    let code: String = (0..crate::onboarding::CODE_LENGTH)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();
    let hash = compute_code_hash(key, &code);
    (code, hash)
}

/// Converts a byte slice to a lowercase hex string.
///
/// Pre-allocates an exact-sized buffer (`bytes.len() * 2`) and uses
/// `fmt::Write` — more efficient than the per-byte `format!("{b:02x}")`
/// pattern that was previously inlined across multiple crates.
pub fn bytes_to_hex(bytes: &[u8]) -> String {
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

    #[test]
    fn zeroize_clears_key_material() {
        let mut key = EmailBlindingKey::new([0xFF; 32], 42);
        // Verify non-zero before zeroize
        assert!(key.as_bytes().iter().any(|&b| b != 0));
        assert_eq!(key.version(), 42);

        // Zeroize trait method zeroes key bytes in-place;
        // version is skipped (not key material, matches RegionMasterKey pattern)
        Zeroize::zeroize(&mut key);
        assert_eq!(key.as_bytes(), &[0u8; 32]);
        assert_eq!(key.version(), 42);
    }

    #[test]
    fn code_hash_deterministic() {
        let key = test_key(1);
        let h1 = compute_code_hash(&key, "ABC123");
        let h2 = compute_code_hash(&key, "ABC123");
        assert_eq!(h1, h2);
    }

    #[test]
    fn code_hash_case_insensitive() {
        let key = test_key(1);
        let h1 = compute_code_hash(&key, "abc123");
        let h2 = compute_code_hash(&key, "ABC123");
        assert_eq!(h1, h2);
    }

    #[test]
    fn code_hash_trims_whitespace() {
        let key = test_key(1);
        let h1 = compute_code_hash(&key, "  ABC123  ");
        let h2 = compute_code_hash(&key, "ABC123");
        assert_eq!(h1, h2);
    }

    #[test]
    fn code_hash_different_codes_differ() {
        let key = test_key(1);
        let h1 = compute_code_hash(&key, "ABC123");
        let h2 = compute_code_hash(&key, "XYZ789");
        assert_ne!(h1, h2);
    }

    #[test]
    fn code_hash_returns_32_bytes() {
        let key = test_key(1);
        let hash = compute_code_hash(&key, "ABC123");
        assert_eq!(hash.len(), 32);
        assert_ne!(hash, [0u8; 32]);
    }

    #[test]
    fn code_hash_domain_separated_from_email_hmac() {
        // The same input string through compute_code_hash and
        // compute_email_hmac must produce different outputs, proving
        // the "code:" vs implicit domain prefix provides separation.
        let key = test_key(1);
        let code_hash = compute_code_hash(&key, "test@example.com");
        let email_hmac = compute_email_hmac(&key, "test@example.com");

        // Convert code_hash to hex for comparison
        let code_hex = bytes_to_hex(&code_hash);
        assert_ne!(code_hex, email_hmac, "code hash and email HMAC must differ for same input");
    }

    #[test]
    fn code_hash_different_keys_differ() {
        let k1 = EmailBlindingKey::new([0xAA; 32], 1);
        let k2 = EmailBlindingKey::new([0xBB; 32], 2);
        let h1 = compute_code_hash(&k1, "ABC123");
        let h2 = compute_code_hash(&k2, "ABC123");
        assert_ne!(h1, h2);
    }

    #[test]
    fn verification_code_length_matches_constant() {
        let key = test_key(1);
        let (code, _hash) = generate_verification_code(&key);
        assert_eq!(code.len(), crate::onboarding::CODE_LENGTH);
    }

    #[test]
    fn verification_code_charset_is_alphanumeric_uppercase() {
        let key = test_key(1);
        // Generate several codes and verify charset
        for _ in 0..20 {
            let (code, _) = generate_verification_code(&key);
            assert!(
                code.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()),
                "Code contains invalid character: {code}"
            );
        }
    }

    #[test]
    fn verification_code_hash_matches_compute_code_hash() {
        let key = test_key(1);
        let (code, hash) = generate_verification_code(&key);
        let recomputed = compute_code_hash(&key, &code);
        assert_eq!(hash, recomputed);
    }

    #[test]
    fn verification_code_hash_case_insensitive() {
        let key = test_key(1);
        let (code, hash) = generate_verification_code(&key);
        let lower_hash = compute_code_hash(&key, &code.to_lowercase());
        assert_eq!(hash, lower_hash);
    }

    #[test]
    fn verification_codes_are_not_constant() {
        let key = test_key(1);
        let (code1, _) = generate_verification_code(&key);
        let (code2, _) = generate_verification_code(&key);
        // With 36^6 combinations, two consecutive codes colliding is ~1/2.18B.
        // This test has a negligible false-positive rate.
        assert_ne!(code1, code2, "Two consecutive codes should differ");
    }

    #[test]
    fn normalize_strips_plus_addressing() {
        assert_eq!(normalize_email("user+tag@example.com"), "user@example.com");
    }

    #[test]
    fn normalize_strips_plus_gmail() {
        assert_eq!(normalize_email("user+tag@gmail.com"), "user@gmail.com");
    }

    #[test]
    fn normalize_removes_gmail_dots() {
        assert_eq!(normalize_email("u.s.e.r@gmail.com"), "user@gmail.com");
    }

    #[test]
    fn normalize_removes_googlemail_dots() {
        assert_eq!(normalize_email("u.s.e.r@googlemail.com"), "user@googlemail.com");
    }

    #[test]
    fn normalize_preserves_non_gmail_dots() {
        assert_eq!(normalize_email("u.s.e.r@corporate.com"), "u.s.e.r@corporate.com");
    }

    #[test]
    fn normalize_plus_and_dots_combined() {
        assert_eq!(normalize_email("u.s.e.r+tag@gmail.com"), "user@gmail.com");
    }

    #[test]
    fn normalize_empty_plus_prefix() {
        assert_eq!(normalize_email("+@gmail.com"), "@gmail.com");
    }

    #[test]
    fn normalize_no_at_sign() {
        assert_eq!(normalize_email("no-at-sign"), "no-at-sign");
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
