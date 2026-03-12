//! Constants for the user onboarding flow (email verification + account creation).
//!
//! These govern verification code generation, rate limiting, onboarding account
//! TTLs, and saga completion timeouts. All values are compile-time constants —
//! runtime reconfiguration is not supported for onboarding parameters.

use std::time::Duration;

/// Verification code length in characters (A-Z0-9 charset).
///
/// 6 characters from a 36-character alphabet gives 36^6 ≈ 2.18 billion
/// combinations, sufficient to prevent brute-force within the
/// [`MAX_CODE_ATTEMPTS`] limit.
pub const CODE_LENGTH: usize = 6;

/// Verification code TTL: 10 minutes.
///
/// After this duration, the pending verification record expires and the
/// user must request a new code. The cleanup job
/// (`CleanupExpiredOnboarding`) garbage-collects expired records.
pub const CODE_TTL: Duration = Duration::from_secs(600);

/// Maximum failed verification attempts per code before lockout.
///
/// After this many incorrect attempts, the code is considered exhausted.
/// The user must request a new code via `initiate_email_verification`.
pub const MAX_CODE_ATTEMPTS: u32 = 5;

/// Maximum initiation requests per email per region per hour.
///
/// Enforced by the Raft state machine using a sliding window counter
/// stored in the `PendingEmailVerification` record. Prevents abuse of
/// the email sending pathway.
pub const MAX_INITIATIONS_PER_HOUR: u32 = 3;

/// Rate limit window duration: 1 hour.
///
/// The sliding window resets after this duration, allowing
/// `MAX_INITIATIONS_PER_HOUR` more verification requests.
pub const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(3600);

/// Onboarding account TTL: 12 hours.
///
/// After email verification, the `OnboardingAccount` record is valid
/// for this duration. The user must call `complete_registration` within
/// this window to finalize account creation.
pub const ONBOARDING_TTL: Duration = Duration::from_secs(43_200);

/// Maximum records scanned per GC cycle for onboarding cleanup.
///
/// Bounds the apply handler latency for `CleanupExpiredOnboarding`.
/// With a 5-minute interval and 5,000 records per cycle, throughput
/// is 60,000 records/hr — far exceeding what the per-email rate limit
/// allows to accumulate.
pub const MAX_ONBOARDING_SCAN: usize = 5_000;

/// Onboarding token prefix for external identification.
///
/// Format: `ilobt_{base64url(32 random bytes)}`. The prefix allows
/// token type identification without parsing the payload.
pub const ONBOARDING_TOKEN_PREFIX: &str = "ilobt_";

/// Maximum time for the onboarding saga to complete before the handler
/// returns `DEADLINE_EXCEEDED`.
///
/// At runtime, the effective timeout is bounded by
/// `min(this, remaining_grpc_deadline - 2s)` to ensure the response
/// is sent before the client's deadline expires.
pub const SAGA_COMPLETION_TIMEOUT: Duration = Duration::from_secs(30);

/// Generates a random onboarding token and its SHA-256 hash.
///
/// Returns `(token_string, token_hash)` where:
/// - `token_string` has format `ilobt_{base64url(32 random bytes)}` (no padding)
/// - `token_hash` is `SHA-256(raw_random_bytes)` — hashed *before* encoding, so the stored hash is
///   encoding-agnostic
///
/// 256-bit entropy makes HMAC unnecessary — the token is unguessable without
/// the blinding key. Only the hash is stored in Raft state; the plaintext
/// token is returned to the client exactly once.
pub fn generate_onboarding_token() -> (String, [u8; 32]) {
    use base64::Engine;
    use rand::Rng;
    use sha2::Digest;

    let mut raw_bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut raw_bytes);

    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw_bytes);
    let token = format!("{ONBOARDING_TOKEN_PREFIX}{encoded}");

    let hash: [u8; 32] = sha2::Sha256::digest(raw_bytes).into();
    (token, hash)
}

/// Decodes an onboarding token string back to the raw 32-byte payload.
///
/// Strips the [`ONBOARDING_TOKEN_PREFIX`] and base64url-decodes the remainder.
/// The caller hashes the returned bytes with SHA-256 for lookup.
///
/// # Errors
///
/// Returns an error if:
/// - The token doesn't start with [`ONBOARDING_TOKEN_PREFIX`]
/// - The base64url payload decodes to something other than 32 bytes
/// - The base64url encoding is invalid
pub fn decode_onboarding_token(token: &str) -> Result<[u8; 32], OnboardingTokenError> {
    use base64::Engine;

    let payload =
        token.strip_prefix(ONBOARDING_TOKEN_PREFIX).ok_or(OnboardingTokenError::InvalidPrefix)?;

    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| OnboardingTokenError::InvalidEncoding)?;

    let raw: [u8; 32] = bytes
        .try_into()
        .map_err(|v: Vec<u8>| OnboardingTokenError::InvalidLength { actual: v.len() })?;

    Ok(raw)
}

/// Errors from [`decode_onboarding_token`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OnboardingTokenError {
    /// Token doesn't start with the expected prefix.
    InvalidPrefix,
    /// Base64url payload is malformed.
    InvalidEncoding,
    /// Decoded payload is not 32 bytes.
    InvalidLength {
        /// Actual decoded length.
        actual: usize,
    },
}

impl std::fmt::Display for OnboardingTokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPrefix => write!(f, "token missing '{ONBOARDING_TOKEN_PREFIX}' prefix"),
            Self::InvalidEncoding => write!(f, "invalid base64url encoding"),
            Self::InvalidLength { actual } => {
                write!(f, "expected 32 bytes, got {actual}")
            },
        }
    }
}

impl std::error::Error for OnboardingTokenError {}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_code_length() {
        assert_eq!(CODE_LENGTH, 6);
    }

    #[test]
    fn test_code_ttl_is_10_minutes() {
        assert_eq!(CODE_TTL, Duration::from_secs(600));
        assert_eq!(CODE_TTL.as_secs() / 60, 10);
    }

    #[test]
    fn test_max_code_attempts() {
        assert_eq!(MAX_CODE_ATTEMPTS, 5);
    }

    #[test]
    fn test_max_initiations_per_hour() {
        assert_eq!(MAX_INITIATIONS_PER_HOUR, 3);
    }

    #[test]
    fn test_onboarding_ttl_is_12_hours() {
        assert_eq!(ONBOARDING_TTL, Duration::from_secs(43_200));
        assert_eq!(ONBOARDING_TTL.as_secs() / 3600, 12);
    }

    #[test]
    fn test_max_onboarding_scan() {
        assert_eq!(MAX_ONBOARDING_SCAN, 5_000);
    }

    #[test]
    fn test_onboarding_token_prefix() {
        assert_eq!(ONBOARDING_TOKEN_PREFIX, "ilobt_");
    }

    #[test]
    fn test_saga_completion_timeout_is_30_seconds() {
        assert_eq!(SAGA_COMPLETION_TIMEOUT, Duration::from_secs(30));
    }

    #[test]
    fn test_saga_timeout_less_than_typical_grpc_deadline() {
        // Typical gRPC deadline is 60s; saga timeout must be less
        // to leave room for the 2s buffer.
        assert!(SAGA_COMPLETION_TIMEOUT.as_secs() < 60);
    }

    #[test]
    fn test_code_ttl_less_than_onboarding_ttl() {
        // Code expires faster than the onboarding account — the user
        // must verify the code before it expires, then has a longer
        // window to complete registration.
        assert!(CODE_TTL < ONBOARDING_TTL);
    }

    // ========================================================================
    // Onboarding Token Tests
    // ========================================================================

    #[test]
    fn test_generate_onboarding_token_has_prefix() {
        let (token, _) = generate_onboarding_token();
        assert!(token.starts_with(ONBOARDING_TOKEN_PREFIX));
    }

    #[test]
    fn test_generate_onboarding_token_roundtrip() {
        let (token, hash) = generate_onboarding_token();
        let raw_bytes = decode_onboarding_token(&token).unwrap();
        // Hash the decoded bytes and verify it matches the generated hash
        use sha2::Digest;
        let recomputed_hash: [u8; 32] = sha2::Sha256::digest(raw_bytes).into();
        assert_eq!(hash, recomputed_hash);
    }

    #[test]
    fn test_generate_onboarding_token_unique() {
        let (t1, _) = generate_onboarding_token();
        let (t2, _) = generate_onboarding_token();
        assert_ne!(t1, t2, "Two consecutive tokens should differ");
    }

    #[test]
    fn test_generate_onboarding_token_hash_is_sha256_of_raw_bytes() {
        let (token, hash) = generate_onboarding_token();
        let raw_bytes = decode_onboarding_token(&token).unwrap();
        use sha2::Digest;
        let expected: [u8; 32] = sha2::Sha256::digest(raw_bytes).into();
        assert_eq!(hash, expected);
    }

    #[test]
    fn test_decode_onboarding_token_invalid_prefix() {
        let err = decode_onboarding_token("wrong_prefix_abc123").unwrap_err();
        assert_eq!(err, OnboardingTokenError::InvalidPrefix);
    }

    #[test]
    fn test_decode_onboarding_token_invalid_encoding() {
        let err = decode_onboarding_token("ilobt_!!!invalid!!!").unwrap_err();
        assert_eq!(err, OnboardingTokenError::InvalidEncoding);
    }

    #[test]
    fn test_decode_onboarding_token_wrong_length() {
        use base64::Engine;
        let short = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([0u8; 16]);
        let token = format!("{ONBOARDING_TOKEN_PREFIX}{short}");
        let err = decode_onboarding_token(&token).unwrap_err();
        assert_eq!(err, OnboardingTokenError::InvalidLength { actual: 16 });
    }

    #[test]
    fn test_onboarding_token_error_display() {
        assert!(OnboardingTokenError::InvalidPrefix.to_string().contains("prefix"));
        assert!(OnboardingTokenError::InvalidEncoding.to_string().contains("base64url"));
        assert!(OnboardingTokenError::InvalidLength { actual: 16 }.to_string().contains("16"));
    }
}
