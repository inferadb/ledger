//! Secure pagination token implementation.
//!
//! Per DESIGN.md §7.4: Page tokens are opaque to clients and include:
//! - HMAC validation to prevent tampering
//! - Context validation (namespace_id, vault_id) to prevent cross-context reuse
//! - Query hash to detect filter changes mid-pagination
//! - Height tracking for consistent pagination across pages

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use inferadb_ledger_types::{decode, encode};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

/// Page token format version for forward compatibility.
const TOKEN_VERSION: u8 = 1;

/// HMAC key length in bytes.
const HMAC_KEY_LENGTH: usize = 32;

/// HMAC output length (truncated).
const HMAC_LENGTH: usize = 16;

/// Internal page token structure.
///
/// This is serialized, HMAC'd, and base64-encoded for client use.
/// Clients should treat tokens as opaque.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageToken {
    /// Token format version (for future changes).
    pub version: u8,
    /// Request context: namespace ID.
    pub namespace_id: i64,
    /// Request context: vault ID.
    pub vault_id: i64,
    /// Resume position: last key returned.
    pub last_key: Vec<u8>,
    /// Consistent reads: height when pagination started.
    pub at_height: u64,
    /// SeaHash of query params (prevents filter changes).
    pub query_hash: [u8; 8],
}

/// Encoded page token with HMAC protection.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct EncodedToken {
    /// The page token data.
    token: PageToken,
    /// HMAC-SHA256 truncated to 16 bytes.
    hmac: [u8; HMAC_LENGTH],
}

/// Page token encoder/decoder with HMAC validation.
pub struct PageTokenCodec {
    /// HMAC key for signing/verifying tokens.
    key: [u8; HMAC_KEY_LENGTH],
}

impl PageTokenCodec {
    /// Create a new codec with the given key.
    ///
    /// The key should be randomly generated at node startup and kept secret.
    /// Different nodes can have different keys (pagination is node-local).
    pub fn new(key: [u8; HMAC_KEY_LENGTH]) -> Self {
        Self { key }
    }

    /// Create a codec with a random key.
    ///
    /// Useful for testing or single-node deployments.
    pub fn with_random_key() -> Self {
        use rand::RngCore;
        let mut key = [0u8; HMAC_KEY_LENGTH];
        rand::thread_rng().fill_bytes(&mut key);
        Self { key }
    }

    /// Encode a page token to an opaque string.
    pub fn encode(&self, token: &PageToken) -> String {
        // Serialize the token - postcard encoding of simple structs is infallible
        let token_bytes = match encode(token) {
            Ok(bytes) => bytes,
            Err(_) => return String::new(), // Unreachable for valid PageToken
        };

        // Compute HMAC - new_from_slice accepts any length for SHA256
        let mut mac = match <Hmac<Sha256>>::new_from_slice(&self.key) {
            Ok(m) => m,
            Err(_) => return String::new(), // Unreachable for 32-byte key
        };
        mac.update(&token_bytes);
        let result = mac.finalize();
        let hmac_full = result.into_bytes();

        // Truncate HMAC to 16 bytes
        let mut hmac = [0u8; HMAC_LENGTH];
        hmac.copy_from_slice(&hmac_full[..HMAC_LENGTH]);

        // Create encoded token
        let encoded = EncodedToken {
            token: token.clone(),
            hmac,
        };

        // Serialize and base64 encode
        let bytes = match encode(&encoded) {
            Ok(b) => b,
            Err(_) => return String::new(), // Unreachable for valid EncodedToken
        };
        URL_SAFE_NO_PAD.encode(&bytes)
    }

    /// Decode and validate a page token.
    ///
    /// Returns an error if:
    /// - The token is malformed or cannot be decoded
    /// - The HMAC validation fails (tampering detected)
    /// - The token version is not supported
    pub fn decode(&self, encoded: &str) -> Result<PageToken, PageTokenError> {
        // Base64 decode
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| PageTokenError::InvalidFormat)?;

        // Deserialize
        let encoded_token: EncodedToken =
            decode(&bytes).map_err(|_| PageTokenError::InvalidFormat)?;

        // Verify HMAC
        let token_bytes =
            encode(&encoded_token.token).map_err(|_| PageTokenError::InvalidFormat)?;

        let mut mac =
            <Hmac<Sha256>>::new_from_slice(&self.key).map_err(|_| PageTokenError::InvalidFormat)?;
        mac.update(&token_bytes);
        let result = mac.finalize();
        let expected_hmac = result.into_bytes();

        // Compare truncated HMAC
        if encoded_token.hmac[..] != expected_hmac[..HMAC_LENGTH] {
            return Err(PageTokenError::InvalidHmac);
        }

        // Check version
        if encoded_token.token.version != TOKEN_VERSION {
            return Err(PageTokenError::UnsupportedVersion(
                encoded_token.token.version,
            ));
        }

        Ok(encoded_token.token)
    }

    /// Validate that a token matches the current request context.
    ///
    /// Returns an error if:
    /// - namespace_id doesn't match
    /// - vault_id doesn't match
    /// - query_hash doesn't match (filters changed)
    pub fn validate_context(
        &self,
        token: &PageToken,
        namespace_id: i64,
        vault_id: i64,
        query_hash: [u8; 8],
    ) -> Result<(), PageTokenError> {
        if token.namespace_id != namespace_id || token.vault_id != vault_id {
            return Err(PageTokenError::ContextMismatch);
        }

        if token.query_hash != query_hash {
            return Err(PageTokenError::QueryChanged);
        }

        Ok(())
    }

    /// Compute a query hash for filter parameters.
    ///
    /// Uses SeaHash for fast, deterministic hashing of query parameters.
    pub fn compute_query_hash(params: &[u8]) -> [u8; 8] {
        let hash = seahash::hash(params);
        hash.to_le_bytes()
    }
}

/// Errors that can occur during page token operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageTokenError {
    /// Token could not be decoded (malformed).
    InvalidFormat,
    /// HMAC validation failed (tampering detected).
    InvalidHmac,
    /// Token version is not supported.
    UnsupportedVersion(u8),
    /// Token namespace/vault doesn't match request.
    ContextMismatch,
    /// Query parameters changed since pagination started.
    QueryChanged,
}

impl std::fmt::Display for PageTokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PageTokenError::InvalidFormat => write!(f, "invalid page token"),
            PageTokenError::InvalidHmac => write!(f, "invalid page token"),
            PageTokenError::UnsupportedVersion(v) => {
                write!(f, "unsupported page token version: {}", v)
            }
            PageTokenError::ContextMismatch => write!(f, "page token does not match request"),
            PageTokenError::QueryChanged => {
                write!(f, "query parameters changed; start new pagination")
            }
        }
    }
}

impl std::error::Error for PageTokenError {}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let codec = PageTokenCodec::with_random_key();

        let token = PageToken {
            version: TOKEN_VERSION,
            namespace_id: 42,
            vault_id: 100,
            last_key: b"entity:user:123".to_vec(),
            at_height: 1000,
            query_hash: [1, 2, 3, 4, 5, 6, 7, 8],
        };

        let encoded = codec.encode(&token);
        let decoded = codec.decode(&encoded).expect("decode should succeed");

        assert_eq!(decoded.version, token.version);
        assert_eq!(decoded.namespace_id, token.namespace_id);
        assert_eq!(decoded.vault_id, token.vault_id);
        assert_eq!(decoded.last_key, token.last_key);
        assert_eq!(decoded.at_height, token.at_height);
        assert_eq!(decoded.query_hash, token.query_hash);
    }

    #[test]
    fn test_tampered_token_rejected() {
        let codec = PageTokenCodec::with_random_key();

        let token = PageToken {
            version: TOKEN_VERSION,
            namespace_id: 42,
            vault_id: 100,
            last_key: b"entity:user:123".to_vec(),
            at_height: 1000,
            query_hash: [1, 2, 3, 4, 5, 6, 7, 8],
        };

        let encoded = codec.encode(&token);

        // Tamper with the encoded string by changing a character
        let chars: Vec<char> = encoded.chars().collect();
        let tampered: String = chars
            .iter()
            .enumerate()
            .map(|(i, c)| {
                if i == 10 {
                    // Change one character
                    if *c == 'A' { 'B' } else { 'A' }
                } else {
                    *c
                }
            })
            .collect();

        // Decoding tampered token should fail
        let result = codec.decode(&tampered);
        assert!(result.is_err(), "tampered token should fail validation");
    }

    #[test]
    fn test_different_key_rejected() {
        let codec1 = PageTokenCodec::with_random_key();
        let codec2 = PageTokenCodec::with_random_key();

        let token = PageToken {
            version: TOKEN_VERSION,
            namespace_id: 42,
            vault_id: 100,
            last_key: b"entity:user:123".to_vec(),
            at_height: 1000,
            query_hash: [1, 2, 3, 4, 5, 6, 7, 8],
        };

        let encoded = codec1.encode(&token);

        // Decoding with different key should fail
        let result = codec2.decode(&encoded);
        assert!(result.is_err(), "token from different codec should fail");
    }

    #[test]
    fn test_context_validation() {
        let codec = PageTokenCodec::with_random_key();
        let query_hash = PageTokenCodec::compute_query_hash(b"prefix:user");

        let token = PageToken {
            version: TOKEN_VERSION,
            namespace_id: 42,
            vault_id: 100,
            last_key: b"entity:user:123".to_vec(),
            at_height: 1000,
            query_hash,
        };

        // Matching context should succeed
        assert!(codec.validate_context(&token, 42, 100, query_hash).is_ok());

        // Wrong namespace should fail
        assert_eq!(
            codec.validate_context(&token, 999, 100, query_hash),
            Err(PageTokenError::ContextMismatch)
        );

        // Wrong vault should fail
        assert_eq!(
            codec.validate_context(&token, 42, 999, query_hash),
            Err(PageTokenError::ContextMismatch)
        );

        // Changed query should fail
        let different_hash = PageTokenCodec::compute_query_hash(b"prefix:admin");
        assert_eq!(
            codec.validate_context(&token, 42, 100, different_hash),
            Err(PageTokenError::QueryChanged)
        );
    }

    #[test]
    fn test_query_hash_deterministic() {
        let hash1 = PageTokenCodec::compute_query_hash(b"prefix:user,include_expired:true");
        let hash2 = PageTokenCodec::compute_query_hash(b"prefix:user,include_expired:true");
        let hash3 = PageTokenCodec::compute_query_hash(b"prefix:user,include_expired:false");

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_invalid_base64_rejected() {
        let codec = PageTokenCodec::with_random_key();

        let result = codec.decode("not-valid-base64!!!");
        assert_eq!(result, Err(PageTokenError::InvalidFormat));
    }
}
