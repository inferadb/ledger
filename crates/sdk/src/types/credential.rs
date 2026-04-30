//! SDK types for user authentication credentials.
//!
//! Provides consumer-facing types for passkey, TOTP, and recovery code
//! credentials. These types are decoupled from protobuf and present
//! idiomatic Rust APIs to SDK consumers.

use std::time::SystemTime;

use inferadb_ledger_types::{UserCredentialId, UserSlug};

// =============================================================================
// Credential Type
// =============================================================================

/// The type of authentication credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum CredentialType {
    /// WebAuthn passkey (FIDO2).
    Passkey,
    /// Time-based one-time password (RFC 6238).
    Totp,
    /// One-time recovery codes for TOTP bypass.
    RecoveryCode,
}

impl CredentialType {
    /// Derives the credential type from the data variant.
    pub fn from_data(data: &CredentialData) -> Self {
        match data {
            CredentialData::Passkey(_) => Self::Passkey,
            CredentialData::Totp(_) => Self::Totp,
            CredentialData::RecoveryCode(_) => Self::RecoveryCode,
        }
    }
}

// =============================================================================
// TOTP Algorithm
// =============================================================================

/// TOTP hash algorithm (RFC 6238).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum TotpAlgorithm {
    /// SHA-1 (default, widest authenticator compatibility).
    Sha1,
    /// SHA-256.
    Sha256,
    /// SHA-512.
    Sha512,
}

// =============================================================================
// Credential Data (type-specific fields)
// =============================================================================

/// WebAuthn passkey credential data.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PasskeyCredentialInfo {
    /// WebAuthn credential ID.
    pub credential_id: Vec<u8>,
    /// COSE-encoded public key.
    pub public_key: Vec<u8>,
    /// Replay protection counter.
    pub sign_count: u32,
    /// Supported transports: `"internal"`, `"usb"`, `"ble"`, `"nfc"`.
    pub transports: Vec<String>,
    /// Whether the credential is eligible for multi-device sync.
    pub backup_eligible: bool,
    /// Whether the credential is currently synced across devices.
    pub backup_state: bool,
    /// WebAuthn attestation statement format identifier.
    pub attestation_format: Option<String>,
    /// 16-byte AAGUID identifying the authenticator model.
    pub aaguid: Option<Vec<u8>>,
}

/// TOTP credential data (RFC 6238).
///
/// The `secret` field is only populated on the initial create response.
/// All subsequent reads return an empty secret (stripped by the server).
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TotpCredentialInfo {
    /// 20-byte HMAC secret (only present on create response).
    pub secret: Vec<u8>,
    /// Hash algorithm.
    pub algorithm: TotpAlgorithm,
    /// Number of digits in the code (6 or 8).
    pub digits: u32,
    /// Time step in seconds (typically 30).
    pub period: u32,
}

impl std::fmt::Debug for TotpCredentialInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TotpCredentialInfo")
            .field("secret", &"[REDACTED]")
            .field("algorithm", &self.algorithm)
            .field("digits", &self.digits)
            .field("period", &self.period)
            .finish()
    }
}

/// Recovery code credential data.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RecoveryCodeCredentialInfo {
    /// SHA-256 hashes of unused recovery codes.
    pub code_hashes: Vec<Vec<u8>>,
    /// Original number of codes generated.
    pub total_generated: u32,
}

/// Type-specific credential data.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum CredentialData {
    /// WebAuthn passkey data.
    Passkey(PasskeyCredentialInfo),
    /// TOTP data (secret redacted after creation).
    Totp(TotpCredentialInfo),
    /// Recovery code data.
    RecoveryCode(RecoveryCodeCredentialInfo),
}

// =============================================================================
// User Credential Info
// =============================================================================

/// A user authentication credential as returned by the Ledger API.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UserCredentialInfo {
    /// Credential identifier.
    pub id: UserCredentialId,
    /// The user who owns this credential.
    pub user: UserSlug,
    /// Credential type discriminant.
    pub credential_type: CredentialType,
    /// Human-readable name (e.g., "MacBook Touch ID", "Authenticator app").
    pub name: String,
    /// Whether the credential is currently active.
    pub enabled: bool,
    /// When the credential was created.
    pub created_at: Option<SystemTime>,
    /// When the credential was last used for authentication.
    pub last_used_at: Option<SystemTime>,
    /// Type-specific credential data.
    pub data: Option<CredentialData>,
}

// =============================================================================
// Recovery Code Consumption Result
// =============================================================================

/// Result of consuming a recovery code.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RecoveryCodeResult {
    /// The session tokens created after recovery code verification.
    pub tokens: crate::token::TokenPair,
    /// Number of unused recovery codes remaining.
    pub remaining_codes: u32,
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::disallowed_methods)]

    use super::*;

    #[test]
    fn totp_credential_info_debug_redacts_secret() {
        let info = TotpCredentialInfo {
            secret: vec![42; 20],
            algorithm: TotpAlgorithm::Sha1,
            digits: 6,
            period: 30,
        };
        let debug = format!("{info:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("42"));
    }

    #[test]
    fn credential_type_from_data() {
        let passkey = CredentialData::Passkey(PasskeyCredentialInfo {
            credential_id: vec![],
            public_key: vec![],
            sign_count: 0,
            transports: vec![],
            backup_eligible: false,
            backup_state: false,
            attestation_format: None,
            aaguid: None,
        });
        assert_eq!(CredentialType::from_data(&passkey), CredentialType::Passkey);

        let totp = CredentialData::Totp(TotpCredentialInfo {
            secret: vec![],
            algorithm: TotpAlgorithm::Sha1,
            digits: 6,
            period: 30,
        });
        assert_eq!(CredentialType::from_data(&totp), CredentialType::Totp);

        let rc = CredentialData::RecoveryCode(RecoveryCodeCredentialInfo {
            code_hashes: vec![],
            total_generated: 0,
        });
        assert_eq!(CredentialType::from_data(&rc), CredentialType::RecoveryCode);
    }
}
