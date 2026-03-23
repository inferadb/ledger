//! User credential types: passkeys, TOTP, recovery codes.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{PrimaryAuthMethod, UserCredentialId, UserId, UserSlug};

// ============================================================================
// Credential Type Discriminator
// ============================================================================

/// Discriminator for credential types stored in Ledger.
///
/// Each variant corresponds to a distinct authentication mechanism.
/// Email-code authentication is not a credential type — it's the
/// built-in primary auth method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialType {
    /// WebAuthn passkey (FIDO2). Stores public key material.
    Passkey,
    /// Time-based One-Time Password (RFC 6238). At most one per user.
    Totp,
    /// One-time recovery codes for TOTP bypass. At most one set per user.
    RecoveryCode,
}

impl fmt::Display for CredentialType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Passkey => write!(f, "passkey"),
            Self::Totp => write!(f, "totp"),
            Self::RecoveryCode => write!(f, "recovery_code"),
        }
    }
}

// ============================================================================
// TOTP Types
// ============================================================================

/// TOTP hash algorithm (RFC 6238 §5.2).
///
/// SHA1 has the widest authenticator compatibility and is the default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TotpAlgorithm {
    /// HMAC-SHA1 (default, widest authenticator compatibility).
    #[default]
    Sha1,
    /// HMAC-SHA256.
    Sha256,
    /// HMAC-SHA512.
    Sha512,
}

impl fmt::Display for TotpAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sha1 => write!(f, "sha1"),
            Self::Sha256 => write!(f, "sha256"),
            Self::Sha512 => write!(f, "sha512"),
        }
    }
}

// ============================================================================
// Credential Data Structs
// ============================================================================

/// WebAuthn passkey credential data.
///
/// Stores the public key material and metadata from a WebAuthn
/// registration ceremony. The private key never leaves the
/// authenticator device.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PasskeyCredential {
    /// WebAuthn credential ID (opaque, authenticator-generated).
    pub credential_id: Vec<u8>,
    /// COSE-encoded public key for signature verification.
    pub public_key: Vec<u8>,
    /// Monotonic counter for replay protection (updated on each use).
    pub sign_count: u32,
    /// Transport hints from the authenticator (e.g., "internal", "usb", "ble", "nfc").
    pub transports: Vec<String>,
    /// Whether the credential is eligible for multi-device sync.
    pub backup_eligible: bool,
    /// Whether the credential is currently synced across devices.
    pub backup_state: bool,
    /// Attestation statement format (e.g., "packed", "tpm"), if provided.
    pub attestation_format: Option<String>,
    /// Authenticator Attestation GUID identifying the authenticator model
    /// (e.g., YubiKey 5, Touch ID). Used for policy enforcement and admin
    /// visibility. `None` if the authenticator did not provide an AAGUID.
    pub aaguid: Option<[u8; 16]>,
}

/// TOTP credential data (RFC 6238).
///
/// The `secret` field is the shared HMAC key used to generate time-based
/// codes. It is wrapped in [`Zeroizing<Vec<u8>>`](zeroize::Zeroizing) to
/// clear memory on drop. Secrets are write-once — never returned after
/// initial creation.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TotpCredential {
    /// HMAC secret (RFC 6238). Zeroed from memory on drop.
    /// Typically 20 bytes for SHA1, 32 for SHA256, or 64 for SHA512.
    #[serde(with = "zeroize_vec_serde")]
    pub secret: zeroize::Zeroizing<Vec<u8>>,
    /// Hash algorithm for TOTP computation.
    #[serde(default)]
    pub algorithm: TotpAlgorithm,
    /// Number of digits in the generated code (standard: 6).
    #[serde(default = "default_totp_digits")]
    pub digits: u8,
    /// Time step in seconds (standard: 30).
    #[serde(default = "default_totp_period")]
    pub period: u32,
}

impl fmt::Debug for TotpCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TotpCredential")
            .field("secret", &"[REDACTED]")
            .field("algorithm", &self.algorithm)
            .field("digits", &self.digits)
            .field("period", &self.period)
            .finish()
    }
}

const fn default_totp_digits() -> u8 {
    6
}

const fn default_totp_period() -> u32 {
    30
}

/// Recovery code credential data.
///
/// Stores SHA-256 hashes of one-time use recovery codes. Each code
/// is consumed atomically — its hash is removed from the list on use.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryCodeCredential {
    /// SHA-256 hashes of unused recovery codes.
    pub code_hashes: Vec<[u8; 32]>,
    /// Original number of codes generated (e.g., 10).
    pub total_generated: u8,
}

/// Type-specific credential data stored alongside a [`UserCredential`].
///
/// Uses serde's default externally-tagged representation for postcard
/// compatibility (internally-tagged enums are not supported by postcard).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialData {
    /// WebAuthn passkey data.
    Passkey(PasskeyCredential),
    /// TOTP shared secret and parameters.
    Totp(TotpCredential),
    /// Recovery code hashes.
    RecoveryCode(RecoveryCodeCredential),
}

impl CredentialData {
    /// Returns the [`CredentialType`] discriminator for this data variant.
    pub fn credential_type(&self) -> CredentialType {
        match self {
            Self::Passkey(_) => CredentialType::Passkey,
            Self::Totp(_) => CredentialType::Totp,
            Self::RecoveryCode(_) => CredentialType::RecoveryCode,
        }
    }
}

/// A user authentication credential stored in Ledger.
///
/// Each credential has an independent lifecycle — passkeys track
/// `sign_count`, recovery codes are consumed individually, and TOTP
/// credentials are immutable after creation (delete and re-create
/// to change).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserCredential {
    /// Unique credential identifier.
    pub id: UserCredentialId,
    /// Owning user.
    pub user: UserId,
    /// High-level credential type discriminator.
    pub credential_type: CredentialType,
    /// Type-specific credential data.
    pub credential_data: CredentialData,
    /// Human-readable label (e.g., "MacBook Touch ID", "Authenticator app").
    pub name: String,
    /// Whether this credential is active. Disabled credentials are skipped
    /// during authentication but preserved for audit.
    pub enabled: bool,
    /// When this credential was registered.
    pub created_at: DateTime<Utc>,
    /// Last successful authentication using this credential.
    pub last_used_at: Option<DateTime<Utc>>,
}

/// Ephemeral challenge created after primary authentication for a
/// TOTP-enabled user.
///
/// Stored under `_tmp:totp_challenge:{user_id}:{nonce_hex}` with a
/// 5-minute TTL. Consumed atomically by `VerifyTotp` or
/// `ConsumeRecoveryCode` to create a session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingTotpChallenge {
    /// Random one-time-use nonce (32 bytes).
    pub nonce: [u8; 32],
    /// User who completed primary authentication.
    pub user: UserId,
    /// User slug for session creation after TOTP verification.
    pub user_slug: UserSlug,
    /// Absolute expiry time (5-minute TTL from creation).
    pub expires_at: DateTime<Utc>,
    /// Failed TOTP attempts against this challenge (max 3, Raft-persisted).
    pub attempts: u8,
    /// Primary auth method that preceded this challenge.
    pub primary_method: PrimaryAuthMethod,
}

/// Serde helper for `Zeroizing<Vec<u8>>` — serializes as a plain `Vec<u8>`.
mod zeroize_vec_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use zeroize::Zeroizing;

    pub fn serialize<S: Serializer>(value: &Zeroizing<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        value.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Zeroizing<Vec<u8>>, D::Error> {
        Vec::<u8>::deserialize(d).map(Zeroizing::new)
    }
}
