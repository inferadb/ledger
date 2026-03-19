//! SDK types for user authentication credentials.
//!
//! Provides consumer-facing types for passkey, TOTP, and recovery code
//! credentials. These types are decoupled from protobuf and present
//! idiomatic Rust APIs to SDK consumers.

use std::time::SystemTime;

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{UserCredentialId, UserSlug};

use crate::proto_util::proto_timestamp_to_system_time;

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

    /// Converts from the proto `CredentialType` i32 wire value.
    pub(crate) fn from_proto_i32(value: i32) -> Option<Self> {
        match proto::CredentialType::try_from(value).ok()? {
            proto::CredentialType::Passkey => Some(Self::Passkey),
            proto::CredentialType::Totp => Some(Self::Totp),
            proto::CredentialType::RecoveryCode => Some(Self::RecoveryCode),
            proto::CredentialType::Unspecified => None,
        }
    }

    /// Converts to the proto `CredentialType` i32 wire value.
    pub(crate) fn to_proto_i32(self) -> i32 {
        let proto_ct = match self {
            Self::Passkey => proto::CredentialType::Passkey,
            Self::Totp => proto::CredentialType::Totp,
            Self::RecoveryCode => proto::CredentialType::RecoveryCode,
        };
        proto_ct.into()
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

impl TotpAlgorithm {
    /// Converts from the proto `TotpAlgorithm` i32 wire value.
    pub(crate) fn from_proto_i32(value: i32) -> Self {
        match proto::TotpAlgorithm::try_from(value) {
            Ok(proto::TotpAlgorithm::Sha256) => Self::Sha256,
            Ok(proto::TotpAlgorithm::Sha512) => Self::Sha512,
            // SHA-1 is the default (value 0) per RFC 6238.
            _ => Self::Sha1,
        }
    }

    /// Converts to the proto `TotpAlgorithm` i32 wire value.
    pub(crate) fn to_proto_i32(self) -> i32 {
        let proto_alg = match self {
            Self::Sha1 => proto::TotpAlgorithm::Sha1,
            Self::Sha256 => proto::TotpAlgorithm::Sha256,
            Self::Sha512 => proto::TotpAlgorithm::Sha512,
        };
        proto_alg.into()
    }
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

impl PasskeyCredentialInfo {
    pub(crate) fn from_proto(p: &proto::PasskeyCredentialData) -> Self {
        Self {
            credential_id: p.credential_id.clone(),
            public_key: p.public_key.clone(),
            sign_count: p.sign_count,
            transports: p.transports.clone(),
            backup_eligible: p.backup_eligible,
            backup_state: p.backup_state,
            attestation_format: p.attestation_format.clone(),
            aaguid: p.aaguid.clone(),
        }
    }

    pub(crate) fn to_proto(&self) -> proto::PasskeyCredentialData {
        proto::PasskeyCredentialData {
            credential_id: self.credential_id.clone(),
            public_key: self.public_key.clone(),
            sign_count: self.sign_count,
            transports: self.transports.clone(),
            backup_eligible: self.backup_eligible,
            backup_state: self.backup_state,
            attestation_format: self.attestation_format.clone(),
            aaguid: self.aaguid.clone(),
        }
    }
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

impl TotpCredentialInfo {
    pub(crate) fn from_proto(p: &proto::TotpCredentialData) -> Self {
        Self {
            secret: p.secret.clone(),
            algorithm: TotpAlgorithm::from_proto_i32(p.algorithm),
            digits: p.digits,
            period: p.period,
        }
    }

    pub(crate) fn to_proto(&self) -> proto::TotpCredentialData {
        proto::TotpCredentialData {
            secret: self.secret.clone(),
            algorithm: self.algorithm.to_proto_i32(),
            digits: self.digits,
            period: self.period,
        }
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

impl RecoveryCodeCredentialInfo {
    pub(crate) fn from_proto(p: &proto::RecoveryCodeCredentialData) -> Self {
        Self { code_hashes: p.code_hashes.clone(), total_generated: p.total_generated }
    }

    pub(crate) fn to_proto(&self) -> proto::RecoveryCodeCredentialData {
        proto::RecoveryCodeCredentialData {
            code_hashes: self.code_hashes.clone(),
            total_generated: self.total_generated,
        }
    }
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

impl UserCredentialInfo {
    /// Converts a proto `UserCredential` to the SDK type.
    pub(crate) fn from_proto(p: &proto::UserCredential) -> Self {
        let data = p.data.as_ref().map(|d| match d {
            proto::user_credential::Data::Passkey(pk) => {
                CredentialData::Passkey(PasskeyCredentialInfo::from_proto(pk))
            },
            proto::user_credential::Data::Totp(totp) => {
                CredentialData::Totp(TotpCredentialInfo::from_proto(totp))
            },
            proto::user_credential::Data::RecoveryCode(rc) => {
                CredentialData::RecoveryCode(RecoveryCodeCredentialInfo::from_proto(rc))
            },
        });

        Self {
            id: UserCredentialId::new(p.id),
            user: UserSlug::new(p.user.as_ref().map_or(0, |s| s.slug)),
            credential_type: CredentialType::from_proto_i32(p.credential_type)
                .unwrap_or(CredentialType::Passkey),
            name: p.name.clone(),
            enabled: p.enabled,
            created_at: p.created_at.as_ref().and_then(proto_timestamp_to_system_time),
            last_used_at: p.last_used_at.as_ref().and_then(proto_timestamp_to_system_time),
            data,
        }
    }
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
    fn credential_type_roundtrip() {
        for ct in [CredentialType::Passkey, CredentialType::Totp, CredentialType::RecoveryCode] {
            let i32_val = ct.to_proto_i32();
            let back = CredentialType::from_proto_i32(i32_val).unwrap();
            assert_eq!(ct, back);
        }
    }

    #[test]
    fn credential_type_unspecified_returns_none() {
        assert!(CredentialType::from_proto_i32(0).is_none());
    }

    #[test]
    fn credential_type_invalid_returns_none() {
        assert!(CredentialType::from_proto_i32(999).is_none());
    }

    #[test]
    fn totp_algorithm_roundtrip() {
        for alg in [TotpAlgorithm::Sha1, TotpAlgorithm::Sha256, TotpAlgorithm::Sha512] {
            let i32_val = alg.to_proto_i32();
            let back = TotpAlgorithm::from_proto_i32(i32_val);
            assert_eq!(alg, back);
        }
    }

    #[test]
    fn totp_algorithm_defaults_to_sha1() {
        // Invalid values default to SHA-1 (widest authenticator compat)
        assert_eq!(TotpAlgorithm::from_proto_i32(999), TotpAlgorithm::Sha1);
    }

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
    fn passkey_credential_info_proto_roundtrip() {
        let info = PasskeyCredentialInfo {
            credential_id: vec![1, 2, 3],
            public_key: vec![4, 5, 6],
            sign_count: 10,
            transports: vec!["internal".to_string(), "usb".to_string()],
            backup_eligible: true,
            backup_state: false,
            attestation_format: Some("packed".to_string()),
            aaguid: Some(vec![0u8; 16]),
        };
        let proto = info.to_proto();
        let back = PasskeyCredentialInfo::from_proto(&proto);
        assert_eq!(info, back);
    }

    #[test]
    fn totp_credential_info_proto_roundtrip() {
        let info = TotpCredentialInfo {
            secret: vec![42; 20],
            algorithm: TotpAlgorithm::Sha256,
            digits: 8,
            period: 60,
        };
        let proto = info.to_proto();
        let back = TotpCredentialInfo::from_proto(&proto);
        assert_eq!(info, back);
    }

    #[test]
    fn recovery_code_credential_info_proto_roundtrip() {
        let info = RecoveryCodeCredentialInfo {
            code_hashes: vec![vec![1u8; 32], vec![2u8; 32]],
            total_generated: 10,
        };
        let proto = info.to_proto();
        let back = RecoveryCodeCredentialInfo::from_proto(&proto);
        assert_eq!(info, back);
    }

    #[test]
    fn user_credential_info_from_proto_passkey() {
        let proto_cred = proto::UserCredential {
            id: 42,
            user: Some(proto::UserSlug { slug: 999 }),
            credential_type: proto::CredentialType::Passkey.into(),
            name: "Touch ID".to_string(),
            enabled: true,
            created_at: Some(prost_types::Timestamp { seconds: 1700000000, nanos: 0 }),
            last_used_at: None,
            data: Some(proto::user_credential::Data::Passkey(proto::PasskeyCredentialData {
                credential_id: vec![1],
                public_key: vec![2],
                sign_count: 5,
                transports: vec!["internal".to_string()],
                backup_eligible: false,
                backup_state: false,
                attestation_format: None,
                aaguid: None,
            })),
        };

        let info = UserCredentialInfo::from_proto(&proto_cred);
        assert_eq!(info.id, UserCredentialId::new(42));
        assert_eq!(info.user.value(), 999);
        assert_eq!(info.credential_type, CredentialType::Passkey);
        assert_eq!(info.name, "Touch ID");
        assert!(info.enabled);
        assert!(info.created_at.is_some());
        assert!(info.last_used_at.is_none());
        assert!(matches!(info.data, Some(CredentialData::Passkey(_))));
    }

    #[test]
    fn user_credential_info_from_proto_totp() {
        let proto_cred = proto::UserCredential {
            id: 10,
            user: Some(proto::UserSlug { slug: 555 }),
            credential_type: proto::CredentialType::Totp.into(),
            name: "Authenticator".to_string(),
            enabled: true,
            created_at: Some(prost_types::Timestamp { seconds: 1700000000, nanos: 0 }),
            last_used_at: Some(prost_types::Timestamp { seconds: 1700001000, nanos: 0 }),
            data: Some(proto::user_credential::Data::Totp(proto::TotpCredentialData {
                secret: vec![], // Stripped by server
                algorithm: proto::TotpAlgorithm::Sha1.into(),
                digits: 6,
                period: 30,
            })),
        };

        let info = UserCredentialInfo::from_proto(&proto_cred);
        assert_eq!(info.credential_type, CredentialType::Totp);
        assert!(info.last_used_at.is_some());
        if let Some(CredentialData::Totp(totp)) = &info.data {
            assert!(totp.secret.is_empty());
            assert_eq!(totp.algorithm, TotpAlgorithm::Sha1);
        } else {
            unreachable!("expected TOTP data");
        }
    }

    #[test]
    fn user_credential_info_from_proto_no_data() {
        let proto_cred = proto::UserCredential {
            id: 1,
            credential_type: proto::CredentialType::Passkey.into(),
            data: None,
            ..Default::default()
        };

        let info = UserCredentialInfo::from_proto(&proto_cred);
        assert!(info.data.is_none());
    }
}
