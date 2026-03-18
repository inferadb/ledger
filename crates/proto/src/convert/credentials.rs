//! Credential conversions: `CredentialType`, `TotpAlgorithm`, `PasskeyCredential`,
//! `TotpCredential`, `RecoveryCodeCredential`, `UserCredential`, and helper functions.

use inferadb_ledger_types::{
    CredentialType, PasskeyCredential, RecoveryCodeCredential, TotpAlgorithm, TotpCredential,
    UserCredential, UserSlug,
};
use tonic::Status;

use super::domain::datetime_to_proto_timestamp;
use crate::proto;

// =============================================================================
// CredentialType conversions (domain <-> proto)
// =============================================================================

/// Converts a domain [`CredentialType`] to the proto enum value.
impl From<CredentialType> for proto::CredentialType {
    fn from(ct: CredentialType) -> Self {
        match ct {
            CredentialType::Passkey => proto::CredentialType::Passkey,
            CredentialType::Totp => proto::CredentialType::Totp,
            CredentialType::RecoveryCode => proto::CredentialType::RecoveryCode,
        }
    }
}

/// Converts a proto [`CredentialType`](proto::CredentialType) to the domain type.
impl TryFrom<proto::CredentialType> for CredentialType {
    type Error = Status;

    fn try_from(proto_ct: proto::CredentialType) -> Result<Self, Self::Error> {
        match proto_ct {
            proto::CredentialType::Passkey => Ok(CredentialType::Passkey),
            proto::CredentialType::Totp => Ok(CredentialType::Totp),
            proto::CredentialType::RecoveryCode => Ok(CredentialType::RecoveryCode),
            proto::CredentialType::Unspecified => {
                Err(Status::invalid_argument("credential type must be specified"))
            },
        }
    }
}

/// Validates and converts an `i32` wire value to a proto [`CredentialType`](proto::CredentialType).
/// Rejects `UNSPECIFIED` and unknown values.
pub fn credential_type_from_i32(value: i32) -> Result<proto::CredentialType, Status> {
    let ct = proto::CredentialType::try_from(value)
        .map_err(|_| Status::invalid_argument(format!("unknown credential type value: {value}")))?;
    match ct {
        proto::CredentialType::Unspecified => {
            Err(Status::invalid_argument("credential type must be specified"))
        },
        _ => Ok(ct),
    }
}

// =============================================================================
// TotpAlgorithm conversions (domain <-> proto)
// =============================================================================

/// Converts a domain [`TotpAlgorithm`] to the proto enum value.
impl From<TotpAlgorithm> for proto::TotpAlgorithm {
    fn from(alg: TotpAlgorithm) -> Self {
        match alg {
            TotpAlgorithm::Sha1 => proto::TotpAlgorithm::Sha1,
            TotpAlgorithm::Sha256 => proto::TotpAlgorithm::Sha256,
            TotpAlgorithm::Sha512 => proto::TotpAlgorithm::Sha512,
        }
    }
}

/// Converts a proto [`TotpAlgorithm`](proto::TotpAlgorithm) to the domain type.
impl From<proto::TotpAlgorithm> for TotpAlgorithm {
    fn from(proto_alg: proto::TotpAlgorithm) -> Self {
        match proto_alg {
            proto::TotpAlgorithm::Sha1 => TotpAlgorithm::Sha1,
            proto::TotpAlgorithm::Sha256 => TotpAlgorithm::Sha256,
            proto::TotpAlgorithm::Sha512 => TotpAlgorithm::Sha512,
        }
    }
}

/// Validates and converts an `i32` wire value to a proto [`TotpAlgorithm`](proto::TotpAlgorithm).
///
/// Unlike [`credential_type_from_i32`], there is no `UNSPECIFIED` sentinel — value 0 maps to
/// `SHA1` (widest authenticator compatibility). An omitted field on the wire is therefore
/// treated as SHA-1, which is the intended default.
pub fn totp_algorithm_from_i32(value: i32) -> Result<proto::TotpAlgorithm, Status> {
    proto::TotpAlgorithm::try_from(value)
        .map_err(|_| Status::invalid_argument(format!("unknown TOTP algorithm value: {value}")))
}

// =============================================================================
// PasskeyCredential conversions (domain <-> proto)
// =============================================================================

/// Converts a domain [`PasskeyCredential`] to proto
/// [`PasskeyCredentialData`](proto::PasskeyCredentialData).
impl From<&PasskeyCredential> for proto::PasskeyCredentialData {
    fn from(pk: &PasskeyCredential) -> Self {
        proto::PasskeyCredentialData {
            credential_id: pk.credential_id.clone(),
            public_key: pk.public_key.clone(),
            sign_count: pk.sign_count,
            transports: pk.transports.clone(),
            backup_eligible: pk.backup_eligible,
            backup_state: pk.backup_state,
            attestation_format: pk.attestation_format.clone(),
            aaguid: pk.aaguid.map(|a| a.to_vec()),
        }
    }
}

/// Converts proto [`PasskeyCredentialData`](proto::PasskeyCredentialData) to a domain
/// [`PasskeyCredential`].
impl TryFrom<&proto::PasskeyCredentialData> for PasskeyCredential {
    type Error = Status;

    fn try_from(proto_pk: &proto::PasskeyCredentialData) -> Result<Self, Self::Error> {
        if proto_pk.credential_id.is_empty() {
            return Err(Status::invalid_argument("passkey credential_id must not be empty"));
        }
        if proto_pk.public_key.is_empty() {
            return Err(Status::invalid_argument("passkey public_key must not be empty"));
        }
        let aaguid = if let Some(ref bytes) = proto_pk.aaguid {
            let arr: [u8; 16] = bytes
                .as_slice()
                .try_into()
                .map_err(|_| Status::invalid_argument("aaguid must be exactly 16 bytes"))?;
            Some(arr)
        } else {
            None
        };
        Ok(PasskeyCredential {
            credential_id: proto_pk.credential_id.clone(),
            public_key: proto_pk.public_key.clone(),
            sign_count: proto_pk.sign_count,
            transports: proto_pk.transports.clone(),
            backup_eligible: proto_pk.backup_eligible,
            backup_state: proto_pk.backup_state,
            attestation_format: proto_pk.attestation_format.clone(),
            aaguid,
        })
    }
}

// =============================================================================
// TotpCredential conversions (domain <-> proto)
// =============================================================================

/// Converts a domain [`TotpCredential`] to proto [`TotpCredentialData`](proto::TotpCredentialData).
impl From<&TotpCredential> for proto::TotpCredentialData {
    fn from(totp: &TotpCredential) -> Self {
        proto::TotpCredentialData {
            secret: totp.secret.to_vec(),
            algorithm: proto::TotpAlgorithm::from(totp.algorithm).into(),
            digits: u32::from(totp.digits),
            period: totp.period,
        }
    }
}

/// Converts proto [`TotpCredentialData`](proto::TotpCredentialData) to a domain [`TotpCredential`].
impl TryFrom<&proto::TotpCredentialData> for TotpCredential {
    type Error = Status;

    fn try_from(proto_totp: &proto::TotpCredentialData) -> Result<Self, Self::Error> {
        if proto_totp.secret.is_empty() {
            return Err(Status::invalid_argument("TOTP secret must not be empty"));
        }
        let algorithm = totp_algorithm_from_i32(proto_totp.algorithm)?;
        let digits = u8::try_from(proto_totp.digits)
            .map_err(|_| Status::invalid_argument("TOTP digits out of range"))?;
        if digits != 6 && digits != 8 {
            return Err(Status::invalid_argument("TOTP digits must be 6 or 8"));
        }
        if proto_totp.period == 0 {
            return Err(Status::invalid_argument("TOTP period must be non-zero"));
        }
        Ok(TotpCredential {
            secret: zeroize::Zeroizing::new(proto_totp.secret.clone()),
            algorithm: algorithm.into(),
            digits,
            period: proto_totp.period,
        })
    }
}

// =============================================================================
// RecoveryCodeCredential conversions (domain <-> proto)
// =============================================================================

/// Converts a domain [`RecoveryCodeCredential`] to proto
/// [`RecoveryCodeCredentialData`](proto::RecoveryCodeCredentialData).
impl From<&RecoveryCodeCredential> for proto::RecoveryCodeCredentialData {
    fn from(rc: &RecoveryCodeCredential) -> Self {
        proto::RecoveryCodeCredentialData {
            code_hashes: rc.code_hashes.iter().map(|h| h.to_vec()).collect(),
            total_generated: u32::from(rc.total_generated),
        }
    }
}

/// Converts proto [`RecoveryCodeCredentialData`](proto::RecoveryCodeCredentialData) to a domain
/// [`RecoveryCodeCredential`].
impl TryFrom<&proto::RecoveryCodeCredentialData> for RecoveryCodeCredential {
    type Error = Status;

    fn try_from(proto_rc: &proto::RecoveryCodeCredentialData) -> Result<Self, Self::Error> {
        if proto_rc.code_hashes.is_empty() {
            return Err(Status::invalid_argument(
                "recovery code credential must contain at least one code hash",
            ));
        }
        let code_hashes = proto_rc
            .code_hashes
            .iter()
            .map(|h| {
                let arr: [u8; 32] = h.as_slice().try_into().map_err(|_| {
                    Status::invalid_argument("recovery code hash must be exactly 32 bytes")
                })?;
                Ok(arr)
            })
            .collect::<Result<Vec<[u8; 32]>, Status>>()?;
        Ok(RecoveryCodeCredential {
            code_hashes,
            total_generated: u8::try_from(proto_rc.total_generated)
                .map_err(|_| Status::invalid_argument("total_generated must be 0-255"))?,
        })
    }
}

// =============================================================================
// UserCredential conversions (domain <-> proto)
// =============================================================================

/// Converts a domain [`UserCredential`] to a proto [`UserCredential`](proto::UserCredential).
///
/// The `user` field is set to `None` because `UserId` → `UserSlug` resolution
/// happens in the service layer via `SlugResolver`. Callers must set the
/// `user` field on the returned proto message.
pub fn user_credential_to_proto(
    cred: &UserCredential,
    user_slug: UserSlug,
) -> proto::UserCredential {
    use inferadb_ledger_types::CredentialData;

    let data = match &cred.credential_data {
        CredentialData::Passkey(pk) => Some(proto::user_credential::Data::Passkey(pk.into())),
        CredentialData::Totp(totp) => Some(proto::user_credential::Data::Totp(totp.into())),
        CredentialData::RecoveryCode(rc) => {
            Some(proto::user_credential::Data::RecoveryCode(rc.into()))
        },
    };

    proto::UserCredential {
        id: cred.id.value(),
        user: Some(proto::UserSlug::from(user_slug)),
        credential_type: proto::CredentialType::from(cred.credential_type).into(),
        name: cred.name.clone(),
        enabled: cred.enabled,
        created_at: Some(datetime_to_proto_timestamp(&cred.created_at)),
        last_used_at: cred.last_used_at.as_ref().map(datetime_to_proto_timestamp),
        data,
    }
}

/// Strips the TOTP secret from a proto [`UserCredential`](proto::UserCredential) for safe
/// return to API consumers. The secret is write-once and never returned after creation.
pub fn strip_totp_secret(cred: &mut proto::UserCredential) {
    if let Some(proto::user_credential::Data::Totp(ref mut totp)) = cred.data {
        totp.secret.clear();
    }
}

/// Strips recovery code hashes from a proto [`UserCredential`](proto::UserCredential).
/// Hashes are SHA-256 of short alphanumeric codes and must not be exposed to API
/// consumers (offline brute-force risk). The remaining count is preserved via the
/// vector length before clearing.
pub fn strip_recovery_code_hashes(cred: &mut proto::UserCredential) {
    if let Some(proto::user_credential::Data::RecoveryCode(ref mut rc)) = cred.data {
        rc.code_hashes.clear();
    }
}

// =============================================================================
// CredentialInfo conversions (domain string <-> proto)
// =============================================================================

/// Validates a [`CredentialInfo`](proto::CredentialInfo) primary method string.
/// Valid values: `"passkey"`, `"email_code"`, `"recovery_code"`.
pub fn validate_credential_info_type(credential_type: &str) -> Result<&str, Status> {
    match credential_type {
        "passkey" | "email_code" | "recovery_code" => Ok(credential_type),
        _ => Err(Status::invalid_argument("invalid credential type in CredentialInfo")),
    }
}
