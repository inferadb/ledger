//! Status and role enums for users, organizations, apps, and signing keys.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::OrganizationId;

// ============================================================================
// User Types
// ============================================================================

/// User account lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserStatus {
    /// User can authenticate.
    #[default]
    Active,
    /// Pending organization creation (saga in progress).
    PendingOrg,
    /// User cannot authenticate.
    Suspended,
    /// Deletion cascade in progress.
    Deleting,
    /// Tombstone for audit.
    Deleted,
}

/// User authorization role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserRole {
    /// Regular user (default).
    #[default]
    User,
    /// Global service administrator.
    Admin,
}

/// Role within an organization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrganizationMemberRole {
    /// Organization administrator — can manage members and settings.
    Admin,
    /// Regular organization member.
    #[default]
    Member,
}

impl fmt::Display for UserStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::PendingOrg => write!(f, "pending_org"),
            Self::Suspended => write!(f, "suspended"),
            Self::Deleting => write!(f, "deleting"),
            Self::Deleted => write!(f, "deleted"),
        }
    }
}

impl fmt::Display for UserRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::User => write!(f, "user"),
            Self::Admin => write!(f, "admin"),
        }
    }
}

impl fmt::Display for OrganizationMemberRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admin => write!(f, "admin"),
            Self::Member => write!(f, "member"),
        }
    }
}

// ============================================================================
// App Types
// ============================================================================

/// Credential type discriminator for application authentication methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppCredentialType {
    /// Client secret credential.
    ClientSecret,
    /// CA-signed mTLS credential.
    MtlsCa,
    /// Self-signed mTLS credential.
    MtlsSelfSigned,
    /// Client assertion (private key JWT) credential type-level toggle.
    ClientAssertion,
}

// ============================================================================
// Signing Key Types
// ============================================================================

/// Scope of a signing key. Sum type with data to eliminate invalid states
/// (e.g., `Global` with an org ID, or `Organization` without one).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SigningKeyScope {
    /// Global signing key used for user session tokens.
    Global,
    /// Per-organization signing key used for vault access tokens.
    Organization(OrganizationId),
}

/// Lifecycle status of a signing key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SigningKeyStatus {
    /// Current signing key for its scope. Used for both signing and verification.
    Active,
    /// Previous key after rotation. Valid for verification only during grace period.
    Rotated,
    /// Permanently invalidated. Cannot be used for signing or verification.
    Revoked,
}

// ============================================================================
// Write Status
// ============================================================================

/// Outcome status of a single operation within a write request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteStatus {
    /// Entity/relationship was created.
    Created,
    /// Entity/relationship already existed (idempotent).
    AlreadyExists,
    /// Entity/relationship was updated.
    Updated,
    /// Entity/relationship was deleted.
    Deleted,
    /// Entity/relationship was not found.
    NotFound,
    /// Precondition failed (for conditional writes).
    /// Contains details about the current state for client-side conflict resolution.
    PreconditionFailed {
        /// The key that failed the condition check.
        key: String,
        /// Current version of the entity (block height when last modified), if it exists.
        current_version: Option<u64>,
        /// Current value of the entity, if it exists.
        current_value: Option<Vec<u8>>,
    },
}

// ============================================================================
// Vault Health
// ============================================================================

/// Health status of a vault.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VaultHealth {
    /// Vault is operating normally.
    Healthy,
    /// Vault has diverged from expected state.
    Diverged {
        /// Expected state root hash.
        expected: crate::hash::Hash,
        /// Computed state root hash.
        computed: crate::hash::Hash,
        /// Height at which divergence was detected.
        at_height: u64,
    },
}

// ============================================================================
// Primary Auth Method
// ============================================================================

/// Primary authentication method that preceded a TOTP challenge.
///
/// Distinct from [`super::CredentialType`] because email-code authentication
/// is not a stored credential — it's the built-in primary auth method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrimaryAuthMethod {
    /// Passwordless email-code verification (built-in, no stored credential).
    EmailCode,
    /// WebAuthn passkey authentication.
    Passkey,
}

impl fmt::Display for PrimaryAuthMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmailCode => write!(f, "email_code"),
            Self::Passkey => write!(f, "passkey"),
        }
    }
}
