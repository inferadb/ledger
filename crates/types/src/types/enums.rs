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

impl UserStatus {
    /// Returns the status as a static string slice.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::PendingOrg => "pending_org",
            Self::Suspended => "suspended",
            Self::Deleting => "deleting",
            Self::Deleted => "deleted",
        }
    }
}

impl fmt::Display for UserStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl UserRole {
    /// Returns the role as a static string slice.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Admin => "admin",
        }
    }
}

impl fmt::Display for UserRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ── UserStatus ──────────────────────────────────────────────────

    #[test]
    fn user_status_display() {
        assert_eq!(UserStatus::Active.to_string(), "active");
        assert_eq!(UserStatus::PendingOrg.to_string(), "pending_org");
        assert_eq!(UserStatus::Suspended.to_string(), "suspended");
        assert_eq!(UserStatus::Deleting.to_string(), "deleting");
        assert_eq!(UserStatus::Deleted.to_string(), "deleted");
    }

    #[test]
    fn user_status_default_is_active() {
        assert_eq!(UserStatus::default(), UserStatus::Active);
    }

    #[test]
    fn user_status_serde_roundtrip() {
        let variants = [
            UserStatus::Active,
            UserStatus::PendingOrg,
            UserStatus::Suspended,
            UserStatus::Deleting,
            UserStatus::Deleted,
        ];
        for v in variants {
            let json = serde_json::to_string(&v).expect("serialize");
            let back: UserStatus = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, v);
        }
    }

    // ── UserRole ────────────────────────────────────────────────────

    #[test]
    fn user_role_display() {
        assert_eq!(UserRole::User.to_string(), "user");
        assert_eq!(UserRole::Admin.to_string(), "admin");
    }

    #[test]
    fn user_role_default_is_user() {
        assert_eq!(UserRole::default(), UserRole::User);
    }

    #[test]
    fn user_role_serde_roundtrip() {
        for v in [UserRole::User, UserRole::Admin] {
            let json = serde_json::to_string(&v).expect("serialize");
            let back: UserRole = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, v);
        }
    }

    // ── OrganizationMemberRole ──────────────────────────────────────

    #[test]
    fn org_member_role_display() {
        assert_eq!(OrganizationMemberRole::Admin.to_string(), "admin");
        assert_eq!(OrganizationMemberRole::Member.to_string(), "member");
    }

    #[test]
    fn org_member_role_default_is_member() {
        assert_eq!(OrganizationMemberRole::default(), OrganizationMemberRole::Member);
    }

    #[test]
    fn org_member_role_serde_roundtrip() {
        for v in [OrganizationMemberRole::Admin, OrganizationMemberRole::Member] {
            let json = serde_json::to_string(&v).expect("serialize");
            let back: OrganizationMemberRole = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, v);
        }
    }

    // ── PrimaryAuthMethod ───────────────────────────────────────────

    #[test]
    fn primary_auth_method_display() {
        assert_eq!(PrimaryAuthMethod::EmailCode.to_string(), "email_code");
        assert_eq!(PrimaryAuthMethod::Passkey.to_string(), "passkey");
    }

    #[test]
    fn primary_auth_method_serde_roundtrip() {
        for v in [PrimaryAuthMethod::EmailCode, PrimaryAuthMethod::Passkey] {
            let json = serde_json::to_string(&v).expect("serialize");
            let back: PrimaryAuthMethod = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, v);
        }
    }

    // ── AppCredentialType ───────────────────────────────────────────

    #[test]
    fn app_credential_type_serde_roundtrip() {
        let variants = [
            AppCredentialType::ClientSecret,
            AppCredentialType::MtlsCa,
            AppCredentialType::MtlsSelfSigned,
            AppCredentialType::ClientAssertion,
        ];
        for v in variants {
            let json = serde_json::to_string(&v).expect("serialize");
            let back: AppCredentialType = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, v);
        }
    }

    // ── SigningKeyScope ─────────────────────────────────────────────

    #[test]
    fn signing_key_scope_organization_carries_id() {
        let scope = SigningKeyScope::Organization(OrganizationId::new(42));
        assert!(matches!(scope, SigningKeyScope::Organization(id) if id.value() == 42));
    }

    #[test]
    fn signing_key_scope_serde_roundtrip() {
        let variants =
            [SigningKeyScope::Global, SigningKeyScope::Organization(OrganizationId::new(7))];
        for v in variants {
            let json = serde_json::to_string(&v).expect("serialize");
            let back: SigningKeyScope = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, v);
        }
    }

    // ── SigningKeyStatus ────────────────────────────────────────────

    #[test]
    fn signing_key_status_serde_roundtrip() {
        for v in [SigningKeyStatus::Active, SigningKeyStatus::Rotated, SigningKeyStatus::Revoked] {
            let json = serde_json::to_string(&v).expect("serialize");
            let back: SigningKeyStatus = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, v);
        }
    }

    // ── WriteStatus ─────────────────────────────────────────────────

    #[test]
    fn write_status_precondition_failed_construction() {
        let status = WriteStatus::PreconditionFailed {
            key: "my-key".to_string(),
            current_version: Some(5),
            current_value: Some(vec![1, 2, 3]),
        };
        assert!(matches!(
            status,
            WriteStatus::PreconditionFailed { ref key, current_version, ref current_value }
                if key == "my-key"
                    && current_version == Some(5)
                    && *current_value == Some(vec![1, 2, 3])
        ));
    }

    #[test]
    fn write_status_serde_roundtrip() {
        let variants = [
            WriteStatus::Created,
            WriteStatus::AlreadyExists,
            WriteStatus::Updated,
            WriteStatus::Deleted,
            WriteStatus::NotFound,
            WriteStatus::PreconditionFailed {
                key: "k".to_string(),
                current_version: None,
                current_value: None,
            },
        ];
        for v in variants {
            let json = serde_json::to_string(&v).expect("serialize");
            let back: WriteStatus = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, v);
        }
    }

    // ── VaultHealth ─────────────────────────────────────────────────

    #[test]
    fn vault_health_diverged_construction() {
        let health =
            VaultHealth::Diverged { expected: [1u8; 32], computed: [2u8; 32], at_height: 99 };
        assert!(matches!(
            health,
            VaultHealth::Diverged { expected, computed, at_height }
                if expected == [1u8; 32]
                    && computed == [2u8; 32]
                    && at_height == 99
        ));
    }

    #[test]
    fn vault_health_serde_roundtrip() {
        let variants = [
            VaultHealth::Healthy,
            VaultHealth::Diverged { expected: [0u8; 32], computed: [1u8; 32], at_height: 10 },
        ];
        for v in variants {
            let json = serde_json::to_string(&v).expect("serialize");
            let back: VaultHealth = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, v);
        }
    }
}
