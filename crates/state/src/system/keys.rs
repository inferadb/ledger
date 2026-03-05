//! Key patterns for the `_system` organization.

use inferadb_ledger_types::{
    EmailVerifyTokenId, NodeId, OrganizationId, OrganizationSlug, Region, TeamId, TeamSlug,
    UserEmailId, UserId, UserSlug, VaultSlug,
};

/// Key pattern generators for `_system` organization entities.
///
/// All keys follow the convention `{entity_type}:{id}` for primary keys
/// and `_idx:{index_name}:{value}` for secondary indexes.
pub struct SystemKeys;

impl SystemKeys {
    // ========================================================================
    // User Keys
    // ========================================================================

    /// Primary key for a user record.
    ///
    /// Pattern: `user:{id}`
    ///
    /// # Example
    /// ```
    /// use inferadb_ledger_state::system::SystemKeys;
    /// use inferadb_ledger_types::UserId;
    /// assert_eq!(SystemKeys::user_key(UserId::new(123)), "user:123");
    /// ```
    pub fn user_key(user_id: UserId) -> String {
        format!("user:{}", user_id.value())
    }

    /// Parses a user ID from a user key.
    ///
    /// Returns `None` if the key doesn't match the expected pattern.
    pub fn parse_user_key(key: &str) -> Option<UserId> {
        key.strip_prefix("user:").and_then(|id| id.parse().ok())
    }

    // ========================================================================
    // Email Keys
    // ========================================================================

    /// Primary key for a user email record.
    ///
    /// Pattern: `user_email:{id}`
    pub fn user_email_key(email_id: UserEmailId) -> String {
        format!("user_email:{}", email_id.value())
    }

    /// Index key for email uniqueness lookup.
    ///
    /// Pattern: `_idx:email:{email}` → email_id
    ///
    /// Emails are normalized to lowercase for consistent lookups.
    pub fn email_index_key(email: &str) -> String {
        format!("_idx:email:{}", email.to_lowercase())
    }

    /// Index key for looking up all emails belonging to a user.
    ///
    /// Pattern: `_idx:user_emails:{user_id}` → [email_id, ...]
    pub fn user_emails_index_key(user_id: UserId) -> String {
        format!("_idx:user_emails:{}", user_id.value())
    }

    /// Primary key for an email verification token.
    ///
    /// Pattern: `email_verify:{id}`
    pub fn email_verify_key(token_id: EmailVerifyTokenId) -> String {
        format!("email_verify:{}", token_id.value())
    }

    /// Index key for looking up a verification token by its hash.
    ///
    /// Pattern: `_idx:email_verify_hash:{hex}` → `EmailVerifyTokenId`
    ///
    /// The hex string is the SHA-256 hash of the plaintext token, formatted
    /// as lowercase hex. This index is written when a token is created and
    /// enables constant-time token verification without scanning.
    pub fn email_verify_hash_index_key(token_hash_hex: &str) -> String {
        format!("_idx:email_verify_hash:{token_hash_hex}")
    }

    // ========================================================================
    // User Directory Keys (GLOBAL control plane)
    // ========================================================================

    /// Primary key for a user directory entry in the GLOBAL control plane.
    ///
    /// Pattern: `_sys:user:{id}` → postcard-serialized `UserDirectoryEntry`
    ///
    /// Directory entries contain no PII — only opaque identifiers, region,
    /// status, and timestamp. Enables cross-region user resolution from any node.
    pub fn user_directory_key(user_id: UserId) -> String {
        format!("_sys:user:{}", user_id.value())
    }

    /// Parses a user ID from a user directory key.
    ///
    /// Returns `None` if the key doesn't match `_sys:user:{id}`.
    pub fn parse_user_directory_key(key: &str) -> Option<UserId> {
        key.strip_prefix(Self::USER_DIRECTORY_PREFIX).and_then(|id| id.parse().ok())
    }

    /// Index key for user slug → user ID lookup in the GLOBAL control plane.
    ///
    /// Pattern: `_idx:user:slug:{slug}` → UserId (as string bytes)
    ///
    /// Enables external API slug resolution without knowing the user's region.
    pub fn user_slug_index_key(slug: UserSlug) -> String {
        format!("_idx:user:slug:{}", slug.value())
    }

    // ========================================================================
    // Organization Keys
    // ========================================================================

    /// Primary key for an organization registry entry.
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    ///
    /// Pattern: `org:{organization_id}`
    pub fn organization_key(organization: OrganizationId) -> String {
        format!("org:{}", organization.value())
    }

    /// Parses an organization ID from an organization key.
    pub fn parse_organization_key(key: &str) -> Option<OrganizationId> {
        key.strip_prefix("org:").and_then(|id| id.parse().ok())
    }

    /// Index key for organization slug lookup.
    ///
    /// Pattern: `_idx:org:slug:{slug}` → organization_id
    pub fn organization_slug_key(slug: OrganizationSlug) -> String {
        format!("_idx:org:slug:{}", slug.value())
    }

    /// Primary key for an organization profile record in the regional store.
    ///
    /// Pattern: `_sys:org_profile:{organization_id}` → postcard-serialized `OrganizationProfile`
    ///
    /// Contains PII (organization name). Stored in the region declared at creation.
    pub fn organization_profile_key(organization: OrganizationId) -> String {
        format!("_sys:org_profile:{}", organization.value())
    }

    /// Key for a pending organization profile written by the gRPC handler
    /// before saga creation.
    ///
    /// Pattern: `_sys:pending_org_profile:{saga_id}` → postcard-serialized
    /// `PendingOrganizationProfile`
    ///
    /// Temporary key with TTL; deleted by `WriteOrganizationProfile` on success
    /// or garbage-collected on saga failure.
    pub fn pending_organization_profile_key(saga_id: &str) -> String {
        format!("_sys:pending_org_profile:{saga_id}")
    }

    // ========================================================================
    // Vault Keys
    // ========================================================================

    /// Index key for vault slug lookup.
    ///
    /// Pattern: `_idx:vault:slug:{slug}` → vault_id
    pub fn vault_slug_key(slug: VaultSlug) -> String {
        format!("_idx:vault:slug:{}", slug.value())
    }

    // ========================================================================
    // Team Keys
    // ========================================================================

    /// Primary key for a team profile record in the system vault.
    ///
    /// Pattern: `_sys:team_profile:{organization_id}:{team_id}` → postcard-serialized `TeamProfile`
    pub fn team_profile_key(organization: OrganizationId, team: TeamId) -> String {
        format!("_sys:team_profile:{}:{}", organization.value(), team.value())
    }

    /// Index key for team slug lookup.
    ///
    /// Pattern: `_idx:team:slug:{slug}` → team_id
    pub fn team_slug_key(slug: TeamSlug) -> String {
        format!("_idx:team:slug:{}", slug.value())
    }

    /// Prefix for team profile keys.
    pub const TEAM_PROFILE_PREFIX: &'static str = "_sys:team_profile:";

    // ========================================================================
    // Node Keys
    // ========================================================================

    /// Primary key for a cluster node info record.
    ///
    /// Pattern: `node:{id}`
    pub fn node_key(node_id: &NodeId) -> String {
        format!("node:{node_id}")
    }

    /// Parses a node ID from a node key.
    pub fn parse_node_key(key: &str) -> Option<NodeId> {
        key.strip_prefix("node:").map(|id| id.to_string())
    }

    // ========================================================================
    // Sequence Counter Keys
    // ========================================================================

    /// Key for the organization ID sequence counter.
    ///
    /// Pattern: `_meta:seq:organization` → next OrganizationId (starts at 1, 0 = _system)
    pub const ORG_SEQ_KEY: &'static str = "_meta:seq:organization";

    /// Key for the vault ID sequence counter.
    ///
    /// Pattern: `_meta:seq:vault` → next VaultId
    pub const VAULT_SEQ_KEY: &'static str = "_meta:seq:vault";

    /// Key for the user ID sequence counter.
    ///
    /// Pattern: `_meta:seq:user` → next UserId
    pub const USER_SEQ_KEY: &'static str = "_meta:seq:user";

    /// Key for the user email ID sequence counter.
    ///
    /// Pattern: `_meta:seq:user_email` → next UserEmailId
    pub const USER_EMAIL_SEQ_KEY: &'static str = "_meta:seq:user_email";

    /// Key for the email verification token ID sequence counter.
    ///
    /// Pattern: `_meta:seq:email_verify` → next TokenId
    pub const EMAIL_VERIFY_SEQ_KEY: &'static str = "_meta:seq:email_verify";

    // ========================================================================
    // Saga Keys
    // ========================================================================

    /// Key for a saga state record.
    ///
    /// Pattern: `saga:{saga_id}`
    pub fn saga_key(saga_id: &str) -> String {
        format!("saga:{saga_id}")
    }

    // ========================================================================
    // Email Hash Index Keys (GLOBAL control plane)
    // ========================================================================

    /// Index key for global email uniqueness via HMAC hash.
    ///
    /// Pattern: `_idx:email_hash:{hmac_hex}` → `UserId` (as string bytes)
    ///
    /// The HMAC is computed as `HMAC-SHA256(blinding_key, normalize(email))`.
    /// Only the keyed hash is stored globally — plaintext emails remain in
    /// regional stores.
    pub fn email_hash_index_key(hmac_hex: &str) -> String {
        format!("_idx:email_hash:{hmac_hex}")
    }

    /// Parses an HMAC hex string from an email hash index key.
    ///
    /// Returns `None` if the key doesn't match `_idx:email_hash:{hmac}`.
    pub fn parse_email_hash_index_key(key: &str) -> Option<&str> {
        key.strip_prefix(Self::EMAIL_HASH_INDEX_PREFIX)
    }

    // ========================================================================
    // Blinding Key Metadata Keys (GLOBAL control plane)
    // ========================================================================

    /// Key for the active email blinding key version.
    ///
    /// Pattern: `_meta:email_blinding_key_version` → version number (u32 as string bytes)
    pub const BLINDING_KEY_VERSION_KEY: &'static str = "_meta:email_blinding_key_version";

    /// Key for blinding key rehash progress per region.
    ///
    /// Pattern: `_meta:blinding_key_rehash_progress:{region}` → entries rehashed (u64 as string
    /// bytes)
    ///
    /// Presence of any such key indicates a rotation is in progress.
    /// Removed once rehash completes for that region.
    pub fn rehash_progress_key(region: Region) -> String {
        format!("_meta:blinding_key_rehash_progress:{}", region.as_str())
    }

    /// Parses a region from a rehash progress key.
    ///
    /// Returns `None` if the key doesn't match the expected pattern.
    pub fn parse_rehash_progress_key(key: &str) -> Option<Region> {
        key.strip_prefix(Self::REHASH_PROGRESS_PREFIX).and_then(|r| r.parse().ok())
    }

    // ========================================================================
    // Subject Key Keys (regional store, per-user encryption)
    // ========================================================================

    /// Primary key for a user's per-subject encryption key.
    ///
    /// Pattern: `_key:user:{user_id}` → postcard-serialized `SubjectKey`
    ///
    /// Stored in the regional store where the user's PII resides.
    /// Encrypted at rest by the region's RMK (via `EncryptedBackend`).
    /// Destroying this key makes the user's PII cryptographically unrecoverable.
    pub fn subject_key(user_id: UserId) -> String {
        format!("_key:user:{}", user_id.value())
    }

    /// Parses a user ID from a subject key.
    ///
    /// Returns `None` if the key doesn't match `_key:user:{id}`.
    pub fn parse_subject_key(key: &str) -> Option<UserId> {
        key.strip_prefix(Self::SUBJECT_KEY_PREFIX).and_then(|id| id.parse().ok())
    }

    // ========================================================================
    // Erasure Audit Keys (GLOBAL control plane)
    // ========================================================================

    /// Key for an erasure audit record.
    ///
    /// Pattern: `_audit:erasure:{user_id}` → postcard-serialized `ErasureAuditRecord`
    ///
    /// Stored in the GLOBAL control plane. Uses insert-if-absent (CAS) for
    /// idempotent crash-resume: re-executing the erasure sequence after a
    /// crash will not create a duplicate audit record.
    pub fn erasure_audit_key(user_id: UserId) -> String {
        format!("_audit:erasure:{}", user_id.value())
    }

    /// Parses a user ID from an erasure audit key.
    ///
    /// Returns `None` if the key doesn't match `_audit:erasure:{id}`.
    pub fn parse_erasure_audit_key(key: &str) -> Option<UserId> {
        key.strip_prefix(Self::ERASURE_AUDIT_PREFIX).and_then(|id| id.parse().ok())
    }

    // ========================================================================
    // Key Prefixes (for scanning)
    // ========================================================================

    /// Prefix for all user keys.
    pub const USER_PREFIX: &'static str = "user:";

    /// Prefix for all user email keys.
    pub const USER_EMAIL_PREFIX: &'static str = "user_email:";

    /// Prefix for email verification hash index entries.
    pub const EMAIL_VERIFY_HASH_INDEX_PREFIX: &'static str = "_idx:email_verify_hash:";

    /// Prefix for all organization keys.
    pub const ORG_PREFIX: &'static str = "org:";

    /// Prefix for all node keys.
    pub const NODE_PREFIX: &'static str = "node:";

    /// Prefix for all saga keys.
    pub const SAGA_PREFIX: &'static str = "saga:";

    /// Prefix for all user directory entries in the GLOBAL control plane.
    pub const USER_DIRECTORY_PREFIX: &'static str = "_sys:user:";

    /// Prefix for all directory entries (`_sys:` namespace).
    pub const SYS_PREFIX: &'static str = "_sys:";

    /// Prefix for user slug index entries.
    pub const USER_SLUG_INDEX_PREFIX: &'static str = "_idx:user:slug:";

    /// Prefix for organization profile keys.
    pub const ORG_PROFILE_PREFIX: &'static str = "_sys:org_profile:";

    /// Prefix for pending organization profile keys.
    pub const PENDING_ORG_PROFILE_PREFIX: &'static str = "_sys:pending_org_profile:";

    /// Prefix for all index keys.
    pub const INDEX_PREFIX: &'static str = "_idx:";

    /// Prefix for all metadata keys.
    pub const META_PREFIX: &'static str = "_meta:";

    /// Prefix for email hash index entries.
    pub const EMAIL_HASH_INDEX_PREFIX: &'static str = "_idx:email_hash:";

    /// Prefix for blinding key rehash progress entries.
    pub const REHASH_PROGRESS_PREFIX: &'static str = "_meta:blinding_key_rehash_progress:";

    /// Prefix for per-subject encryption keys.
    pub const SUBJECT_KEY_PREFIX: &'static str = "_key:user:";

    /// Prefix for erasure audit records.
    pub const ERASURE_AUDIT_PREFIX: &'static str = "_audit:erasure:";
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_user_key() {
        assert_eq!(SystemKeys::user_key(UserId::new(123)), "user:123");
        assert_eq!(SystemKeys::parse_user_key("user:123"), Some(UserId::new(123)));
        assert_eq!(SystemKeys::parse_user_key("invalid:123"), None);
    }

    #[test]
    fn test_email_index_key() {
        // Should normalize to lowercase
        assert_eq!(
            SystemKeys::email_index_key("Alice@Example.COM"),
            "_idx:email:alice@example.com"
        );
    }

    #[test]
    fn test_organization_key() {
        assert_eq!(SystemKeys::organization_key(OrganizationId::new(42)), "org:42");
        assert_eq!(SystemKeys::parse_organization_key("org:42"), Some(OrganizationId::new(42)));
    }

    #[test]
    fn test_organization_profile_key() {
        assert_eq!(
            SystemKeys::organization_profile_key(OrganizationId::new(42)),
            "_sys:org_profile:42"
        );
        assert!(
            SystemKeys::organization_profile_key(OrganizationId::new(1))
                .starts_with(SystemKeys::ORG_PROFILE_PREFIX)
        );
        assert!(
            SystemKeys::organization_profile_key(OrganizationId::new(1))
                .starts_with(SystemKeys::SYS_PREFIX)
        );
    }

    #[test]
    fn test_pending_organization_profile_key() {
        assert_eq!(
            SystemKeys::pending_organization_profile_key("saga-abc"),
            "_sys:pending_org_profile:saga-abc"
        );
        assert!(
            SystemKeys::pending_organization_profile_key("x")
                .starts_with(SystemKeys::PENDING_ORG_PROFILE_PREFIX)
        );
        assert!(
            SystemKeys::pending_organization_profile_key("x").starts_with(SystemKeys::SYS_PREFIX)
        );
    }

    #[test]
    fn test_org_profile_does_not_collide_with_pending() {
        let profile_key = SystemKeys::organization_profile_key(OrganizationId::new(42));
        let pending_key = SystemKeys::pending_organization_profile_key("42");
        assert_ne!(profile_key, pending_key);
    }

    #[test]
    fn test_organization_slug_key() {
        assert_eq!(
            SystemKeys::organization_slug_key(OrganizationSlug::new(12345)),
            "_idx:org:slug:12345"
        );
    }

    #[test]
    fn test_vault_slug_key() {
        assert_eq!(SystemKeys::vault_slug_key(VaultSlug::new(67890)), "_idx:vault:slug:67890");
    }

    #[test]
    fn test_node_key() {
        assert_eq!(SystemKeys::node_key(&"node-1".to_string()), "node:node-1");
        assert_eq!(SystemKeys::parse_node_key("node:node-1"), Some("node-1".to_string()));
    }

    #[test]
    fn test_sequence_keys() {
        assert_eq!(SystemKeys::ORG_SEQ_KEY, "_meta:seq:organization");
        assert_eq!(SystemKeys::USER_SEQ_KEY, "_meta:seq:user");
    }

    #[test]
    fn test_saga_key() {
        assert_eq!(SystemKeys::saga_key("create-org-abc123"), "saga:create-org-abc123");
    }

    #[test]
    fn test_prefixes() {
        assert!(SystemKeys::user_key(UserId::new(1)).starts_with(SystemKeys::USER_PREFIX));
        assert!(
            SystemKeys::organization_key(OrganizationId::new(1))
                .starts_with(SystemKeys::ORG_PREFIX)
        );
        assert!(SystemKeys::node_key(&"n".to_string()).starts_with(SystemKeys::NODE_PREFIX));
    }

    #[test]
    fn test_user_directory_key() {
        assert_eq!(SystemKeys::user_directory_key(UserId::new(42)), "_sys:user:42");
        assert_eq!(SystemKeys::parse_user_directory_key("_sys:user:42"), Some(UserId::new(42)));
        assert_eq!(SystemKeys::parse_user_directory_key("user:42"), None);
        assert_eq!(SystemKeys::parse_user_directory_key("_sys:user:abc"), None);
    }

    #[test]
    fn test_user_slug_index_key() {
        assert_eq!(SystemKeys::user_slug_index_key(UserSlug::new(99999)), "_idx:user:slug:99999");
    }

    #[test]
    fn test_user_directory_prefixes() {
        assert!(
            SystemKeys::user_directory_key(UserId::new(1))
                .starts_with(SystemKeys::USER_DIRECTORY_PREFIX)
        );
        assert!(SystemKeys::user_directory_key(UserId::new(1)).starts_with(SystemKeys::SYS_PREFIX));
        assert!(
            SystemKeys::user_slug_index_key(UserSlug::new(1))
                .starts_with(SystemKeys::USER_SLUG_INDEX_PREFIX)
        );
        assert!(
            SystemKeys::user_slug_index_key(UserSlug::new(1)).starts_with(SystemKeys::INDEX_PREFIX)
        );
    }

    #[test]
    fn test_user_directory_key_does_not_collide_with_user_key() {
        // _sys:user:42 (directory) vs user:42 (regional record) are distinct
        let directory_key = SystemKeys::user_directory_key(UserId::new(42));
        let user_key = SystemKeys::user_key(UserId::new(42));
        assert_ne!(directory_key, user_key);
        assert!(directory_key.starts_with("_sys:"));
        assert!(!user_key.starts_with("_sys:"));
    }

    #[test]
    fn test_email_hash_index_key() {
        let hmac = "a1b2c3d4e5f6".repeat(5);
        let key = SystemKeys::email_hash_index_key(&hmac);
        assert!(key.starts_with(SystemKeys::EMAIL_HASH_INDEX_PREFIX));
        assert!(key.starts_with(SystemKeys::INDEX_PREFIX));
        assert_eq!(SystemKeys::parse_email_hash_index_key(&key), Some(hmac.as_str()));
    }

    #[test]
    fn test_email_hash_index_key_parse_invalid() {
        assert_eq!(SystemKeys::parse_email_hash_index_key("_idx:email:foo"), None);
        assert_eq!(SystemKeys::parse_email_hash_index_key("random:key"), None);
    }

    #[test]
    fn test_blinding_key_version_key() {
        assert!(SystemKeys::BLINDING_KEY_VERSION_KEY.starts_with(SystemKeys::META_PREFIX));
    }

    #[test]
    fn test_rehash_progress_key() {
        let key = SystemKeys::rehash_progress_key(Region::US_EAST_VA);
        assert!(key.starts_with(SystemKeys::REHASH_PROGRESS_PREFIX));
        assert!(key.starts_with(SystemKeys::META_PREFIX));
        assert!(key.contains("us-east-va"));
        assert_eq!(SystemKeys::parse_rehash_progress_key(&key), Some(Region::US_EAST_VA));
    }

    #[test]
    fn test_rehash_progress_key_parse_invalid() {
        assert_eq!(SystemKeys::parse_rehash_progress_key("_meta:other:key"), None);
    }

    #[test]
    fn test_email_hash_index_does_not_collide_with_email_index() {
        // _idx:email_hash:{hmac} (global) vs _idx:email:{email} (regional) are distinct
        let hash_key = SystemKeys::email_hash_index_key("abc123");
        let email_key = SystemKeys::email_index_key("alice@example.com");
        assert_ne!(hash_key, email_key);
        assert!(hash_key.starts_with("_idx:email_hash:"));
        assert!(email_key.starts_with("_idx:email:"));
    }
}
