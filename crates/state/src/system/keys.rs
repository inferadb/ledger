//! Key patterns for the `_system` organization.

use std::fmt::Write;

use inferadb_ledger_types::{
    AppId, AppSlug, ClientAssertionId, EmailVerifyTokenId, NodeId, OrganizationId,
    OrganizationSlug, RefreshTokenId, Region, SigningKeyId, TeamId, TeamSlug, TokenSubject,
    UserEmailId, UserId, UserSlug, VaultId, VaultSlug,
};

use super::types::SigningKeyScope;

/// Encodes a byte slice as lowercase hexadecimal.
fn encode_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // write! to a String is infallible
        let _ = write!(s, "{b:02x}");
    }
    s
}

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
    // App Keys
    // ========================================================================

    /// Primary key for an application record in the system vault.
    ///
    /// Pattern: `_sys:app:{organization_id}:{app_id}` → postcard-serialized `App`
    pub fn app_key(organization: OrganizationId, app: AppId) -> String {
        format!("_sys:app:{}:{}", organization.value(), app.value())
    }

    /// Prefix for listing all apps in an organization.
    ///
    /// Pattern: `_sys:app:{organization_id}:`
    pub fn app_prefix(organization: OrganizationId) -> String {
        format!("_sys:app:{}:", organization.value())
    }

    /// Index key for app slug lookup.
    ///
    /// Pattern: `_idx:app:slug:{slug}` → `{org_id}:{app_id}`
    pub fn app_slug_key(slug: AppSlug) -> String {
        format!("_idx:app:slug:{}", slug.value())
    }

    /// Index key for app name uniqueness within an organization.
    ///
    /// Pattern: `_idx:app:name:{org_id}:{name}` → app_id
    pub fn app_name_index_key(organization: OrganizationId, name: &str) -> String {
        format!("_idx:app:name:{}:{}", organization.value(), name)
    }

    /// Prefix for listing all app name index entries for an organization.
    ///
    /// Pattern: `_idx:app:name:{org_id}:`
    pub fn app_name_index_prefix(organization: OrganizationId) -> String {
        format!("_idx:app:name:{}:", organization.value())
    }

    /// Primary key for a client assertion entry.
    ///
    /// Pattern: `_sys:app_assertion:{org_id}:{app_id}:{assertion_id}`
    pub fn app_assertion_key(
        organization: OrganizationId,
        app: AppId,
        assertion: ClientAssertionId,
    ) -> String {
        format!("_sys:app_assertion:{}:{}:{}", organization.value(), app.value(), assertion.value())
    }

    /// Prefix for listing all client assertion entries for an app.
    ///
    /// Pattern: `_sys:app_assertion:{org_id}:{app_id}:`
    pub fn app_assertion_prefix(organization: OrganizationId, app: AppId) -> String {
        format!("_sys:app_assertion:{}:{}:", organization.value(), app.value())
    }

    /// Primary key for a vault connection for an app.
    ///
    /// Pattern: `_sys:app_vault:{org_id}:{app_id}:{vault_id}`
    pub fn app_vault_key(organization: OrganizationId, app: AppId, vault: VaultId) -> String {
        format!("_sys:app_vault:{}:{}:{}", organization.value(), app.value(), vault.value())
    }

    /// Prefix for listing all vault connections for an app.
    ///
    /// Pattern: `_sys:app_vault:{org_id}:{app_id}:`
    pub fn app_vault_prefix(organization: OrganizationId, app: AppId) -> String {
        format!("_sys:app_vault:{}:{}:", organization.value(), app.value())
    }

    /// Prefix for app keys.
    pub const APP_PREFIX: &'static str = "_sys:app:";

    /// Prefix for app slug index entries.
    pub const APP_SLUG_INDEX_PREFIX: &'static str = "_idx:app:slug:";

    /// Prefix for app name index entries.
    pub const APP_NAME_INDEX_PREFIX: &'static str = "_idx:app:name:";

    /// Prefix for app assertion entries.
    pub const APP_ASSERTION_PREFIX: &'static str = "_sys:app_assertion:";

    /// Prefix for app vault connection entries.
    pub const APP_VAULT_PREFIX: &'static str = "_sys:app_vault:";

    /// Key for the app ID sequence counter.
    ///
    /// Pattern: `_meta:seq:app` → next AppId
    pub const APP_SEQ_KEY: &'static str = "_meta:seq:app";

    /// Key for the client assertion ID sequence counter.
    ///
    /// Pattern: `_meta:seq:client_assertion` → next ClientAssertionId
    pub const CLIENT_ASSERTION_SEQ_KEY: &'static str = "_meta:seq:client_assertion";

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

    // ========================================================================
    // Signing Key Keys
    // ========================================================================

    /// Primary key for a signing key record.
    ///
    /// Pattern: `_sys:signing_key:{id}`
    pub fn signing_key(id: SigningKeyId) -> String {
        format!("_sys:signing_key:{}", id.value())
    }

    /// Parses a signing key ID from a signing key primary key.
    ///
    /// Returns `None` if the key doesn't match `_sys:signing_key:{id}`.
    pub fn parse_signing_key(key: &str) -> Option<SigningKeyId> {
        key.strip_prefix(Self::SIGNING_KEY_PREFIX).and_then(|id| id.parse().ok())
    }

    /// Index key for signing key lookup by kid (UUID key identifier).
    ///
    /// Pattern: `_idx:signing_key:kid:{kid}` → `SigningKeyId`
    pub fn signing_key_kid_index(kid: &str) -> String {
        format!("_idx:signing_key:kid:{kid}")
    }

    /// Index key for the current active signing key of a given scope.
    ///
    /// Pattern: `_idx:signing_key:scope:global` or
    /// `_idx:signing_key:scope:org:{org_id}`
    ///
    /// Points to the current active key's kid. Replaced atomically on rotation.
    pub fn signing_key_scope_index(scope: &SigningKeyScope) -> String {
        match scope {
            SigningKeyScope::Global => "_idx:signing_key:scope:global".to_string(),
            SigningKeyScope::Organization(org_id) => {
                format!("_idx:signing_key:scope:org:{}", org_id.value())
            },
        }
    }

    /// Prefix for iterating all signing keys belonging to an organization.
    ///
    /// Pattern: `_sys:signing_key:org:{org_id}:`
    ///
    /// Used for org deletion cascade — iterates all keys for an org.
    pub fn signing_key_org_prefix(org: OrganizationId) -> String {
        format!("_sys:signing_key:org:{}:", org.value())
    }

    /// Prefix for all signing key primary keys.
    pub const SIGNING_KEY_PREFIX: &'static str = "_sys:signing_key:";

    /// Prefix for signing key kid index entries.
    pub const SIGNING_KEY_KID_INDEX_PREFIX: &'static str = "_idx:signing_key:kid:";

    /// Prefix for signing key scope index entries.
    pub const SIGNING_KEY_SCOPE_INDEX_PREFIX: &'static str = "_idx:signing_key:scope:";

    /// Prefix for signing key org index entries.
    pub const SIGNING_KEY_ORG_PREFIX: &'static str = "_sys:signing_key:org:";

    /// Key for the signing key ID sequence counter.
    ///
    /// Pattern: `_meta:seq:signing_key` → next `SigningKeyId`
    pub const SIGNING_KEY_SEQ_KEY: &'static str = "_meta:seq:signing_key";

    // ========================================================================
    // Refresh Token Keys
    // ========================================================================

    /// Primary key for a refresh token record.
    ///
    /// Pattern: `_sys:refresh_token:{id}`
    pub fn refresh_token(id: RefreshTokenId) -> String {
        format!("_sys:refresh_token:{}", id.value())
    }

    /// Parses a refresh token ID from a refresh token primary key.
    ///
    /// Returns `None` if the key doesn't match `_sys:refresh_token:{id}`.
    pub fn parse_refresh_token(key: &str) -> Option<RefreshTokenId> {
        key.strip_prefix(Self::REFRESH_TOKEN_PREFIX).and_then(|id| id.parse().ok())
    }

    /// Index key for refresh token lookup by SHA-256 hash.
    ///
    /// Pattern: `_idx:refresh_token:hash:{hex}` → `RefreshTokenId`
    pub fn refresh_token_hash_index(hash: &[u8; 32]) -> String {
        format!("_idx:refresh_token:hash:{}", encode_hex(hash))
    }

    /// Prefix for iterating all refresh tokens in a token family.
    ///
    /// Pattern: `_idx:refresh_token:family:{hex}:`
    pub fn refresh_token_family_prefix(family: &[u8; 16]) -> String {
        format!("_idx:refresh_token:family:{}:", encode_hex(family))
    }

    /// Index entry for a refresh token within its family.
    ///
    /// Pattern: `_idx:refresh_token:family:{hex}:{id}` → `RefreshTokenId`
    pub fn refresh_token_family_entry(family: &[u8; 16], id: RefreshTokenId) -> String {
        format!("_idx:refresh_token:family:{}:{}", encode_hex(family), id.value())
    }

    /// Prefix for iterating all refresh tokens belonging to a subject.
    ///
    /// Pattern: `_idx:refresh_token:user:{slug}:` or
    /// `_idx:refresh_token:app:{slug}:`
    pub fn refresh_token_subject_prefix(subject: &TokenSubject) -> String {
        match subject {
            TokenSubject::User(slug) => {
                format!("_idx:refresh_token:user:{}:", slug.value())
            },
            TokenSubject::App(slug) => {
                format!("_idx:refresh_token:app:{}:", slug.value())
            },
        }
    }

    /// Index entry for a refresh token within its subject index.
    ///
    /// Pattern: `_idx:refresh_token:{user|app}:{slug}:{id}` → `RefreshTokenId`
    pub fn refresh_token_subject_entry(subject: &TokenSubject, id: RefreshTokenId) -> String {
        match subject {
            TokenSubject::User(slug) => {
                format!("_idx:refresh_token:user:{}:{}", slug.value(), id.value())
            },
            TokenSubject::App(slug) => {
                format!("_idx:refresh_token:app:{}:{}", slug.value(), id.value())
            },
        }
    }

    /// Prefix for iterating all refresh tokens for a specific app+vault pair.
    ///
    /// Pattern: `_idx:refresh_token:app_vault:{app_slug}:{vault_id}:`
    ///
    /// Used for vault disconnect cascade — avoids O(n) scan of all app tokens.
    pub fn refresh_token_app_vault_prefix(app: AppSlug, vault: VaultId) -> String {
        format!("_idx:refresh_token:app_vault:{}:{}:", app.value(), vault.value())
    }

    /// Index entry for a refresh token within its app+vault index.
    ///
    /// Pattern: `_idx:refresh_token:app_vault:{app_slug}:{vault_id}:{id}`
    pub fn refresh_token_app_vault_entry(
        app: AppSlug,
        vault: VaultId,
        id: RefreshTokenId,
    ) -> String {
        format!("_idx:refresh_token:app_vault:{}:{}:{}", app.value(), vault.value(), id.value())
    }

    /// Marker key for a poisoned token family (reuse detected).
    ///
    /// Pattern: `_idx:refresh_token:family_poisoned:{hex}`
    ///
    /// Presence of this key means the family is poisoned. O(1) check in
    /// `UseRefreshToken`. Background `TokenMaintenanceJob` garbage-collects
    /// poisoned families.
    pub fn refresh_token_family_poisoned(family: &[u8; 16]) -> String {
        format!("_idx:refresh_token:family_poisoned:{}", encode_hex(family))
    }

    /// Prefix for all refresh token primary keys.
    pub const REFRESH_TOKEN_PREFIX: &'static str = "_sys:refresh_token:";

    /// Prefix for refresh token hash index entries.
    pub const REFRESH_TOKEN_HASH_INDEX_PREFIX: &'static str = "_idx:refresh_token:hash:";

    /// Prefix for refresh token family index entries.
    pub const REFRESH_TOKEN_FAMILY_PREFIX: &'static str = "_idx:refresh_token:family:";

    /// Prefix for poisoned family markers.
    pub const REFRESH_TOKEN_FAMILY_POISONED_PREFIX: &'static str =
        "_idx:refresh_token:family_poisoned:";

    /// Prefix for refresh token app_vault index entries.
    pub const REFRESH_TOKEN_APP_VAULT_PREFIX: &'static str = "_idx:refresh_token:app_vault:";

    /// Key for the refresh token ID sequence counter.
    ///
    /// Pattern: `_meta:seq:refresh_token` → next `RefreshTokenId`
    pub const REFRESH_TOKEN_SEQ_KEY: &'static str = "_meta:seq:refresh_token";
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_key_generation() {
        let cases: Vec<(&str, String, &str)> = vec![
            ("user_key", SystemKeys::user_key(UserId::new(123)), "user:123"),
            (
                "email_index_key (normalized)",
                SystemKeys::email_index_key("Alice@Example.COM"),
                "_idx:email:alice@example.com",
            ),
            ("organization_key", SystemKeys::organization_key(OrganizationId::new(42)), "org:42"),
            (
                "organization_profile_key",
                SystemKeys::organization_profile_key(OrganizationId::new(42)),
                "_sys:org_profile:42",
            ),
            (
                "pending_organization_profile_key",
                SystemKeys::pending_organization_profile_key("saga-abc"),
                "_sys:pending_org_profile:saga-abc",
            ),
            (
                "organization_slug_key",
                SystemKeys::organization_slug_key(OrganizationSlug::new(12345)),
                "_idx:org:slug:12345",
            ),
            (
                "vault_slug_key",
                SystemKeys::vault_slug_key(VaultSlug::new(67890)),
                "_idx:vault:slug:67890",
            ),
            ("node_key", SystemKeys::node_key(&"node-1".to_string()), "node:node-1"),
            ("saga_key", SystemKeys::saga_key("create-org-abc123"), "saga:create-org-abc123"),
            ("user_directory_key", SystemKeys::user_directory_key(UserId::new(42)), "_sys:user:42"),
            (
                "user_slug_index_key",
                SystemKeys::user_slug_index_key(UserSlug::new(99999)),
                "_idx:user:slug:99999",
            ),
        ];
        for (label, actual, expected) in &cases {
            assert_eq!(actual, expected, "{label}");
        }
    }

    #[test]
    fn test_parse_key_roundtrips() {
        assert_eq!(SystemKeys::parse_user_key("user:123"), Some(UserId::new(123)));
        assert_eq!(SystemKeys::parse_user_key("invalid:123"), None);
        assert_eq!(SystemKeys::parse_organization_key("org:42"), Some(OrganizationId::new(42)));
        assert_eq!(SystemKeys::parse_node_key("node:node-1"), Some("node-1".to_string()));
        assert_eq!(SystemKeys::parse_user_directory_key("_sys:user:42"), Some(UserId::new(42)));
        assert_eq!(SystemKeys::parse_user_directory_key("user:42"), None);
        assert_eq!(SystemKeys::parse_user_directory_key("_sys:user:abc"), None);
    }

    #[test]
    fn test_sequence_keys() {
        assert_eq!(SystemKeys::ORG_SEQ_KEY, "_meta:seq:organization");
        assert_eq!(SystemKeys::USER_SEQ_KEY, "_meta:seq:user");
    }

    #[test]
    fn test_prefix_consistency() {
        let cases: Vec<(&str, String, &[&str])> = vec![
            ("user_key", SystemKeys::user_key(UserId::new(1)), &[SystemKeys::USER_PREFIX]),
            (
                "organization_key",
                SystemKeys::organization_key(OrganizationId::new(1)),
                &[SystemKeys::ORG_PREFIX],
            ),
            ("node_key", SystemKeys::node_key(&"n".to_string()), &[SystemKeys::NODE_PREFIX]),
            (
                "organization_profile_key",
                SystemKeys::organization_profile_key(OrganizationId::new(1)),
                &[SystemKeys::ORG_PROFILE_PREFIX, SystemKeys::SYS_PREFIX],
            ),
            (
                "pending_organization_profile_key",
                SystemKeys::pending_organization_profile_key("x"),
                &[SystemKeys::PENDING_ORG_PROFILE_PREFIX, SystemKeys::SYS_PREFIX],
            ),
            (
                "user_directory_key",
                SystemKeys::user_directory_key(UserId::new(1)),
                &[SystemKeys::USER_DIRECTORY_PREFIX, SystemKeys::SYS_PREFIX],
            ),
            (
                "user_slug_index_key",
                SystemKeys::user_slug_index_key(UserSlug::new(1)),
                &[SystemKeys::USER_SLUG_INDEX_PREFIX, SystemKeys::INDEX_PREFIX],
            ),
        ];
        for (label, key, prefixes) in &cases {
            for prefix in *prefixes {
                assert!(key.starts_with(prefix), "{label} should start with {prefix}");
            }
        }
    }

    #[test]
    fn test_org_profile_does_not_collide_with_pending() {
        let profile_key = SystemKeys::organization_profile_key(OrganizationId::new(42));
        let pending_key = SystemKeys::pending_organization_profile_key("42");
        assert_ne!(profile_key, pending_key);
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

    // =========================================================================
    // App key tests
    // =========================================================================

    #[test]
    fn test_app_key_generation() {
        let cases: Vec<(&str, String, &str)> = vec![
            (
                "app_key",
                SystemKeys::app_key(OrganizationId::new(5), AppId::new(42)),
                "_sys:app:5:42",
            ),
            ("app_prefix", SystemKeys::app_prefix(OrganizationId::new(5)), "_sys:app:5:"),
            ("app_slug_key", SystemKeys::app_slug_key(AppSlug::new(9999)), "_idx:app:slug:9999"),
            (
                "app_name_index_key",
                SystemKeys::app_name_index_key(OrganizationId::new(3), "my-app"),
                "_idx:app:name:3:my-app",
            ),
            (
                "app_name_index_prefix",
                SystemKeys::app_name_index_prefix(OrganizationId::new(3)),
                "_idx:app:name:3:",
            ),
            (
                "app_assertion_key",
                SystemKeys::app_assertion_key(
                    OrganizationId::new(1),
                    AppId::new(2),
                    ClientAssertionId::new(3),
                ),
                "_sys:app_assertion:1:2:3",
            ),
            (
                "app_assertion_prefix",
                SystemKeys::app_assertion_prefix(OrganizationId::new(1), AppId::new(2)),
                "_sys:app_assertion:1:2:",
            ),
            (
                "app_vault_key",
                SystemKeys::app_vault_key(OrganizationId::new(1), AppId::new(2), VaultId::new(3)),
                "_sys:app_vault:1:2:3",
            ),
            (
                "app_vault_prefix",
                SystemKeys::app_vault_prefix(OrganizationId::new(1), AppId::new(2)),
                "_sys:app_vault:1:2:",
            ),
        ];
        for (label, actual, expected) in &cases {
            assert_eq!(actual, expected, "{label}");
        }
    }

    #[test]
    fn test_app_prefix_consistency() {
        let cases: Vec<(&str, String, &[&str])> = vec![
            (
                "app_key",
                SystemKeys::app_key(OrganizationId::new(5), AppId::new(42)),
                &[SystemKeys::APP_PREFIX, "_sys:app:5:"],
            ),
            (
                "app_slug_key",
                SystemKeys::app_slug_key(AppSlug::new(9999)),
                &[SystemKeys::APP_SLUG_INDEX_PREFIX],
            ),
            (
                "app_name_index_key",
                SystemKeys::app_name_index_key(OrganizationId::new(3), "my-app"),
                &[SystemKeys::APP_NAME_INDEX_PREFIX, "_idx:app:name:3:"],
            ),
            (
                "app_assertion_key",
                SystemKeys::app_assertion_key(
                    OrganizationId::new(1),
                    AppId::new(2),
                    ClientAssertionId::new(7),
                ),
                &[SystemKeys::APP_ASSERTION_PREFIX, "_sys:app_assertion:1:2:"],
            ),
            (
                "app_vault_key",
                SystemKeys::app_vault_key(OrganizationId::new(1), AppId::new(2), VaultId::new(9)),
                &[SystemKeys::APP_VAULT_PREFIX, "_sys:app_vault:1:2:"],
            ),
        ];
        for (label, key, prefixes) in &cases {
            for prefix in *prefixes {
                assert!(key.starts_with(prefix), "{label} should start with {prefix}");
            }
        }
    }

    #[test]
    fn test_app_key_isolation_across_organizations() {
        // Same app ID in different orgs must produce different keys
        let key_a = SystemKeys::app_key(OrganizationId::new(1), AppId::new(10));
        let key_b = SystemKeys::app_key(OrganizationId::new(2), AppId::new(10));
        assert_ne!(key_a, key_b);
    }

    #[test]
    fn test_app_prefixes_do_not_collide() {
        // Ensure app, assertion, and vault prefixes are distinct namespaces
        assert_ne!(SystemKeys::APP_PREFIX, SystemKeys::APP_ASSERTION_PREFIX);
        assert_ne!(SystemKeys::APP_PREFIX, SystemKeys::APP_VAULT_PREFIX);
        assert_ne!(SystemKeys::APP_ASSERTION_PREFIX, SystemKeys::APP_VAULT_PREFIX);
    }

    // =========================================================================
    // Signing key tests
    // =========================================================================

    #[test]
    fn test_signing_key_generation() {
        let cases: Vec<(&str, String, &str)> = vec![
            ("signing_key", SystemKeys::signing_key(SigningKeyId::new(7)), "_sys:signing_key:7"),
            (
                "signing_key_kid_index",
                SystemKeys::signing_key_kid_index("550e8400-e29b-41d4-a716-446655440000"),
                "_idx:signing_key:kid:550e8400-e29b-41d4-a716-446655440000",
            ),
            (
                "signing_key_scope_index_global",
                SystemKeys::signing_key_scope_index(&SigningKeyScope::Global),
                "_idx:signing_key:scope:global",
            ),
            (
                "signing_key_scope_index_org",
                SystemKeys::signing_key_scope_index(&SigningKeyScope::Organization(
                    OrganizationId::new(42),
                )),
                "_idx:signing_key:scope:org:42",
            ),
            (
                "signing_key_org_prefix",
                SystemKeys::signing_key_org_prefix(OrganizationId::new(42)),
                "_sys:signing_key:org:42:",
            ),
        ];
        for (label, actual, expected) in &cases {
            assert_eq!(actual, expected, "{label}");
        }
    }

    #[test]
    fn test_signing_key_parse_roundtrip() {
        assert_eq!(SystemKeys::parse_signing_key("_sys:signing_key:7"), Some(SigningKeyId::new(7)));
        assert_eq!(SystemKeys::parse_signing_key("_sys:signing_key:abc"), None);
        assert_eq!(SystemKeys::parse_signing_key("other:7"), None);
    }

    #[test]
    fn test_signing_key_prefix_consistency() {
        let cases: Vec<(&str, String, &[&str])> = vec![
            (
                "signing_key",
                SystemKeys::signing_key(SigningKeyId::new(1)),
                &[SystemKeys::SIGNING_KEY_PREFIX, SystemKeys::SYS_PREFIX],
            ),
            (
                "signing_key_kid_index",
                SystemKeys::signing_key_kid_index("abc"),
                &[SystemKeys::SIGNING_KEY_KID_INDEX_PREFIX, SystemKeys::INDEX_PREFIX],
            ),
            (
                "signing_key_scope_index_global",
                SystemKeys::signing_key_scope_index(&SigningKeyScope::Global),
                &[SystemKeys::SIGNING_KEY_SCOPE_INDEX_PREFIX, SystemKeys::INDEX_PREFIX],
            ),
            (
                "signing_key_scope_index_org",
                SystemKeys::signing_key_scope_index(&SigningKeyScope::Organization(
                    OrganizationId::new(1),
                )),
                &[SystemKeys::SIGNING_KEY_SCOPE_INDEX_PREFIX, SystemKeys::INDEX_PREFIX],
            ),
            (
                "signing_key_org_prefix",
                SystemKeys::signing_key_org_prefix(OrganizationId::new(1)),
                &[SystemKeys::SIGNING_KEY_ORG_PREFIX, SystemKeys::SYS_PREFIX],
            ),
        ];
        for (label, key, prefixes) in &cases {
            for prefix in *prefixes {
                assert!(key.starts_with(prefix), "{label} should start with {prefix}");
            }
        }
    }

    #[test]
    fn test_signing_key_sequence_key() {
        assert_eq!(SystemKeys::SIGNING_KEY_SEQ_KEY, "_meta:seq:signing_key");
        assert!(SystemKeys::SIGNING_KEY_SEQ_KEY.starts_with(SystemKeys::META_PREFIX));
    }

    #[test]
    fn test_signing_key_scope_index_isolation() {
        // Global and org scope indexes must produce different keys
        let global = SystemKeys::signing_key_scope_index(&SigningKeyScope::Global);
        let org = SystemKeys::signing_key_scope_index(&SigningKeyScope::Organization(
            OrganizationId::new(1),
        ));
        assert_ne!(global, org);
    }

    // =========================================================================
    // Refresh token tests
    // =========================================================================

    #[test]
    fn test_refresh_token_key_generation() {
        let hash: [u8; 32] = [0xab; 32];
        let family: [u8; 16] = [0xcd; 16];

        let cases: Vec<(&str, String, &str)> = vec![
            (
                "refresh_token",
                SystemKeys::refresh_token(RefreshTokenId::new(99)),
                "_sys:refresh_token:99",
            ),
            (
                "refresh_token_hash_index",
                SystemKeys::refresh_token_hash_index(&hash),
                "_idx:refresh_token:hash:abababababababababababababababababababababababababababababababab",
            ),
            (
                "refresh_token_family_prefix",
                SystemKeys::refresh_token_family_prefix(&family),
                "_idx:refresh_token:family:cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd:",
            ),
            (
                "refresh_token_family_entry",
                SystemKeys::refresh_token_family_entry(&family, RefreshTokenId::new(5)),
                "_idx:refresh_token:family:cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd:5",
            ),
            (
                "refresh_token_subject_prefix_user",
                SystemKeys::refresh_token_subject_prefix(&TokenSubject::User(UserSlug::new(42))),
                "_idx:refresh_token:user:42:",
            ),
            (
                "refresh_token_subject_prefix_app",
                SystemKeys::refresh_token_subject_prefix(&TokenSubject::App(AppSlug::new(99))),
                "_idx:refresh_token:app:99:",
            ),
            (
                "refresh_token_subject_entry_user",
                SystemKeys::refresh_token_subject_entry(
                    &TokenSubject::User(UserSlug::new(42)),
                    RefreshTokenId::new(7),
                ),
                "_idx:refresh_token:user:42:7",
            ),
            (
                "refresh_token_subject_entry_app",
                SystemKeys::refresh_token_subject_entry(
                    &TokenSubject::App(AppSlug::new(99)),
                    RefreshTokenId::new(3),
                ),
                "_idx:refresh_token:app:99:3",
            ),
            (
                "refresh_token_app_vault_prefix",
                SystemKeys::refresh_token_app_vault_prefix(AppSlug::new(10), VaultId::new(20)),
                "_idx:refresh_token:app_vault:10:20:",
            ),
            (
                "refresh_token_app_vault_entry",
                SystemKeys::refresh_token_app_vault_entry(
                    AppSlug::new(10),
                    VaultId::new(20),
                    RefreshTokenId::new(30),
                ),
                "_idx:refresh_token:app_vault:10:20:30",
            ),
            (
                "refresh_token_family_poisoned",
                SystemKeys::refresh_token_family_poisoned(&family),
                "_idx:refresh_token:family_poisoned:cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd",
            ),
        ];
        for (label, actual, expected) in &cases {
            assert_eq!(actual, expected, "{label}");
        }
    }

    #[test]
    fn test_refresh_token_parse_roundtrip() {
        assert_eq!(
            SystemKeys::parse_refresh_token("_sys:refresh_token:99"),
            Some(RefreshTokenId::new(99))
        );
        assert_eq!(SystemKeys::parse_refresh_token("_sys:refresh_token:abc"), None);
        assert_eq!(SystemKeys::parse_refresh_token("other:99"), None);
    }

    #[test]
    fn test_refresh_token_prefix_consistency() {
        let hash: [u8; 32] = [0x01; 32];
        let family: [u8; 16] = [0x02; 16];

        let cases: Vec<(&str, String, &[&str])> = vec![
            (
                "refresh_token",
                SystemKeys::refresh_token(RefreshTokenId::new(1)),
                &[SystemKeys::REFRESH_TOKEN_PREFIX, SystemKeys::SYS_PREFIX],
            ),
            (
                "refresh_token_hash_index",
                SystemKeys::refresh_token_hash_index(&hash),
                &[SystemKeys::REFRESH_TOKEN_HASH_INDEX_PREFIX, SystemKeys::INDEX_PREFIX],
            ),
            (
                "refresh_token_family_prefix",
                SystemKeys::refresh_token_family_prefix(&family),
                &[SystemKeys::REFRESH_TOKEN_FAMILY_PREFIX, SystemKeys::INDEX_PREFIX],
            ),
            (
                "refresh_token_family_poisoned",
                SystemKeys::refresh_token_family_poisoned(&family),
                &[SystemKeys::REFRESH_TOKEN_FAMILY_POISONED_PREFIX, SystemKeys::INDEX_PREFIX],
            ),
            (
                "refresh_token_app_vault_prefix",
                SystemKeys::refresh_token_app_vault_prefix(AppSlug::new(1), VaultId::new(2)),
                &[SystemKeys::REFRESH_TOKEN_APP_VAULT_PREFIX, SystemKeys::INDEX_PREFIX],
            ),
        ];
        for (label, key, prefixes) in &cases {
            for prefix in *prefixes {
                assert!(key.starts_with(prefix), "{label} should start with {prefix}");
            }
        }
    }

    #[test]
    fn test_refresh_token_sequence_key() {
        assert_eq!(SystemKeys::REFRESH_TOKEN_SEQ_KEY, "_meta:seq:refresh_token");
        assert!(SystemKeys::REFRESH_TOKEN_SEQ_KEY.starts_with(SystemKeys::META_PREFIX));
    }

    #[test]
    fn test_refresh_token_family_prefix_does_not_collide_with_poisoned() {
        let family: [u8; 16] = [0xff; 16];
        let family_prefix = SystemKeys::refresh_token_family_prefix(&family);
        let poisoned = SystemKeys::refresh_token_family_poisoned(&family);
        assert_ne!(family_prefix, poisoned);
        // family prefix starts with "family:" while poisoned starts with "family_poisoned:"
        assert!(family_prefix.contains(":family:"));
        assert!(poisoned.contains(":family_poisoned:"));
    }

    #[test]
    fn test_signing_key_and_refresh_token_prefixes_do_not_collide() {
        assert_ne!(SystemKeys::SIGNING_KEY_PREFIX, SystemKeys::REFRESH_TOKEN_PREFIX);
        assert_ne!(
            SystemKeys::SIGNING_KEY_KID_INDEX_PREFIX,
            SystemKeys::REFRESH_TOKEN_HASH_INDEX_PREFIX
        );
    }

    #[test]
    fn test_encode_hex() {
        assert_eq!(encode_hex(&[0x00, 0xff, 0xab]), "00ffab");
        assert_eq!(encode_hex(&[]), "");
        assert_eq!(encode_hex(&[0x01]), "01");
    }
}
