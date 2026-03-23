//! Key patterns for the `_system` organization.
//!
//! Defines storage key builders for all system entity families: users,
//! organizations, apps, teams, signing keys, refresh tokens, invitations,
//! and credentials. Keys are classified by [`KeyTier`] (Global vs Regional)
//! and [`KeyFamily`] (directory, index, meta, shred, temporary, audit, entity).
//! [`SystemKeys::validate_key_tier`] enforces tier correctness at write time.

use std::fmt::Write;

use inferadb_ledger_types::{
    AppId, AppSlug, ClientAssertionId, CredentialType, EmailVerifyTokenId, InviteId, InviteSlug,
    NodeId, OrganizationId, OrganizationSlug, RefreshTokenId, Region, SigningKeyId,
    SigningKeyScope, TeamId, TeamSlug, TokenSubject, UserCredentialId, UserEmailId, UserId,
    UserSlug, VaultId, VaultSlug,
};

/// Encodes a byte slice as lowercase hexadecimal.
fn encode_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // write! to a String is infallible
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// The state layer tier a storage key belongs to.
///
/// Every key in the system organization targets exactly one tier:
/// - **Global**: structural data, routing, cross-region indexes, bookkeeping.
/// - **Regional**: PII, crypto-shredding material, ephemeral state, per-user records.
///
/// Writing a key to the wrong tier is a data residency bug: PII leaks to GLOBAL,
/// or structural data becomes region-isolated. [`SystemKeys::validate_key_tier`]
/// catches these at write time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyTier {
    /// Key must only be written to the GLOBAL state layer.
    Global,
    /// Key must only be written to a REGIONAL state layer.
    Regional,
}

/// Prefix family classification for system keys.
///
/// Groups key constants by their prefix convention. Used by [`SystemKeys::KEY_REGISTRY`]
/// to enforce naming invariants: e.g., all `Directory` entries must start
/// with `_dir:`, all `Index` entries with `_idx:`, etc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyFamily {
    /// `_dir:` — Directory routing (ID to region/slug/status).
    Directory,
    /// `_idx:` — Secondary indexes.
    Index,
    /// `_meta:` — Bookkeeping (sequences, saga state, node membership).
    Meta,
    /// `_shred:` — Crypto-shredding keys.
    Shred,
    /// `_tmp:` — Ephemeral state (TTL-bound onboarding).
    Temporary,
    /// `_audit:` — Compliance trail.
    Audit,
    /// Bare entity keys (no underscore prefix).
    Entity,
    /// `_meta:seq:` — Sequence counters (subset of Meta).
    Sequence,
}

/// Metadata about a single key constant in the system organization.
///
/// Every `PREFIX` or `SEQ_KEY` constant defined on [`SystemKeys`] has a
/// corresponding entry. The registry is the single source of truth for:
/// - Tier classification (Global vs Regional)
/// - Prefix family (`_dir:`, `_idx:`, `_meta:`, `_shred:`, `_tmp:`, `_audit:`, bare, seq)
/// - Constant name (for diagnostic messages)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyRegistryEntry {
    /// The constant name (e.g., `"USER_DIRECTORY_PREFIX"`).
    pub name: &'static str,
    /// The constant value (e.g., `"_dir:user:"`).
    pub value: &'static str,
    /// The expected tier for keys matching this prefix.
    pub tier: KeyTier,
    /// The prefix family this constant belongs to.
    pub family: KeyFamily,
}

/// Key pattern generators for `_system` organization entities.
///
/// All keys follow the convention `{entity_type}:{id}` for primary keys
/// and `_idx:{index_name}:{value}` for secondary indexes.
pub struct SystemKeys;

/// Constructs a `KeyRegistryEntry` from a constant name, tier, and family.
///
/// Stringifies the constant name for diagnostic messages and references
/// `SystemKeys::$name` for the value. Used exclusively by
/// [`SystemKeys::KEY_REGISTRY`].
macro_rules! key_entry {
    ($name:ident, $tier:ident, $family:ident) => {
        KeyRegistryEntry {
            name: stringify!($name),
            value: SystemKeys::$name,
            tier: KeyTier::$tier,
            family: KeyFamily::$family,
        }
    };
}

impl SystemKeys {
    // ========================================================================
    // Key Registry — single source of truth for key constant metadata
    // ========================================================================

    /// Registry of all key constants with their metadata.
    ///
    /// This is the single source of truth for key classification. When
    /// adding a new key constant, add an entry here — the count guard
    /// in tests will fail if forgotten.
    ///
    /// `INDEX_PREFIX` (`_idx:`) is excluded because it spans both tiers
    /// (some indexes are GLOBAL, some REGIONAL). It is validated separately
    /// in the taxonomy test.
    pub const KEY_REGISTRY: &[KeyRegistryEntry] = &[
        // -- _dir: Directory routing (GLOBAL) --
        key_entry!(DIR_PREFIX, Global, Directory),
        key_entry!(USER_DIRECTORY_PREFIX, Global, Directory),
        key_entry!(ORG_REGISTRY_PREFIX, Global, Directory),
        // -- _idx: Secondary indexes (GLOBAL) --
        key_entry!(USER_SLUG_INDEX_PREFIX, Global, Index),
        key_entry!(EMAIL_HASH_INDEX_PREFIX, Global, Index),
        key_entry!(APP_SLUG_INDEX_PREFIX, Global, Index),
        key_entry!(SIGNING_KEY_KID_INDEX_PREFIX, Global, Index),
        key_entry!(SIGNING_KEY_SCOPE_INDEX_PREFIX, Global, Index),
        key_entry!(SIGNING_KEY_ORG_PREFIX, Global, Index),
        key_entry!(REFRESH_TOKEN_HASH_INDEX_PREFIX, Global, Index),
        key_entry!(REFRESH_TOKEN_FAMILY_PREFIX, Global, Index),
        key_entry!(REFRESH_TOKEN_FAMILY_POISONED_PREFIX, Global, Index),
        key_entry!(REFRESH_TOKEN_APP_VAULT_PREFIX, Global, Index),
        key_entry!(REFRESH_TOKEN_ORG_PREFIX, Global, Index),
        key_entry!(TEAM_SLUG_INDEX_PREFIX, Global, Index),
        // -- _idx: Secondary indexes (REGIONAL) --
        key_entry!(EMAIL_VERIFY_HASH_INDEX_PREFIX, Regional, Index),
        key_entry!(APP_NAME_INDEX_PREFIX, Regional, Index),
        key_entry!(USER_EMAILS_INDEX_PREFIX, Regional, Index),
        key_entry!(USER_CREDENTIAL_TYPE_INDEX_PREFIX, Regional, Index),
        // -- _meta: Bookkeeping (GLOBAL) --
        key_entry!(META_PREFIX, Global, Meta),
        key_entry!(NODE_PREFIX, Global, Meta),
        key_entry!(SAGA_PREFIX, Global, Meta),
        key_entry!(REHASH_PROGRESS_PREFIX, Global, Meta),
        key_entry!(BLINDING_KEY_VERSION_KEY, Global, Meta),
        // -- _shred: Crypto-shredding (REGIONAL) --
        key_entry!(USER_SHRED_KEY_PREFIX, Regional, Shred),
        key_entry!(ORG_SHRED_KEY_PREFIX, Regional, Shred),
        // -- _tmp: Ephemeral (REGIONAL) --
        key_entry!(ONBOARD_VERIFY_PREFIX, Regional, Temporary),
        key_entry!(ONBOARD_ACCOUNT_PREFIX, Regional, Temporary),
        key_entry!(TOTP_CHALLENGE_PREFIX, Regional, Temporary),
        // -- _audit: Compliance (GLOBAL) --
        key_entry!(ERASURE_AUDIT_PREFIX, Global, Audit),
        key_entry!(SIGNING_KEY_AUDIT_PREFIX, Global, Audit),
        key_entry!(ORG_AUDIT_PREFIX, Global, Audit),
        key_entry!(USER_ROLE_AUDIT_PREFIX, Global, Audit),
        key_entry!(APP_AUDIT_PREFIX, Global, Audit),
        key_entry!(VAULT_AUDIT_PREFIX, Global, Audit),
        key_entry!(USER_AUDIT_PREFIX, Global, Audit),
        // -- Bare entity prefixes (GLOBAL) --
        key_entry!(APP_PREFIX, Global, Entity),
        key_entry!(ORG_PREFIX, Global, Entity),
        key_entry!(SIGNING_KEY_PREFIX, Global, Entity),
        key_entry!(REFRESH_TOKEN_PREFIX, Global, Entity),
        key_entry!(APP_ASSERTION_PREFIX, Global, Entity),
        key_entry!(APP_VAULT_PREFIX, Global, Entity),
        // -- Bare entity prefixes (REGIONAL) --
        key_entry!(USER_PREFIX, Regional, Entity),
        key_entry!(USER_EMAIL_PREFIX, Regional, Entity),
        key_entry!(TEAM_PREFIX, Regional, Entity),
        key_entry!(APP_PROFILE_PREFIX, Regional, Entity),
        key_entry!(ORG_PROFILE_PREFIX, Regional, Entity),
        key_entry!(ASSERTION_NAME_PREFIX, Regional, Entity),
        key_entry!(EMAIL_VERIFY_PREFIX, Regional, Entity),
        key_entry!(USER_CREDENTIAL_PREFIX, Regional, Entity),
        // -- _meta:seq: Sequence counters (GLOBAL) --
        key_entry!(ORG_SEQ_KEY, Global, Sequence),
        key_entry!(USER_SEQ_KEY, Global, Sequence),
        key_entry!(APP_SEQ_KEY, Global, Sequence),
        key_entry!(VAULT_SEQ_KEY, Global, Sequence),
        key_entry!(USER_EMAIL_SEQ_KEY, Global, Sequence),
        key_entry!(EMAIL_VERIFY_SEQ_KEY, Global, Sequence),
        key_entry!(CLIENT_ASSERTION_SEQ_KEY, Global, Sequence),
        key_entry!(SIGNING_KEY_SEQ_KEY, Global, Sequence),
        key_entry!(REFRESH_TOKEN_SEQ_KEY, Global, Sequence),
        key_entry!(INVITE_SEQ_KEY, Global, Sequence),
        // -- _meta:seq: Sequence counters (REGIONAL) --
        key_entry!(USER_CREDENTIAL_SEQ_KEY, Regional, Sequence),
        // -- _idx: Invitation indexes (GLOBAL) --
        key_entry!(INVITE_SLUG_INDEX_PREFIX, Global, Index),
        key_entry!(INVITE_TOKEN_HASH_INDEX_PREFIX, Global, Index),
        key_entry!(INVITE_EMAIL_HASH_INDEX_PREFIX, Global, Index),
        // -- Bare entity prefixes (REGIONAL) — Invitations --
        key_entry!(INVITE_PREFIX, Regional, Entity),
    ];

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

    /// Prefix for user-to-emails index keys (REGIONAL per-user lookup).
    pub const USER_EMAILS_INDEX_PREFIX: &'static str = "_idx:user_emails:";

    /// Prefix for email verification token keys (REGIONAL).
    pub const EMAIL_VERIFY_PREFIX: &'static str = "email_verify:";

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
    /// Pattern: `_dir:user:{id}` → postcard-serialized `UserDirectoryEntry`
    ///
    /// Directory entries contain no PII — only opaque identifiers, region,
    /// status, and timestamp. Enables cross-region user resolution from any node.
    pub fn user_directory_key(user_id: UserId) -> String {
        format!("_dir:user:{}", user_id.value())
    }

    /// Parses a user ID from a user directory key.
    ///
    /// Returns `None` if the key doesn't match `_dir:user:{id}`.
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

    /// Primary key for an `Organization` skeleton in the GLOBAL state layer.
    ///
    /// The skeleton holds all structural fields (`slug`, `region`, `tier`,
    /// `status`, `members`, `created_at`) with `name: ""`. PII is overlaid
    /// from the REGIONAL `OrganizationProfile` via `overlay_org_profile()`.
    ///
    /// Pattern: `org:{organization_id}`
    pub fn organization_key(organization: OrganizationId) -> String {
        format!("org:{}", organization.value())
    }

    /// Parses an organization ID from an `Organization` skeleton key.
    pub fn parse_organization_key(key: &str) -> Option<OrganizationId> {
        key.strip_prefix(Self::ORG_PREFIX).and_then(|id| id.parse().ok())
    }

    /// Primary key for an `OrganizationRegistry` (cluster topology / routing).
    ///
    /// This is internal routing infrastructure, not a domain entity.
    ///
    /// Pattern: `_dir:org_registry:{organization_id}`
    pub fn organization_registry_key(organization: OrganizationId) -> String {
        format!("_dir:org_registry:{}", organization.value())
    }

    /// Parses an organization ID from an `OrganizationRegistry` key.
    pub fn parse_organization_registry_key(key: &str) -> Option<OrganizationId> {
        key.strip_prefix(Self::ORG_REGISTRY_PREFIX).and_then(|id| id.parse().ok())
    }

    /// Index key for organization slug lookup.
    ///
    /// Pattern: `_idx:org:slug:{slug}` → organization_id
    pub fn organization_slug_key(slug: OrganizationSlug) -> String {
        format!("_idx:org:slug:{}", slug.value())
    }

    /// Primary key for an organization profile record in the regional store.
    ///
    /// Pattern: `org_profile:{organization_id}` → postcard-serialized `OrganizationProfile`
    ///
    /// Contains PII (organization name). Stored in the region declared at creation.
    pub fn organization_profile_key(organization: OrganizationId) -> String {
        format!("org_profile:{}", organization.value())
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

    /// Primary key for a team record in the system vault (REGIONAL).
    ///
    /// Teams are REGIONAL-only (Pattern 1) — no GLOBAL skeleton exists.
    ///
    /// Pattern: `team:{organization_id}:{team_id}` → postcard-serialized `Team`
    pub fn team_key(organization: OrganizationId, team: TeamId) -> String {
        format!("team:{}:{}", organization.value(), team.value())
    }

    /// Index key for team slug lookup.
    ///
    /// Pattern: `_idx:team:slug:{slug}` → team_id
    pub fn team_slug_key(slug: TeamSlug) -> String {
        format!("_idx:team:slug:{}", slug.value())
    }

    /// Prefix for team slug index keys (GLOBAL cross-region resolution).
    pub const TEAM_SLUG_INDEX_PREFIX: &'static str = "_idx:team:slug:";

    /// Prefix for team keys (REGIONAL-only domain entities).
    pub const TEAM_PREFIX: &'static str = "team:";

    // ========================================================================
    // App Keys
    // ========================================================================

    /// Primary key for an application record in the system vault (GLOBAL skeleton).
    ///
    /// The bare `app:` prefix is safe from collisions with `app_profile:`,
    /// `app_assertion:`, and `app_vault:` because `:` (0x3A) sorts before
    /// `_` (0x5F) in ASCII — a prefix scan on `app:{org}:` never matches
    /// keys from those other families.
    ///
    /// Pattern: `app:{organization_id}:{app_id}` → postcard-serialized `App`
    pub fn app_key(organization: OrganizationId, app: AppId) -> String {
        format!("app:{}:{}", organization.value(), app.value())
    }

    /// Prefix for listing all apps in an organization.
    ///
    /// Pattern: `app:{organization_id}:`
    ///
    /// # Prefix collision safety
    ///
    /// Because `:` (0x3A) sorts before `_` (0x5F) in ASCII, a B-tree
    /// prefix scan on `app:{org}:` will never match keys from the
    /// `app_profile:`, `app_assertion:`, or `app_vault:` families.
    /// This invariant holds for all bare-prefix key families.
    pub fn app_prefix(organization: OrganizationId) -> String {
        format!("app:{}:", organization.value())
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

    /// Primary key for a client assertion entry (GLOBAL).
    ///
    /// Pattern: `app_assertion:{org_id}:{app_id}:{assertion_id}`
    pub fn app_assertion_key(
        organization: OrganizationId,
        app: AppId,
        assertion: ClientAssertionId,
    ) -> String {
        format!("app_assertion:{}:{}:{}", organization.value(), app.value(), assertion.value())
    }

    /// Prefix for listing all client assertion entries for an app.
    ///
    /// Pattern: `app_assertion:{org_id}:{app_id}:`
    pub fn app_assertion_prefix(organization: OrganizationId, app: AppId) -> String {
        format!("app_assertion:{}:{}:", organization.value(), app.value())
    }

    /// Key for a client assertion's user-provided name (REGIONAL PII overlay).
    ///
    /// This is a Pattern 2 PII overlay keyed by the assertion's primary key,
    /// not a secondary index. The name is user-provided PII separated from the
    /// GLOBAL `ClientAssertionEntry` for data residency.
    ///
    /// Pattern: `assertion_name:{org_id}:{app_id}:{assertion_id}`
    pub fn assertion_name_key(
        organization: OrganizationId,
        app: AppId,
        assertion: ClientAssertionId,
    ) -> String {
        format!("assertion_name:{}:{}:{}", organization.value(), app.value(), assertion.value())
    }

    /// Prefix for listing all assertion names for an app (REGIONAL state).
    ///
    /// Pattern: `assertion_name:{org_id}:{app_id}:`
    pub fn assertion_name_prefix(organization: OrganizationId, app: AppId) -> String {
        format!("assertion_name:{}:{}:", organization.value(), app.value())
    }

    /// Prefix for listing all assertion names for an organization (REGIONAL state).
    ///
    /// Pattern: `assertion_name:{org_id}:`
    pub fn assertion_name_org_prefix(organization: OrganizationId) -> String {
        format!("assertion_name:{}:", organization.value())
    }

    /// Primary key for a vault connection for an app (GLOBAL).
    ///
    /// Pattern: `app_vault:{org_id}:{app_id}:{vault_id}`
    pub fn app_vault_key(organization: OrganizationId, app: AppId, vault: VaultId) -> String {
        format!("app_vault:{}:{}:{}", organization.value(), app.value(), vault.value())
    }

    /// Prefix for listing all vault connections for an app.
    ///
    /// Pattern: `app_vault:{org_id}:{app_id}:`
    pub fn app_vault_prefix(organization: OrganizationId, app: AppId) -> String {
        format!("app_vault:{}:{}:", organization.value(), app.value())
    }

    /// Prefix for app keys (GLOBAL skeleton).
    pub const APP_PREFIX: &'static str = "app:";

    /// Primary key for an application's PII profile in the regional state layer.
    ///
    /// Pattern: `app_profile:{organization_id}:{app_id}` → postcard-serialized `AppProfile`
    pub fn app_profile_key(organization: OrganizationId, app: AppId) -> String {
        format!("app_profile:{}:{}", organization.value(), app.value())
    }

    /// Prefix for app profile keys (REGIONAL PII overlay).
    pub const APP_PROFILE_PREFIX: &'static str = "app_profile:";

    /// Prefix for app slug index entries.
    pub const APP_SLUG_INDEX_PREFIX: &'static str = "_idx:app:slug:";

    /// Prefix for app name index entries.
    pub const APP_NAME_INDEX_PREFIX: &'static str = "_idx:app:name:";

    /// Prefix for app assertion entries (GLOBAL).
    pub const APP_ASSERTION_PREFIX: &'static str = "app_assertion:";

    /// Prefix for assertion name entries (REGIONAL PII overlay).
    pub const ASSERTION_NAME_PREFIX: &'static str = "assertion_name:";

    /// Prefix for app vault connection entries (GLOBAL).
    pub const APP_VAULT_PREFIX: &'static str = "app_vault:";

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
    /// Pattern: `_meta:node:{id}`
    ///
    /// Cluster membership is infrastructure, not a domain entity.
    pub fn node_key(node_id: &NodeId) -> String {
        format!("_meta:node:{node_id}")
    }

    /// Parses a node ID from a node key.
    pub fn parse_node_key(key: &str) -> Option<NodeId> {
        key.strip_prefix(Self::NODE_PREFIX).map(NodeId::new)
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
    /// Pattern: `_meta:saga:{saga_id}`
    ///
    /// Internal orchestration state is bookkeeping, not a domain entity.
    pub fn saga_key(saga_id: &str) -> String {
        format!("_meta:saga:{saga_id}")
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
    // Crypto-Shredding Keys (regional store, per-entity encryption)
    // ========================================================================

    /// Primary key for a user's crypto-shredding encryption key.
    ///
    /// Pattern: `_shred:user:{user_id}` → postcard-serialized `UserShredKey`
    ///
    /// Stored in the regional store where the user's PII resides.
    /// Encrypted at rest by the region's RMK (via `EncryptedBackend`).
    /// Destroying this key makes the user's PII cryptographically unrecoverable.
    pub fn user_shred_key(user_id: UserId) -> String {
        format!("_shred:user:{}", user_id.value())
    }

    /// Parses a user ID from a user shred key.
    ///
    /// Returns `None` if the key doesn't match `_shred:user:{id}`.
    pub fn parse_user_shred_key(key: &str) -> Option<UserId> {
        key.strip_prefix(Self::USER_SHRED_KEY_PREFIX).and_then(|id| id.parse().ok())
    }

    // ========================================================================
    // Organization Shred Key Functions (per-org encryption for crypto-shredding)
    // ========================================================================

    /// Per-organization encryption key for crypto-shredding.
    ///
    /// Stored in the regional store where the organization's PII resides.
    /// Encrypted at rest by the region's RMK (via `EncryptedBackend`).
    /// Destroying this key makes the organization's PII cryptographically unrecoverable.
    pub fn org_shred_key(organization: OrganizationId) -> String {
        format!("_shred:org:{}", organization.value())
    }

    /// Parses an organization ID from an org shred key.
    ///
    /// Returns `None` if the key doesn't match `_shred:org:{id}`.
    pub fn parse_org_shred_key(key: &str) -> Option<OrganizationId> {
        key.strip_prefix(Self::ORG_SHRED_KEY_PREFIX).and_then(|id| id.parse().ok())
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
    // Onboarding Keys (REGIONAL store, ephemeral)
    // ========================================================================

    /// Key for a pending email verification record.
    ///
    /// Pattern: `_tmp:onboard_verify:{email_hmac}` → postcard-serialized
    /// [`PendingEmailVerification`](super::types::PendingEmailVerification)
    ///
    /// Stored in a REGIONAL Raft group. The `email_hmac` suffix is the
    /// HMAC-SHA256 hex string of the email address (same value used in
    /// the GLOBAL `_idx:email_hash:` index).
    pub fn onboard_verify_key(email_hmac: &str) -> String {
        format!("_tmp:onboard_verify:{email_hmac}")
    }

    /// Parses an email HMAC hex string from an onboard verify key.
    ///
    /// Returns `None` if the key doesn't match `_tmp:onboard_verify:{hmac}`.
    pub fn parse_onboard_verify_key(key: &str) -> Option<&str> {
        key.strip_prefix(Self::ONBOARD_VERIFY_PREFIX)
    }

    /// Key for an onboarding account record.
    ///
    /// Pattern: `_tmp:onboard_account:{email_hmac}` → postcard-serialized
    /// [`OnboardingAccount`](super::types::OnboardingAccount)
    ///
    /// Stored in a REGIONAL Raft group. Contains no PII — the email HMAC
    /// in the key suffix is the only link to the user's identity.
    pub fn onboard_account_key(email_hmac: &str) -> String {
        format!("_tmp:onboard_account:{email_hmac}")
    }

    /// Parses an email HMAC hex string from an onboard account key.
    ///
    /// Returns `None` if the key doesn't match `_tmp:onboard_account:{hmac}`.
    pub fn parse_onboard_account_key(key: &str) -> Option<&str> {
        key.strip_prefix(Self::ONBOARD_ACCOUNT_PREFIX)
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

    /// Prefix for `Organization` skeleton keys (GLOBAL domain entity).
    pub const ORG_PREFIX: &'static str = "org:";

    /// Prefix for `OrganizationRegistry` keys (routing infrastructure).
    pub const ORG_REGISTRY_PREFIX: &'static str = "_dir:org_registry:";

    /// Prefix for all node keys (infrastructure bookkeeping).
    pub const NODE_PREFIX: &'static str = "_meta:node:";

    /// Prefix for all saga keys (infrastructure bookkeeping).
    pub const SAGA_PREFIX: &'static str = "_meta:saga:";

    /// Prefix for all user directory entries in the GLOBAL control plane.
    pub const USER_DIRECTORY_PREFIX: &'static str = "_dir:user:";

    /// Prefix for all directory entries (`_dir:` namespace).
    pub const DIR_PREFIX: &'static str = "_dir:";

    /// Prefix for user slug index entries.
    pub const USER_SLUG_INDEX_PREFIX: &'static str = "_idx:user:slug:";

    /// Prefix for organization profile keys (REGIONAL PII overlay).
    pub const ORG_PROFILE_PREFIX: &'static str = "org_profile:";

    /// Prefix for all index keys.
    pub const INDEX_PREFIX: &'static str = "_idx:";

    /// Prefix for all metadata keys.
    pub const META_PREFIX: &'static str = "_meta:";

    /// Prefix for email hash index entries.
    pub const EMAIL_HASH_INDEX_PREFIX: &'static str = "_idx:email_hash:";

    /// Prefix for blinding key rehash progress entries.
    pub const REHASH_PROGRESS_PREFIX: &'static str = "_meta:blinding_key_rehash_progress:";

    /// Prefix for per-user crypto-shredding keys.
    pub const USER_SHRED_KEY_PREFIX: &'static str = "_shred:user:";

    /// Prefix for per-organization crypto-shredding keys.
    pub const ORG_SHRED_KEY_PREFIX: &'static str = "_shred:org:";

    /// Prefix for erasure audit records.
    pub const ERASURE_AUDIT_PREFIX: &'static str = "_audit:erasure:";

    /// Prefix for signing key lifecycle audit records.
    pub const SIGNING_KEY_AUDIT_PREFIX: &'static str = "_audit:signing_key:";

    /// Prefix for organization lifecycle audit records.
    pub const ORG_AUDIT_PREFIX: &'static str = "_audit:org:";

    /// Prefix for user role change audit records.
    pub const USER_ROLE_AUDIT_PREFIX: &'static str = "_audit:user_role:";

    /// Prefix for app lifecycle audit records.
    pub const APP_AUDIT_PREFIX: &'static str = "_audit:app:";

    /// Prefix for vault lifecycle audit records.
    pub const VAULT_AUDIT_PREFIX: &'static str = "_audit:vault:";

    /// Prefix for user lifecycle audit records (delete, erase, revoke sessions).
    pub const USER_AUDIT_PREFIX: &'static str = "_audit:user:";

    /// Prefix for onboarding email verification keys (ephemeral, regional).
    pub const ONBOARD_VERIFY_PREFIX: &'static str = "_tmp:onboard_verify:";

    /// Prefix for onboarding account keys (ephemeral, regional).
    pub const ONBOARD_ACCOUNT_PREFIX: &'static str = "_tmp:onboard_account:";

    // ========================================================================
    // User Credential Keys
    // ========================================================================

    /// Primary key for a user credential record (REGIONAL).
    ///
    /// Pattern: `user_credential:{user_id}:{credential_id}`
    ///
    /// Stored in a REGIONAL Raft group. The composite key enables prefix
    /// scan for all credentials belonging to a user.
    pub fn user_credential_key(user_id: UserId, credential_id: UserCredentialId) -> String {
        format!("user_credential:{}:{}", user_id.value(), credential_id.value())
    }

    /// Prefix for scanning all credentials belonging to a user.
    ///
    /// Pattern: `user_credential:{user_id}:`
    pub fn user_credential_prefix(user_id: UserId) -> String {
        format!("user_credential:{}:", user_id.value())
    }

    /// Parses a user credential key into its components.
    ///
    /// Returns `None` if the key doesn't match `user_credential:{user_id}:{credential_id}`.
    pub fn parse_user_credential_key(key: &str) -> Option<(UserId, UserCredentialId)> {
        let rest = key.strip_prefix(Self::USER_CREDENTIAL_PREFIX)?;
        let (user_str, cred_str) = rest.split_once(':')?;
        let user_id = user_str.parse::<i64>().ok().map(UserId::new)?;
        let credential_id = cred_str.parse::<i64>().ok().map(UserCredentialId::new)?;
        Some((user_id, credential_id))
    }

    /// Index key for scanning credentials by type within a user.
    ///
    /// Pattern: `_idx:user_credential:type:{user_id}:{type}:`
    ///
    /// The trailing colon makes this a scan prefix — iterate to find all
    /// credential IDs of this type for the user. Used to enforce
    /// one-TOTP-per-user and one-recovery-code-per-user invariants.
    pub fn user_credential_type_index_key(
        user_id: UserId,
        credential_type: &CredentialType,
    ) -> String {
        format!("_idx:user_credential:type:{}:{credential_type}:", user_id.value())
    }

    /// Prefix for scanning credential type index entries for a user.
    ///
    /// Pattern: `_idx:user_credential:type:{user_id}:`
    pub fn user_credential_type_index_prefix(user_id: UserId) -> String {
        format!("_idx:user_credential:type:{}:", user_id.value())
    }

    /// Key for a pending TOTP challenge (ephemeral, REGIONAL).
    ///
    /// Pattern: `_tmp:totp_challenge:{user_id}:{nonce_hex}`
    ///
    /// Stored in a REGIONAL Raft group. Auto-expires after 5 minutes.
    /// The nonce is a 32-byte random value encoded as lowercase hex.
    pub fn totp_challenge_key(user_id: UserId, nonce: &[u8; 32]) -> String {
        format!("_tmp:totp_challenge:{}:{}", user_id.value(), encode_hex(nonce))
    }

    /// Prefix for scanning all TOTP challenges for a user.
    ///
    /// Pattern: `_tmp:totp_challenge:{user_id}:`
    pub fn totp_challenge_prefix(user_id: UserId) -> String {
        format!("_tmp:totp_challenge:{}:", user_id.value())
    }

    /// Prefix for all user credential primary keys (REGIONAL).
    pub const USER_CREDENTIAL_PREFIX: &'static str = "user_credential:";

    /// Prefix for credential type index keys (REGIONAL).
    pub const USER_CREDENTIAL_TYPE_INDEX_PREFIX: &'static str = "_idx:user_credential:type:";

    /// Key for the user credential ID sequence counter (REGIONAL).
    ///
    /// This is the first REGIONAL `_meta:seq:` key — all existing sequences
    /// are GLOBAL. Credential IDs are user-scoped and region-scoped, so a
    /// REGIONAL sequence avoids the cross-tier saga GLOBAL allocation requires.
    pub const USER_CREDENTIAL_SEQ_KEY: &'static str = "_meta:seq:user_credential";

    /// Prefix for TOTP challenge keys (ephemeral, REGIONAL).
    pub const TOTP_CHALLENGE_PREFIX: &'static str = "_tmp:totp_challenge:";

    // ========================================================================
    // Signing Key Keys
    // ========================================================================

    /// Primary key for a signing key record (GLOBAL).
    ///
    /// Pattern: `signing_key:{id}`
    pub fn signing_key(id: SigningKeyId) -> String {
        format!("signing_key:{}", id.value())
    }

    /// Parses a signing key ID from a signing key primary key.
    ///
    /// Returns `None` if the key doesn't match `signing_key:{id}`.
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
    /// Pattern: `_idx:signing_key:org:{org_id}:`
    ///
    /// This is a secondary index (org → signing key IDs), not a primary record.
    /// Used for org deletion cascade — iterates all keys for an org.
    pub fn signing_key_org_prefix(org: OrganizationId) -> String {
        format!("_idx:signing_key:org:{}:", org.value())
    }

    /// Index entry for a signing key within its organization index.
    ///
    /// Pattern: `_idx:signing_key:org:{org_id}:{key_id}`
    pub fn signing_key_org_entry(org: OrganizationId, key_id: SigningKeyId) -> String {
        format!("_idx:signing_key:org:{}:{}", org.value(), key_id.value())
    }

    /// Prefix for all signing key primary keys (GLOBAL).
    pub const SIGNING_KEY_PREFIX: &'static str = "signing_key:";

    /// Prefix for signing key kid index entries.
    pub const SIGNING_KEY_KID_INDEX_PREFIX: &'static str = "_idx:signing_key:kid:";

    /// Prefix for signing key scope index entries.
    pub const SIGNING_KEY_SCOPE_INDEX_PREFIX: &'static str = "_idx:signing_key:scope:";

    /// Prefix for signing key org index entries (secondary index).
    pub const SIGNING_KEY_ORG_PREFIX: &'static str = "_idx:signing_key:org:";

    /// Key for the signing key ID sequence counter.
    ///
    /// Pattern: `_meta:seq:signing_key` → next `SigningKeyId`
    pub const SIGNING_KEY_SEQ_KEY: &'static str = "_meta:seq:signing_key";

    // ========================================================================
    // Refresh Token Keys
    // ========================================================================

    /// Primary key for a refresh token record (GLOBAL).
    ///
    /// Pattern: `refresh_token:{id}`
    pub fn refresh_token(id: RefreshTokenId) -> String {
        format!("refresh_token:{}", id.value())
    }

    /// Parses a refresh token ID from a refresh token primary key.
    ///
    /// Returns `None` if the key doesn't match `refresh_token:{id}`.
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

    /// Prefix for iterating all refresh tokens belonging to an organization.
    ///
    /// Pattern: `_idx:refresh_token:org:{org_id}:`
    ///
    /// Used for org deletion cascade — avoids O(n) scan of all tokens.
    pub fn refresh_token_org_prefix(org: OrganizationId) -> String {
        format!("_idx:refresh_token:org:{}:", org.value())
    }

    /// Index entry for a refresh token within its org index.
    ///
    /// Pattern: `_idx:refresh_token:org:{org_id}:{id}` → `RefreshTokenId`
    pub fn refresh_token_org_entry(org: OrganizationId, id: RefreshTokenId) -> String {
        format!("_idx:refresh_token:org:{}:{}", org.value(), id.value())
    }

    /// Prefix for all refresh token primary keys (GLOBAL).
    pub const REFRESH_TOKEN_PREFIX: &'static str = "refresh_token:";

    /// Prefix for refresh token hash index entries.
    pub const REFRESH_TOKEN_HASH_INDEX_PREFIX: &'static str = "_idx:refresh_token:hash:";

    /// Prefix for refresh token family index entries.
    pub const REFRESH_TOKEN_FAMILY_PREFIX: &'static str = "_idx:refresh_token:family:";

    /// Prefix for poisoned family markers.
    pub const REFRESH_TOKEN_FAMILY_POISONED_PREFIX: &'static str =
        "_idx:refresh_token:family_poisoned:";

    /// Prefix for refresh token app_vault index entries.
    pub const REFRESH_TOKEN_APP_VAULT_PREFIX: &'static str = "_idx:refresh_token:app_vault:";

    /// Prefix for refresh token org index entries.
    pub const REFRESH_TOKEN_ORG_PREFIX: &'static str = "_idx:refresh_token:org:";

    /// Key for the refresh token ID sequence counter.
    ///
    /// Pattern: `_meta:seq:refresh_token` → next `RefreshTokenId`
    pub const REFRESH_TOKEN_SEQ_KEY: &'static str = "_meta:seq:refresh_token";

    // ========================================================================
    // Invitation Keys
    // ========================================================================

    /// Primary key for an organization invitation record (REGIONAL).
    ///
    /// Pattern: `invite:{org_id}:{invite_id}`
    ///
    /// The full record contains PII (invitee email) and is encrypted with the
    /// organization's `OrgShredKey` before entering the Raft log.
    pub fn invite_key(organization: OrganizationId, invite: InviteId) -> String {
        format!("invite:{}:{}", organization.value(), invite.value())
    }

    /// Parses an organization ID and invite ID from an invitation key.
    ///
    /// Returns `None` if the key doesn't match `invite:{org_id}:{invite_id}`.
    pub fn parse_invite_key(key: &str) -> Option<(OrganizationId, InviteId)> {
        let rest = key.strip_prefix(Self::INVITE_PREFIX)?;
        let (org_str, invite_str) = rest.split_once(':')?;
        Some((org_str.parse().ok()?, invite_str.parse().ok()?))
    }

    /// Prefix for listing all invitations in an organization.
    ///
    /// Pattern: `invite:{org_id}:`
    pub fn invite_prefix(organization: OrganizationId) -> String {
        format!("invite:{}:", organization.value())
    }

    /// Prefix for invitation keys (REGIONAL domain entities).
    pub const INVITE_PREFIX: &'static str = "invite:";

    /// Index key for invitation slug lookup.
    ///
    /// Pattern: `_idx:invite:slug:{slug}` → `InviteIndexEntry`
    pub fn invite_slug_index_key(slug: InviteSlug) -> String {
        format!("_idx:invite:slug:{}", slug.value())
    }

    /// Prefix for invitation slug index keys (GLOBAL cross-region resolution).
    pub const INVITE_SLUG_INDEX_PREFIX: &'static str = "_idx:invite:slug:";

    /// Index key for invitation token hash lookup.
    ///
    /// Pattern: `_idx:invite:token_hash:{hex}` → `InviteIndexEntry`
    ///
    /// The hex string is the SHA-256 hash of the raw invitation token.
    /// This index is removed on any terminal state transition.
    pub fn invite_token_hash_index_key(token_hash_hex: &str) -> String {
        format!("_idx:invite:token_hash:{token_hash_hex}")
    }

    /// Prefix for invitation token hash index keys (GLOBAL).
    pub const INVITE_TOKEN_HASH_INDEX_PREFIX: &'static str = "_idx:invite:token_hash:";

    /// Index key for per-email invitation lookup.
    ///
    /// Pattern: `_idx:invite:email_hash:{hmac}:{invite_id}` → `InviteEmailEntry`
    ///
    /// Enables per-email rate limiting and received-invitations listing
    /// without storing plaintext emails in GLOBAL state.
    pub fn invite_email_hash_index_key(hmac_hex: &str, invite: InviteId) -> String {
        format!("_idx:invite:email_hash:{}:{}", hmac_hex, invite.value())
    }

    /// Prefix for per-email invitation index keys (GLOBAL).
    ///
    /// Use with a specific HMAC to scan all invitations for one email:
    /// `_idx:invite:email_hash:{hmac}:`
    pub const INVITE_EMAIL_HASH_INDEX_PREFIX: &'static str = "_idx:invite:email_hash:";

    /// Prefix for scanning all invitations for a specific email HMAC.
    ///
    /// Pattern: `_idx:invite:email_hash:{hmac}:`
    pub fn invite_email_hash_prefix(hmac_hex: &str) -> String {
        format!("_idx:invite:email_hash:{hmac_hex}:")
    }

    /// Key for the invitation ID sequence counter (GLOBAL).
    ///
    /// Pattern: `_meta:seq:invite` → next `InviteId`
    pub const INVITE_SEQ_KEY: &'static str = "_meta:seq:invite";

    // ========================================================================
    // Key Tier Classification
    // ========================================================================

    /// Classifies a storage key into its expected tier (GLOBAL or REGIONAL).
    ///
    /// Returns `None` only if the key doesn't match any known prefix pattern.
    /// All production keys generated by [`SystemKeys`] methods are classified.
    pub fn classify_key_tier(key: &str) -> Option<KeyTier> {
        // --- GLOBAL: infrastructure, structural data, cross-region indexes ---

        // REGIONAL `_meta:seq:` — exact match, checked before the blanket `_meta:` → Global
        // rule. `user_credential` is the first REGIONAL sequence counter. Use `==` (not
        // `starts_with`) to avoid over-capturing future GLOBAL keys like
        // `_meta:seq:user_credential_audit`.
        if key == Self::USER_CREDENTIAL_SEQ_KEY {
            return Some(KeyTier::Regional);
        }

        if key.starts_with("_dir:") || key.starts_with("_meta:") || key.starts_with("_audit:") {
            return Some(KeyTier::Global);
        }

        // GLOBAL indexes: cross-region resolution and token infrastructure.
        // Note: `_idx:email_hash:` must be checked before `_idx:email:` (Regional).
        // Note: `_idx:app:name:` is Regional (contains plaintext app names) — check before
        // `_idx:app:`.
        // Note: `_idx:team:slug:` is GLOBAL (cross-region slug resolution) but a future
        // `_idx:team:name:` would be Regional — use the specific prefix, not `_idx:team:`.
        if key.starts_with("_idx:user:slug:")
            || key.starts_with("_idx:email_hash:")
            || key.starts_with("_idx:signing_key:")
            || key.starts_with("_idx:refresh_token:")
            || (key.starts_with("_idx:app:") && !key.starts_with("_idx:app:name:"))
            || key.starts_with("_idx:org:")
            || key.starts_with("_idx:vault:")
            || key.starts_with("_idx:team:slug:")
            || key.starts_with("_idx:invite:")
        {
            return Some(KeyTier::Global);
        }

        // GLOBAL bare entities: skeletons (Pattern 2/3) and structural data
        if key.starts_with("app:")
            || key.starts_with("org:")
            || key.starts_with("signing_key:")
            || key.starts_with("refresh_token:")
            || key.starts_with("app_assertion:")
            || key.starts_with("app_vault:")
        {
            return Some(KeyTier::Global);
        }

        // --- REGIONAL: PII, crypto-shredding, ephemeral state ---

        if key.starts_with("_shred:") || key.starts_with("_tmp:") {
            return Some(KeyTier::Regional);
        }

        // REGIONAL indexes: per-user data, PII-containing lookups, credentials
        if key.starts_with("_idx:user_emails:")
            || key.starts_with("_idx:email_verify_hash:")
            || key.starts_with("_idx:email:")
            || key.starts_with("_idx:app:name:")
            || key.starts_with("_idx:user_credential:")
        {
            return Some(KeyTier::Regional);
        }

        // REGIONAL bare entities: PII (Pattern 1) and overlays (Pattern 2)
        if key.starts_with("user:")
            || key.starts_with("user_email:")
            || key.starts_with("email_verify:")
            || key.starts_with("team:")
            || key.starts_with("app_profile:")
            || key.starts_with("org_profile:")
            || key.starts_with("assertion_name:")
            || key.starts_with("user_credential:")
            || key.starts_with("invite:")
        {
            return Some(KeyTier::Regional);
        }

        None
    }

    /// Validates that a key matches the expected tier.
    ///
    /// Returns `Ok(())` if the key's tier matches `expected`, or if the key
    /// is unrecognized (unknown keys are not rejected — new key families may
    /// be added before the classifier is updated).
    ///
    /// Returns `Err` with a descriptive message on tier mismatch.
    pub fn validate_key_tier(key: &str, expected: KeyTier) -> std::result::Result<(), String> {
        if let Some(actual) = Self::classify_key_tier(key)
            && actual != expected
        {
            return Err(format!(
                "tier mismatch: key \"{key}\" is {:?} but write targets {:?} state layer",
                actual, expected,
            ));
        }
        Ok(())
    }
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
                "organization_registry_key",
                SystemKeys::organization_registry_key(OrganizationId::new(42)),
                "_dir:org_registry:42",
            ),
            (
                "organization_profile_key",
                SystemKeys::organization_profile_key(OrganizationId::new(42)),
                "org_profile:42",
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
            ("node_key", SystemKeys::node_key(&NodeId::new("node-1")), "_meta:node:node-1"),
            ("saga_key", SystemKeys::saga_key("create-org-abc123"), "_meta:saga:create-org-abc123"),
            ("user_directory_key", SystemKeys::user_directory_key(UserId::new(42)), "_dir:user:42"),
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
        assert_eq!(SystemKeys::parse_node_key("_meta:node:node-1"), Some(NodeId::new("node-1")));
        assert_eq!(SystemKeys::parse_user_directory_key("_dir:user:42"), Some(UserId::new(42)));
        assert_eq!(SystemKeys::parse_user_directory_key("user:42"), None);
        assert_eq!(SystemKeys::parse_user_directory_key("_dir:user:abc"), None);
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
            (
                "organization_registry_key",
                SystemKeys::organization_registry_key(OrganizationId::new(1)),
                &[SystemKeys::ORG_REGISTRY_PREFIX],
            ),
            (
                "node_key",
                SystemKeys::node_key(&NodeId::new("n")),
                &[SystemKeys::NODE_PREFIX, SystemKeys::META_PREFIX],
            ),
            (
                "organization_profile_key",
                SystemKeys::organization_profile_key(OrganizationId::new(1)),
                &[SystemKeys::ORG_PROFILE_PREFIX],
            ),
            (
                "user_directory_key",
                SystemKeys::user_directory_key(UserId::new(1)),
                &[SystemKeys::USER_DIRECTORY_PREFIX, SystemKeys::DIR_PREFIX],
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
    fn test_user_directory_key_does_not_collide_with_user_key() {
        // _dir:user:42 (directory) vs user:42 (regional record) are distinct
        let directory_key = SystemKeys::user_directory_key(UserId::new(42));
        let user_key = SystemKeys::user_key(UserId::new(42));
        assert_ne!(directory_key, user_key);
        assert!(directory_key.starts_with("_dir:"));
        assert!(!user_key.starts_with("_dir:"));
    }

    #[test]
    fn test_org_registry_key_is_distinct_from_org_skeleton() {
        let skeleton_key = SystemKeys::organization_key(OrganizationId::new(42));
        let registry_key = SystemKeys::organization_registry_key(OrganizationId::new(42));
        assert_eq!(skeleton_key, "org:42");
        assert_eq!(registry_key, "_dir:org_registry:42");
        assert_ne!(skeleton_key, registry_key);
        assert!(registry_key.starts_with(SystemKeys::ORG_REGISTRY_PREFIX));
    }

    #[test]
    fn test_parse_organization_registry_key() {
        assert_eq!(
            SystemKeys::parse_organization_registry_key("_dir:org_registry:42"),
            Some(OrganizationId::new(42))
        );
        assert_eq!(SystemKeys::parse_organization_registry_key("org:42"), None);
        assert_eq!(SystemKeys::parse_organization_registry_key("_dir:org_registry:abc"), None);
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
            ("app_key", SystemKeys::app_key(OrganizationId::new(5), AppId::new(42)), "app:5:42"),
            ("app_prefix", SystemKeys::app_prefix(OrganizationId::new(5)), "app:5:"),
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
                "app_profile_key",
                SystemKeys::app_profile_key(OrganizationId::new(5), AppId::new(42)),
                "app_profile:5:42",
            ),
            (
                "app_assertion_key",
                SystemKeys::app_assertion_key(
                    OrganizationId::new(1),
                    AppId::new(2),
                    ClientAssertionId::new(3),
                ),
                "app_assertion:1:2:3",
            ),
            (
                "app_assertion_prefix",
                SystemKeys::app_assertion_prefix(OrganizationId::new(1), AppId::new(2)),
                "app_assertion:1:2:",
            ),
            (
                "app_vault_key",
                SystemKeys::app_vault_key(OrganizationId::new(1), AppId::new(2), VaultId::new(3)),
                "app_vault:1:2:3",
            ),
            (
                "app_vault_prefix",
                SystemKeys::app_vault_prefix(OrganizationId::new(1), AppId::new(2)),
                "app_vault:1:2:",
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
                &[SystemKeys::APP_PREFIX, "app:5:"],
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
                "app_profile_key",
                SystemKeys::app_profile_key(OrganizationId::new(5), AppId::new(42)),
                &[SystemKeys::APP_PROFILE_PREFIX, "app_profile:5:"],
            ),
            (
                "app_assertion_key",
                SystemKeys::app_assertion_key(
                    OrganizationId::new(1),
                    AppId::new(2),
                    ClientAssertionId::new(7),
                ),
                &[SystemKeys::APP_ASSERTION_PREFIX, "app_assertion:1:2:"],
            ),
            (
                "app_vault_key",
                SystemKeys::app_vault_key(OrganizationId::new(1), AppId::new(2), VaultId::new(9)),
                &[SystemKeys::APP_VAULT_PREFIX, "app_vault:1:2:"],
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
        // Ensure app, profile, assertion, and vault prefixes are distinct namespaces
        let prefixes = [
            SystemKeys::APP_PREFIX,
            SystemKeys::APP_PROFILE_PREFIX,
            SystemKeys::APP_ASSERTION_PREFIX,
            SystemKeys::APP_VAULT_PREFIX,
        ];
        for (i, a) in prefixes.iter().enumerate() {
            for b in &prefixes[i + 1..] {
                assert_ne!(a, b, "{a} should not collide with {b}");
            }
        }
    }

    // =========================================================================
    // Signing key tests
    // =========================================================================

    #[test]
    fn test_signing_key_generation() {
        let cases: Vec<(&str, String, &str)> = vec![
            ("signing_key", SystemKeys::signing_key(SigningKeyId::new(7)), "signing_key:7"),
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
                "_idx:signing_key:org:42:",
            ),
        ];
        for (label, actual, expected) in &cases {
            assert_eq!(actual, expected, "{label}");
        }
    }

    #[test]
    fn test_signing_key_parse_roundtrip() {
        assert_eq!(SystemKeys::parse_signing_key("signing_key:7"), Some(SigningKeyId::new(7)));
        assert_eq!(SystemKeys::parse_signing_key("signing_key:abc"), None);
        assert_eq!(SystemKeys::parse_signing_key("other:7"), None);
    }

    #[test]
    fn test_signing_key_prefix_consistency() {
        let cases: Vec<(&str, String, &[&str])> = vec![
            (
                "signing_key",
                SystemKeys::signing_key(SigningKeyId::new(1)),
                &[SystemKeys::SIGNING_KEY_PREFIX],
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
                &[SystemKeys::SIGNING_KEY_ORG_PREFIX, SystemKeys::INDEX_PREFIX],
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
                "refresh_token:99",
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
            SystemKeys::parse_refresh_token("refresh_token:99"),
            Some(RefreshTokenId::new(99))
        );
        assert_eq!(SystemKeys::parse_refresh_token("refresh_token:abc"), None);
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
                &[SystemKeys::REFRESH_TOKEN_PREFIX],
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

    // ========================================================================
    // Onboarding Key Tests
    // ========================================================================

    #[test]
    fn test_onboard_verify_key() {
        let hmac = "abc123def456";
        let key = SystemKeys::onboard_verify_key(hmac);
        assert_eq!(key, "_tmp:onboard_verify:abc123def456");
        assert!(key.starts_with(SystemKeys::ONBOARD_VERIFY_PREFIX));
    }

    #[test]
    fn test_parse_onboard_verify_key() {
        let key = "_tmp:onboard_verify:abc123def456";
        assert_eq!(SystemKeys::parse_onboard_verify_key(key), Some("abc123def456"));
    }

    #[test]
    fn test_parse_onboard_verify_key_invalid() {
        assert_eq!(SystemKeys::parse_onboard_verify_key("_tmp:wrong:abc"), None);
        assert_eq!(SystemKeys::parse_onboard_verify_key("random_key"), None);
    }

    #[test]
    fn test_onboard_account_key() {
        let hmac = "fedcba987654";
        let key = SystemKeys::onboard_account_key(hmac);
        assert_eq!(key, "_tmp:onboard_account:fedcba987654");
        assert!(key.starts_with(SystemKeys::ONBOARD_ACCOUNT_PREFIX));
    }

    #[test]
    fn test_parse_onboard_account_key() {
        let key = "_tmp:onboard_account:fedcba987654";
        assert_eq!(SystemKeys::parse_onboard_account_key(key), Some("fedcba987654"));
    }

    #[test]
    fn test_parse_onboard_account_key_invalid() {
        assert_eq!(SystemKeys::parse_onboard_account_key("_tmp:onboard_verify:abc"), None);
        assert_eq!(SystemKeys::parse_onboard_account_key("other:key"), None);
    }

    #[test]
    fn test_onboard_keys_do_not_collide() {
        let hmac = "same_hmac_value";
        let verify_key = SystemKeys::onboard_verify_key(hmac);
        let account_key = SystemKeys::onboard_account_key(hmac);
        assert_ne!(verify_key, account_key);
        assert_ne!(SystemKeys::ONBOARD_VERIFY_PREFIX, SystemKeys::ONBOARD_ACCOUNT_PREFIX);
    }

    // ========================================================================
    // Key Registry Tests
    // ========================================================================

    /// Replaces `test_key_tier_exhaustive` + `test_key_tier_classification`.
    ///
    /// Every entry in `KEY_REGISTRY` must:
    /// 1. Be classified by `classify_key_tier` (not `None`).
    /// 2. Classify to the expected tier declared in the registry.
    ///
    /// Value correctness is enforced at compile time — registry entries
    /// reference `Self::*` constants directly (e.g., `Self::DIR_PREFIX`).
    ///
    /// The count guard ensures a developer bumps `EXPECTED_COUNT` when adding
    /// a new constant, preventing silent omissions.
    #[test]
    fn test_key_registry_completeness() {
        const EXPECTED_COUNT: usize = 65;
        assert_eq!(
            SystemKeys::KEY_REGISTRY.len(),
            EXPECTED_COUNT,
            "KEY_REGISTRY has {} entries, expected {EXPECTED_COUNT} — \
             did you add a new constant without updating the registry?",
            SystemKeys::KEY_REGISTRY.len(),
        );

        for entry in SystemKeys::KEY_REGISTRY {
            // Every registered constant must be classifiable
            let actual = SystemKeys::classify_key_tier(entry.value);
            assert!(
                actual.is_some(),
                "{} = \"{}\" returned None from classify_key_tier — add it to the classifier",
                entry.name,
                entry.value,
            );

            // Classification must match the declared tier
            assert_eq!(
                actual,
                Some(entry.tier),
                "{} = \"{}\" classified as {:?} but registry declares {:?}",
                entry.name,
                entry.value,
                actual,
                entry.tier,
            );
        }
    }

    /// Replaces `test_prefix_taxonomy`. Iterates `KEY_REGISTRY` grouped by
    /// `family` and asserts naming invariants.
    ///
    /// Also validates `INDEX_PREFIX` (`_idx:`) separately since it spans both
    /// tiers and is excluded from the registry.
    #[test]
    fn test_key_registry_taxonomy() {
        /// Returns the expected prefix for a key family, or `None` for Entity
        /// (which asserts the *absence* of an underscore prefix).
        fn expected_prefix(family: KeyFamily) -> Option<&'static str> {
            match family {
                KeyFamily::Directory => Some("_dir:"),
                KeyFamily::Index => Some("_idx:"),
                KeyFamily::Meta => Some("_meta:"),
                KeyFamily::Shred => Some("_shred:"),
                KeyFamily::Temporary => Some("_tmp:"),
                KeyFamily::Audit => Some("_audit:"),
                KeyFamily::Entity => None,
                KeyFamily::Sequence => Some("_meta:seq:"),
            }
        }

        for entry in SystemKeys::KEY_REGISTRY {
            if let Some(prefix) = expected_prefix(entry.family) {
                assert!(
                    entry.value.starts_with(prefix),
                    "{} = \"{}\" is {:?} but doesn't start with \"{prefix}\"",
                    entry.name,
                    entry.value,
                    entry.family,
                );
            } else {
                assert!(
                    !entry.value.starts_with('_'),
                    "{} = \"{}\" is Entity but starts with '_'",
                    entry.name,
                    entry.value,
                );
            }
        }

        // INDEX_PREFIX spans both tiers — validate it starts with _idx: but
        // is not in the registry (no single tier applies).
        assert!(
            SystemKeys::INDEX_PREFIX.starts_with("_idx:"),
            "INDEX_PREFIX should start with \"_idx:\""
        );

        // Bare entity prefixes: no prefix is a proper prefix of another.
        // Guards the `:` (0x3A) < `_` (0x5F) ASCII ordering collision safety.
        let entity_entries: Vec<&KeyRegistryEntry> = SystemKeys::KEY_REGISTRY
            .iter()
            .filter(|e| matches!(e.family, KeyFamily::Entity))
            .collect();
        for (i, a) in entity_entries.iter().enumerate() {
            for b in &entity_entries[i + 1..] {
                assert!(
                    !a.value.starts_with(b.value) && !b.value.starts_with(a.value),
                    "bare prefix collision: {} = \"{}\" vs {} = \"{}\"",
                    a.name,
                    a.value,
                    b.name,
                    b.value,
                );
            }
        }
    }

    /// Verifies classification of concrete keys (not just PREFIX constants).
    /// Tests keys generated by `SystemKeys::*` functions to catch any
    /// prefix-matching edge cases.
    #[test]
    fn test_key_tier_concrete_keys() {
        // GLOBAL concrete keys
        let global_keys = [
            SystemKeys::user_directory_key(UserId::new(1)),
            SystemKeys::organization_registry_key(OrganizationId::new(1)),
            SystemKeys::organization_key(OrganizationId::new(1)),
            SystemKeys::app_key(OrganizationId::new(1), AppId::new(1)),
            SystemKeys::signing_key(SigningKeyId::new(1)),
            SystemKeys::user_slug_index_key(UserSlug::new(99)),
            SystemKeys::organization_slug_key(OrganizationSlug::new(99)),
            SystemKeys::app_slug_key(AppSlug::new(99)),
            SystemKeys::app_assertion_key(
                OrganizationId::new(1),
                AppId::new(1),
                ClientAssertionId::new(1),
            ),
            SystemKeys::app_vault_key(OrganizationId::new(1), AppId::new(1), VaultId::new(1)),
            SystemKeys::email_hash_index_key("abc123"),
            SystemKeys::team_slug_key(TeamSlug::new(99)),
        ];
        for key in &global_keys {
            assert_eq!(
                SystemKeys::classify_key_tier(key),
                Some(KeyTier::Global),
                "concrete key \"{key}\" should be Global"
            );
        }

        // REGIONAL concrete keys
        let regional_keys = [
            SystemKeys::user_key(UserId::new(1)),
            SystemKeys::user_email_key(UserEmailId::new(1)),
            SystemKeys::email_verify_key(EmailVerifyTokenId::new(1)),
            SystemKeys::team_key(OrganizationId::new(1), TeamId::new(1)),
            SystemKeys::app_profile_key(OrganizationId::new(1), AppId::new(1)),
            SystemKeys::organization_profile_key(OrganizationId::new(1)),
            SystemKeys::assertion_name_key(
                OrganizationId::new(1),
                AppId::new(1),
                ClientAssertionId::new(1),
            ),
            SystemKeys::user_shred_key(UserId::new(1)),
            SystemKeys::org_shred_key(OrganizationId::new(1)),
            SystemKeys::email_index_key("test@example.com"),
            SystemKeys::user_emails_index_key(UserId::new(1)),
            SystemKeys::email_verify_hash_index_key("abc123"),
            SystemKeys::app_name_index_key(OrganizationId::new(1), "my-app"),
            SystemKeys::user_credential_key(UserId::new(1), UserCredentialId::new(1)),
            SystemKeys::user_credential_type_index_key(UserId::new(1), &CredentialType::Passkey),
            SystemKeys::totp_challenge_key(UserId::new(1), &[0xab; 32]),
        ];
        for key in &regional_keys {
            assert_eq!(
                SystemKeys::classify_key_tier(key),
                Some(KeyTier::Regional),
                "concrete key \"{key}\" should be Regional"
            );
        }

        // REGIONAL sequence key (first REGIONAL _meta:seq:)
        assert_eq!(
            SystemKeys::classify_key_tier(SystemKeys::USER_CREDENTIAL_SEQ_KEY),
            Some(KeyTier::Regional),
            "user credential sequence key should be Regional"
        );
    }

    /// Negative tests: `validate_key_tier` returns an error on tier mismatch.
    #[test]
    fn test_validate_key_tier_mismatch() {
        // Writing a _dir: key to REGIONAL should fail
        let dir_key = SystemKeys::user_directory_key(UserId::new(42));
        let result = SystemKeys::validate_key_tier(&dir_key, KeyTier::Regional);
        assert!(result.is_err(), "_dir: key should not be written to Regional");
        assert!(result.unwrap_err().contains("tier mismatch"));

        // Writing a user: key to GLOBAL should fail
        let user_key = SystemKeys::user_key(UserId::new(42));
        let result = SystemKeys::validate_key_tier(&user_key, KeyTier::Global);
        assert!(result.is_err(), "user: key should not be written to Global");

        // Writing to the correct tier should succeed
        assert!(SystemKeys::validate_key_tier(&dir_key, KeyTier::Global).is_ok());
        assert!(SystemKeys::validate_key_tier(&user_key, KeyTier::Regional).is_ok());

        // Unknown keys pass validation (no false positives)
        assert!(SystemKeys::validate_key_tier("unknown:42", KeyTier::Global).is_ok());
        assert!(SystemKeys::validate_key_tier("unknown:42", KeyTier::Regional).is_ok());
    }

    // ========================================================================
    // User Credential Key Tests
    // ========================================================================

    #[test]
    fn test_user_credential_key_generation() {
        let key = SystemKeys::user_credential_key(UserId::new(42), UserCredentialId::new(7));
        assert_eq!(key, "user_credential:42:7");
    }

    #[test]
    fn test_user_credential_prefix() {
        let prefix = SystemKeys::user_credential_prefix(UserId::new(42));
        assert_eq!(prefix, "user_credential:42:");

        // Primary key must start with user prefix for scan
        let key = SystemKeys::user_credential_key(UserId::new(42), UserCredentialId::new(7));
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn test_parse_user_credential_key_roundtrip() {
        let user_id = UserId::new(42);
        let cred_id = UserCredentialId::new(7);
        let key = SystemKeys::user_credential_key(user_id, cred_id);

        let (parsed_user, parsed_cred) = SystemKeys::parse_user_credential_key(&key).unwrap();
        assert_eq!(parsed_user, user_id);
        assert_eq!(parsed_cred, cred_id);
    }

    #[test]
    fn test_parse_user_credential_key_invalid() {
        assert!(SystemKeys::parse_user_credential_key("user_credential:").is_none());
        assert!(SystemKeys::parse_user_credential_key("user_credential:42").is_none());
        assert!(SystemKeys::parse_user_credential_key("user_credential:abc:7").is_none());
        assert!(SystemKeys::parse_user_credential_key("user:42:7").is_none());
        assert!(SystemKeys::parse_user_credential_key("").is_none());
    }

    #[test]
    fn test_user_credential_type_index_key() {
        let key =
            SystemKeys::user_credential_type_index_key(UserId::new(42), &CredentialType::Passkey);
        assert_eq!(key, "_idx:user_credential:type:42:passkey:");

        let key =
            SystemKeys::user_credential_type_index_key(UserId::new(42), &CredentialType::Totp);
        assert_eq!(key, "_idx:user_credential:type:42:totp:");

        let key = SystemKeys::user_credential_type_index_key(
            UserId::new(42),
            &CredentialType::RecoveryCode,
        );
        assert_eq!(key, "_idx:user_credential:type:42:recovery_code:");
    }

    #[test]
    fn test_user_credential_type_index_prefix() {
        let prefix = SystemKeys::user_credential_type_index_prefix(UserId::new(42));
        assert_eq!(prefix, "_idx:user_credential:type:42:");

        // Type index key must start with user type prefix
        let key =
            SystemKeys::user_credential_type_index_key(UserId::new(42), &CredentialType::Passkey);
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn test_totp_challenge_key() {
        let nonce = [0xab; 32];
        let key = SystemKeys::totp_challenge_key(UserId::new(42), &nonce);
        let expected_hex = "ab".repeat(32);
        assert_eq!(key, format!("_tmp:totp_challenge:42:{expected_hex}"));
    }

    #[test]
    fn test_totp_challenge_prefix() {
        let prefix = SystemKeys::totp_challenge_prefix(UserId::new(42));
        assert_eq!(prefix, "_tmp:totp_challenge:42:");

        // Challenge key must start with user challenge prefix
        let nonce = [0xab; 32];
        let key = SystemKeys::totp_challenge_key(UserId::new(42), &nonce);
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn test_user_credential_prefix_does_not_collide_with_user() {
        // `user_credential:` must not be a prefix of `user:` or vice versa
        assert!(!SystemKeys::USER_CREDENTIAL_PREFIX.starts_with(SystemKeys::USER_PREFIX));
        assert!(!SystemKeys::USER_PREFIX.starts_with(SystemKeys::USER_CREDENTIAL_PREFIX));
    }

    #[test]
    fn test_user_credential_seq_key_is_regional() {
        // The user credential sequence is the first REGIONAL _meta:seq: key
        assert_eq!(
            SystemKeys::classify_key_tier(SystemKeys::USER_CREDENTIAL_SEQ_KEY),
            Some(KeyTier::Regional),
        );

        // Validate that writing it to REGIONAL succeeds
        assert!(
            SystemKeys::validate_key_tier(SystemKeys::USER_CREDENTIAL_SEQ_KEY, KeyTier::Regional,)
                .is_ok()
        );

        // Validate that writing it to GLOBAL fails (catches the pre-existing
        // blanket _meta: → Global rule)
        assert!(
            SystemKeys::validate_key_tier(SystemKeys::USER_CREDENTIAL_SEQ_KEY, KeyTier::Global,)
                .is_err()
        );

        // Existing GLOBAL sequence keys must remain Global after the REGIONAL
        // ordering change — verify the exact-match guard doesn't over-capture.
        for global_seq in [
            SystemKeys::USER_SEQ_KEY,
            SystemKeys::USER_EMAIL_SEQ_KEY,
            SystemKeys::ORG_SEQ_KEY,
            SystemKeys::APP_SEQ_KEY,
            SystemKeys::VAULT_SEQ_KEY,
            SystemKeys::EMAIL_VERIFY_SEQ_KEY,
            SystemKeys::CLIENT_ASSERTION_SEQ_KEY,
            SystemKeys::SIGNING_KEY_SEQ_KEY,
            SystemKeys::REFRESH_TOKEN_SEQ_KEY,
        ] {
            assert_eq!(
                SystemKeys::classify_key_tier(global_seq),
                Some(KeyTier::Global),
                "{global_seq} should remain Global after REGIONAL sequence addition",
            );
        }
    }

    #[test]
    fn test_totp_challenge_prefix_does_not_collide_with_onboard() {
        // `_tmp:totp_challenge:` and `_tmp:onboard_*:` are distinct
        assert!(!SystemKeys::TOTP_CHALLENGE_PREFIX.starts_with(SystemKeys::ONBOARD_VERIFY_PREFIX));
        assert!(!SystemKeys::TOTP_CHALLENGE_PREFIX.starts_with(SystemKeys::ONBOARD_ACCOUNT_PREFIX));
        assert!(!SystemKeys::ONBOARD_VERIFY_PREFIX.starts_with(SystemKeys::TOTP_CHALLENGE_PREFIX));
        assert!(!SystemKeys::ONBOARD_ACCOUNT_PREFIX.starts_with(SystemKeys::TOTP_CHALLENGE_PREFIX));
    }

    // ========================================================================
    // Invitation Key Tests
    // ========================================================================

    #[test]
    fn test_invite_key_generation() {
        let org = OrganizationId::new(42);
        let invite = InviteId::new(7);
        assert_eq!(SystemKeys::invite_key(org, invite), "invite:42:7");
    }

    #[test]
    fn test_parse_invite_key_roundtrip() {
        let org = OrganizationId::new(42);
        let invite = InviteId::new(7);
        let key = SystemKeys::invite_key(org, invite);
        let (parsed_org, parsed_invite) = SystemKeys::parse_invite_key(&key).unwrap();
        assert_eq!(parsed_org, org);
        assert_eq!(parsed_invite, invite);
    }

    #[test]
    fn test_parse_invite_key_invalid() {
        assert!(SystemKeys::parse_invite_key("user:42").is_none());
        assert!(SystemKeys::parse_invite_key("invite:abc:7").is_none());
        assert!(SystemKeys::parse_invite_key("invite:42").is_none());
    }

    #[test]
    fn test_invite_prefix_consistency() {
        let org = OrganizationId::new(42);
        let key = SystemKeys::invite_key(org, InviteId::new(1));
        let prefix = SystemKeys::invite_prefix(org);
        assert!(key.starts_with(&prefix));
        assert!(key.starts_with(SystemKeys::INVITE_PREFIX));
    }

    #[test]
    fn test_invite_slug_index_key() {
        let slug = InviteSlug::new(9876543210);
        assert_eq!(SystemKeys::invite_slug_index_key(slug), "_idx:invite:slug:9876543210");
    }

    #[test]
    fn test_invite_token_hash_index_key() {
        assert_eq!(
            SystemKeys::invite_token_hash_index_key("abcdef0123456789"),
            "_idx:invite:token_hash:abcdef0123456789"
        );
    }

    #[test]
    fn test_invite_email_hash_index_key() {
        let invite = InviteId::new(7);
        let key = SystemKeys::invite_email_hash_index_key("deadbeef", invite);
        assert_eq!(key, "_idx:invite:email_hash:deadbeef:7");
        // Verify the key starts with the email hash prefix
        assert!(key.starts_with(SystemKeys::INVITE_EMAIL_HASH_INDEX_PREFIX));
    }

    #[test]
    fn test_invite_email_hash_prefix_scan() {
        let hmac = "deadbeef";
        let prefix = SystemKeys::invite_email_hash_prefix(hmac);
        assert_eq!(prefix, "_idx:invite:email_hash:deadbeef:");
        // Keys for different invites with the same email HMAC share this prefix
        let key1 = SystemKeys::invite_email_hash_index_key(hmac, InviteId::new(1));
        let key2 = SystemKeys::invite_email_hash_index_key(hmac, InviteId::new(2));
        assert!(key1.starts_with(&prefix));
        assert!(key2.starts_with(&prefix));
    }

    #[test]
    fn test_invite_seq_key() {
        assert_eq!(SystemKeys::INVITE_SEQ_KEY, "_meta:seq:invite");
    }

    #[test]
    fn test_invite_key_tier_classification() {
        let org = OrganizationId::new(1);
        let invite = InviteId::new(1);
        // REGIONAL: invitation record
        assert_eq!(
            SystemKeys::classify_key_tier(&SystemKeys::invite_key(org, invite)),
            Some(KeyTier::Regional),
        );
        // GLOBAL: slug index
        assert_eq!(
            SystemKeys::classify_key_tier(&SystemKeys::invite_slug_index_key(InviteSlug::new(1))),
            Some(KeyTier::Global),
        );
        // GLOBAL: token hash index
        assert_eq!(
            SystemKeys::classify_key_tier(&SystemKeys::invite_token_hash_index_key("abc")),
            Some(KeyTier::Global),
        );
        // GLOBAL: email hash index
        assert_eq!(
            SystemKeys::classify_key_tier(&SystemKeys::invite_email_hash_index_key("abc", invite)),
            Some(KeyTier::Global),
        );
        // GLOBAL: sequence counter
        assert_eq!(
            SystemKeys::classify_key_tier(SystemKeys::INVITE_SEQ_KEY),
            Some(KeyTier::Global),
        );
    }

    #[test]
    fn test_invite_prefix_does_not_collide_with_idx() {
        // Bare `invite:` prefix must not collide with `_idx:invite:`
        assert!(!SystemKeys::INVITE_PREFIX.starts_with("_idx:"));
        assert!(!"_idx:invite:".starts_with(SystemKeys::INVITE_PREFIX));
    }
}
