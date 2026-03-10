//! Service layer for `_system` organization operations.
//!
//! Provides high-level operations on the _system organization:
//! - Node registration and discovery
//! - Organization routing table management
//! - Sequence counter management for ID generation
//!
//! The _system organization uses organization_id = 0 and vault_id = 0.

use std::sync::Arc;

use chrono::Utc;
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{
    EmailVerifyTokenId, NodeId, Operation, OrganizationId, OrganizationSlug, Region, SetCondition,
    SigningKeyScope, TokenVersion, UserEmailId, UserId, UserRole, UserSlug, UserStatus, VaultId,
    VaultSlug, decode, encode,
};
use snafu::{ResultExt, Snafu};
use tracing::warn;

use super::{
    keys::SystemKeys,
    types::{
        EmailVerificationToken, ErasureAuditRecord, MigrationSummary, NodeInfo,
        OrganizationRegistry, OrganizationStatus, SubjectKey, User, UserDirectoryEntry,
        UserDirectoryStatus, UserEmail, UserMigrationEntry,
    },
};
use crate::state::{StateError, StateLayer};

/// The reserved organization ID for _system.
pub const SYSTEM_ORGANIZATION_ID: OrganizationId = OrganizationId::new(0);

/// Maximum number of nodes returned by `list_nodes`.
const MAX_LIST_NODES: usize = 1_000;
/// Maximum number of organizations returned by `list_organizations`.
const MAX_LIST_ORGANIZATIONS: usize = 10_000;
/// Maximum number of user directory entries returned by `list_user_directory`.
const MAX_LIST_USER_DIRECTORY: usize = 10_000;
/// Maximum number of users returned by `list_flat_users` and `list_erased_user_ids`.
const MAX_LIST_USERS: usize = 100_000;
/// Maximum number of email hash entries scanned by `erase_user`.
const MAX_EMAIL_HASH_SCAN: usize = 100_000;
/// Maximum number of erasure audit records returned.
const MAX_LIST_ERASURE_AUDITS: usize = 100_000;

/// The reserved vault ID for _system entities.
pub const SYSTEM_VAULT_ID: VaultId = VaultId::new(0);

/// Errors from system organization operations.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(super)))]
pub enum SystemError {
    /// Underlying state layer operation failed.
    #[snafu(display("State layer error: {source}"))]
    State {
        /// The underlying state layer error.
        #[snafu(source(from(StateError, Box::new)))]
        source: Box<StateError>,
    },

    /// Codec error during serialization/deserialization.
    #[snafu(display("Codec error: {source}"))]
    Codec {
        /// The underlying codec error.
        source: inferadb_ledger_types::CodecError,
    },

    /// Requested entity was not found.
    #[snafu(display("Not found: {entity}"))]
    NotFound {
        /// Description of the entity that was not found.
        entity: String,
    },

    /// Entity already exists (duplicate key).
    #[snafu(display("Already exists: {entity}"))]
    AlreadyExists {
        /// Description of the entity that already exists.
        entity: String,
    },

    /// A revocation scan was truncated at `MAX_TOKEN_SCAN`, meaning not all
    /// tokens were processed. This is a security error — partial revocation
    /// leaves tokens valid that should have been revoked.
    #[snafu(display(
        "Revocation scan truncated at {limit} entries for {operation} — {revoked} tokens revoked before truncation"
    ))]
    RevocationIncomplete {
        /// The operation that was truncated.
        operation: String,
        /// The scan limit that was hit.
        limit: usize,
        /// Number of tokens successfully revoked before truncation.
        revoked: u64,
    },
}

/// Result type for system organization operations.
pub type Result<T> = std::result::Result<T, SystemError>;

/// Service for reading from and writing to the `_system` organization.
///
/// All _system data is stored in organization_id=0, vault_id=0.
/// StateLayer is internally thread-safe via inferadb-ledger-store's MVCC.
pub struct SystemOrganizationService<B: StorageBackend> {
    pub(super) state: Arc<StateLayer<B>>,
}

impl<B: StorageBackend> SystemOrganizationService<B> {
    /// Creates a new system organization service.
    pub fn new(state: Arc<StateLayer<B>>) -> Self {
        Self { state }
    }

    // =========================================================================
    // Sequence Counters
    // =========================================================================

    /// Returns the next value from a sequence counter and increments it.
    ///
    /// If the counter doesn't exist, initializes it to `start_value`.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the underlying read or write fails.
    pub fn next_sequence(&self, key: &str, start_value: i64) -> Result<i64> {
        // StateLayer is internally thread-safe via inferadb-ledger-store MVCC
        // Read current value
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        let current = match entity_opt {
            Some(entity) => {
                let value_str = String::from_utf8_lossy(&entity.value);
                value_str.parse::<i64>().unwrap_or(start_value)
            },
            None => start_value,
        };

        // Increment and save
        let next_value = current + 1;
        let ops = vec![Operation::SetEntity {
            key: key.to_string(),
            value: next_value.to_string().into_bytes(),
            condition: None,
            expires_at: None,
        }];

        self.state
            .apply_operations(SYSTEM_VAULT_ID, &ops, 0) // height 0 for system ops
            .context(StateSnafu)?;

        Ok(current)
    }

    /// Returns the next organization ID.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the sequence counter read or write fails.
    pub fn next_organization_id(&self) -> Result<OrganizationId> {
        // Start at 1 because 0 is reserved for _system
        self.next_sequence(SystemKeys::ORG_SEQ_KEY, 1).map(OrganizationId::new)
    }

    /// Returns the next vault ID.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the sequence counter read or write fails.
    pub fn next_vault_id(&self) -> Result<VaultId> {
        self.next_sequence(SystemKeys::VAULT_SEQ_KEY, 1).map(VaultId::new)
    }

    // =========================================================================
    // Node Operations
    // =========================================================================

    /// Registers a node in the cluster.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::Codec`] if serialization fails, or
    /// [`SystemError::State`] if the write operation fails.
    pub fn register_node(&self, node: &NodeInfo) -> Result<()> {
        let key = SystemKeys::node_key(&node.node_id);
        let value = encode(node).context(CodecSnafu)?;

        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];

        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(())
    }

    /// Returns a node by ID.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the read fails, or
    /// [`SystemError::Codec`] if deserialization fails.
    pub fn get_node(&self, node_id: &NodeId) -> Result<Option<NodeInfo>> {
        let key = SystemKeys::node_key(node_id);

        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let node: NodeInfo = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(node))
            },
            None => Ok(None),
        }
    }

    /// Lists all registered nodes.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the underlying list operation fails.
    pub fn list_nodes(&self) -> Result<Vec<NodeInfo>> {
        let entities = self
            .state
            .list_entities(SYSTEM_VAULT_ID, Some(SystemKeys::NODE_PREFIX), None, MAX_LIST_NODES)
            .context(StateSnafu)?;

        let mut nodes = Vec::new();
        for entity in entities {
            match decode::<NodeInfo>(&entity.value) {
                Ok(node) => nodes.push(node),
                Err(e) => warn!(error = %e, "Skipping corrupt NodeInfo entry during list"),
            }
        }

        Ok(nodes)
    }

    /// Removes a node from the cluster.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the delete operation fails.
    pub fn remove_node(&self, node_id: &NodeId) -> Result<bool> {
        let key = SystemKeys::node_key(node_id);
        let ops = vec![Operation::DeleteEntity { key }];

        let statuses = self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(matches!(statuses.first(), Some(inferadb_ledger_types::WriteStatus::Deleted)))
    }

    // =========================================================================
    // Organization Registry Operations
    // =========================================================================

    /// Registers a new organization.
    ///
    /// Stores the organization registry entry and its slug index entry.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::Codec`] if serialization fails, or
    /// [`SystemError::State`] if the write operation fails.
    pub fn register_organization(
        &self,
        registry: &OrganizationRegistry,
        slug: OrganizationSlug,
    ) -> Result<()> {
        let key = SystemKeys::organization_key(registry.organization_id);
        let value = encode(registry).context(CodecSnafu)?;

        // Also create the slug index
        let slug_index_key = SystemKeys::organization_slug_key(slug);
        let slug_index_value = registry.organization_id.value().to_string().into_bytes();

        let ops = vec![
            Operation::SetEntity { key, value, condition: None, expires_at: None },
            Operation::SetEntity {
                key: slug_index_key,
                value: slug_index_value,
                condition: None,
                expires_at: None,
            },
        ];

        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(())
    }

    /// Returns an organization by ID.
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the read fails, or
    /// [`SystemError::Codec`] if deserialization fails.
    pub fn get_organization(
        &self,
        organization: OrganizationId,
    ) -> Result<Option<OrganizationRegistry>> {
        let key = SystemKeys::organization_key(organization);

        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let registry: OrganizationRegistry = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(registry))
            },
            None => Ok(None),
        }
    }

    /// Returns an organization by slug.
    ///
    /// Looks up the slug index to find the internal organization ID, then
    /// retrieves the full registry entry.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the index or registry read fails, or
    /// [`SystemError::Codec`] if deserialization fails.
    pub fn get_organization_by_slug(
        &self,
        slug: OrganizationSlug,
    ) -> Result<Option<OrganizationRegistry>> {
        let index_key = SystemKeys::organization_slug_key(slug);

        // Look up the organization ID from the slug index
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, index_key.as_bytes()).context(StateSnafu)?;
        let organization_id = match entity_opt {
            Some(entity) => {
                let id_str = String::from_utf8_lossy(&entity.value);
                id_str.parse::<OrganizationId>().ok()
            },
            None => None,
        };

        match organization_id {
            Some(id) => self.get_organization(id),
            None => Ok(None),
        }
    }

    /// Lists all organizations.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the underlying list operation fails.
    pub fn list_organizations(&self) -> Result<Vec<OrganizationRegistry>> {
        let entities = self
            .state
            .list_entities(
                SYSTEM_VAULT_ID,
                Some(SystemKeys::ORG_PREFIX),
                None,
                MAX_LIST_ORGANIZATIONS,
            )
            .context(StateSnafu)?;

        let mut organizations = Vec::new();
        for entity in entities {
            match decode::<OrganizationRegistry>(&entity.value) {
                Ok(registry) => organizations.push(registry),
                Err(e) => {
                    warn!(error = %e, "Skipping corrupt OrganizationRegistry entry during list")
                },
            }
        }

        Ok(organizations)
    }

    /// Updates organization status.
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::NotFound`] if the organization does not exist, or
    /// [`SystemError::Codec`] / [`SystemError::State`] if the update fails.
    pub fn update_organization_status(
        &self,
        organization: OrganizationId,
        slug: OrganizationSlug,
        status: OrganizationStatus,
    ) -> Result<()> {
        // Get existing registry
        let mut registry = self.get_organization(organization)?.ok_or_else(|| {
            SystemError::NotFound { entity: format!("organization:{}", organization) }
        })?;

        // Update status
        registry.status = status;
        registry.config_version += 1;

        // Save
        self.register_organization(&registry, slug)
    }

    // =========================================================================
    // Shard Routing
    // =========================================================================

    /// Returns the region for an organization.
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] or [`SystemError::Codec`] if the
    /// organization lookup fails.
    pub fn get_region_for_organization(
        &self,
        organization: OrganizationId,
    ) -> Result<Option<Region>> {
        self.get_organization(organization).map(|opt| opt.map(|r| r.region))
    }

    /// Resolves a [`SigningKeyScope`] to the [`Region`] whose RMK protects it.
    ///
    /// - `Global` → `Region::GLOBAL`
    /// - `Organization(id)` → the organization's assigned region.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::NotFound`] if the organization does not exist.
    pub fn resolve_scope_region(&self, scope: &SigningKeyScope) -> Result<Region> {
        match scope {
            SigningKeyScope::Global => Ok(Region::GLOBAL),
            SigningKeyScope::Organization(org_id) => self
                .get_region_for_organization(*org_id)?
                .ok_or_else(|| SystemError::NotFound { entity: format!("Organization {org_id}") }),
        }
    }

    /// Assigns an organization to a region.
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::NotFound`] if the organization does not exist, or
    /// [`SystemError::Codec`] / [`SystemError::State`] if the update fails.
    pub fn assign_organization_to_region(
        &self,
        organization: OrganizationId,
        slug: OrganizationSlug,
        region: Region,
        member_nodes: Vec<NodeId>,
    ) -> Result<()> {
        let mut registry = self.get_organization(organization)?.ok_or_else(|| {
            SystemError::NotFound { entity: format!("organization:{}", organization) }
        })?;

        registry.region = region;
        registry.member_nodes = member_nodes;
        registry.config_version += 1;

        self.register_organization(&registry, slug)
    }

    // ========================================================================
    // Vault Slug Index
    // ========================================================================

    /// Registers a vault slug → internal ID mapping.
    ///
    /// * `vault` - Internal vault identifier (`VaultId`).
    ///
    /// Stores the mapping in the system vault for persistent slug resolution.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the write operation fails.
    pub fn register_vault_slug(&self, slug: VaultSlug, vault: VaultId) -> Result<()> {
        let key = SystemKeys::vault_slug_key(slug);
        let value = vault.value().to_string().into_bytes();

        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];

        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(())
    }

    /// Removes a vault slug mapping.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the delete operation fails.
    pub fn remove_vault_slug(&self, slug: VaultSlug) -> Result<()> {
        let key = SystemKeys::vault_slug_key(slug);

        let ops = vec![Operation::DeleteEntity { key }];

        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(())
    }

    /// Looks up the internal vault ID for a given vault slug.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the read operation fails.
    pub fn get_vault_id_by_slug(&self, slug: VaultSlug) -> Result<Option<VaultId>> {
        let key = SystemKeys::vault_slug_key(slug);

        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;

        Ok(entity_opt.and_then(|entity| {
            let id_str = String::from_utf8_lossy(&entity.value);
            id_str.parse::<VaultId>().ok()
        }))
    }

    // ========================================================================
    // User Directory (GLOBAL control plane)
    // ========================================================================

    /// Registers a user directory entry in the GLOBAL control plane.
    ///
    /// Stores the non-PII directory entry at `_sys:user:{user_id}` and
    /// the slug index at `_idx:user:slug:{user_slug}`. The slug is
    /// extracted from `entry.slug` — callers must set it before registration.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::NotFound`] if `entry.slug` is `None`,
    /// [`SystemError::Codec`] if serialization fails, or
    /// [`SystemError::State`] if the write operation fails.
    pub fn register_user_directory(&self, entry: &UserDirectoryEntry) -> Result<()> {
        let slug = entry.slug.ok_or_else(|| SystemError::NotFound {
            entity: format!("user_directory:{} has no slug", entry.user),
        })?;

        let key = SystemKeys::user_directory_key(entry.user);
        let value = encode(entry).context(CodecSnafu)?;

        let slug_index_key = SystemKeys::user_slug_index_key(slug);
        let slug_index_value = entry.user.value().to_string().into_bytes();

        let ops = vec![
            Operation::SetEntity { key, value, condition: None, expires_at: None },
            Operation::SetEntity {
                key: slug_index_key,
                value: slug_index_value,
                condition: None,
                expires_at: None,
            },
        ];

        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(())
    }

    /// Returns a user directory entry by user ID.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the read fails, or
    /// [`SystemError::Codec`] if deserialization fails.
    pub fn get_user_directory(&self, user_id: UserId) -> Result<Option<UserDirectoryEntry>> {
        let key = SystemKeys::user_directory_key(user_id);

        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let entry: UserDirectoryEntry = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(entry))
            },
            None => Ok(None),
        }
    }

    /// Resolves a user slug to a user ID via the GLOBAL slug index.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the read operation fails.
    pub fn get_user_id_by_slug(&self, slug: UserSlug) -> Result<Option<UserId>> {
        let key = SystemKeys::user_slug_index_key(slug);

        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;

        Ok(entity_opt.and_then(|entity| {
            let id_str = String::from_utf8_lossy(&entity.value);
            id_str.parse::<UserId>().ok()
        }))
    }

    /// Returns a user directory entry by slug.
    ///
    /// Looks up the slug index to find the user ID, then retrieves the
    /// full directory entry.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the read fails, or
    /// [`SystemError::Codec`] if deserialization fails.
    pub fn get_user_directory_by_slug(&self, slug: UserSlug) -> Result<Option<UserDirectoryEntry>> {
        match self.get_user_id_by_slug(slug)? {
            Some(user_id) => self.get_user_directory(user_id),
            None => Ok(None),
        }
    }

    /// Updates the status of a user directory entry.
    ///
    /// Rejected if the current status is `Deleted` (permanent tombstone).
    /// When transitioning to [`UserDirectoryStatus::Deleted`], optional fields
    /// (`slug`, `region`, `updated_at`) are set to `None` for tombstone
    /// minimization, and the slug index entry is removed.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::NotFound`] if the directory entry does not exist,
    /// [`SystemError::AlreadyExists`] if the entry is already deleted, or
    /// [`SystemError::Codec`] / [`SystemError::State`] if the update fails.
    pub fn update_user_directory_status(
        &self,
        user_id: UserId,
        status: UserDirectoryStatus,
    ) -> Result<()> {
        let mut entry = self
            .get_user_directory(user_id)?
            .ok_or_else(|| SystemError::NotFound { entity: format!("user_directory:{user_id}") })?;

        // Deleted is a permanent tombstone — reject any transition from it
        if entry.status == UserDirectoryStatus::Deleted {
            return Err(SystemError::AlreadyExists {
                entity: format!("user_directory:{user_id} is deleted (permanent tombstone)"),
            });
        }

        let old_slug = entry.slug;
        entry.status = status;

        if status == UserDirectoryStatus::Deleted {
            // Tombstone minimization: clear optional PII-adjacent fields
            entry.slug = None;
            entry.region = None;
            entry.updated_at = None;
        } else {
            entry.updated_at = Some(Utc::now());
        }

        let key = SystemKeys::user_directory_key(user_id);
        let value = encode(&entry).context(CodecSnafu)?;

        let mut ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];

        // Remove slug index on deletion
        if status == UserDirectoryStatus::Deleted
            && let Some(slug) = old_slug
        {
            ops.push(Operation::DeleteEntity { key: SystemKeys::user_slug_index_key(slug) });
        }

        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(())
    }

    /// Updates the region of a user directory entry (for migration).
    ///
    /// Rejected if the entry is in `Deleted` status (permanent tombstone).
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::NotFound`] if the directory entry does not exist
    /// or is deleted, or [`SystemError::Codec`] / [`SystemError::State`] if
    /// the update fails.
    pub fn update_user_directory_region(&self, user_id: UserId, region: Region) -> Result<()> {
        let mut entry = self
            .get_user_directory(user_id)?
            .ok_or_else(|| SystemError::NotFound { entity: format!("user_directory:{user_id}") })?;

        if entry.status == UserDirectoryStatus::Deleted {
            return Err(SystemError::AlreadyExists {
                entity: format!("user_directory:{user_id} is deleted (permanent tombstone)"),
            });
        }

        entry.region = Some(region);
        entry.updated_at = Some(Utc::now());

        let key = SystemKeys::user_directory_key(user_id);
        let value = encode(&entry).context(CodecSnafu)?;

        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(())
    }

    /// Lists all user directory entries in the GLOBAL control plane.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the underlying list operation fails.
    pub fn list_user_directory(&self) -> Result<Vec<UserDirectoryEntry>> {
        let entities = self
            .state
            .list_entities(
                SYSTEM_VAULT_ID,
                Some(SystemKeys::USER_DIRECTORY_PREFIX),
                None,
                MAX_LIST_USER_DIRECTORY,
            )
            .context(StateSnafu)?;

        let mut entries = Vec::new();
        for entity in entities {
            match decode::<UserDirectoryEntry>(&entity.value) {
                Ok(entry) => entries.push(entry),
                Err(e) => warn!(error = %e, "Skipping corrupt UserDirectoryEntry during list"),
            }
        }

        Ok(entries)
    }

    // ========================================================================
    // Email Hash Index (GLOBAL control plane)
    // ========================================================================

    /// Registers an email HMAC hash → user ID mapping in the global control plane.
    ///
    /// Uses compare-and-swap (`MustNotExist`) to guarantee global email uniqueness.
    /// If the HMAC already exists, returns [`SystemError::AlreadyExists`].
    ///
    /// # Arguments
    ///
    /// * `hmac_hex` - The hex-encoded `HMAC-SHA256(blinding_key, normalized_email)`.
    /// * `user_id` - The user ID to associate with this email hash.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::AlreadyExists`] if the HMAC is already registered,
    /// or [`SystemError::State`] for other storage failures.
    pub fn register_email_hash(&self, hmac_hex: &str, user_id: UserId) -> Result<()> {
        let key = SystemKeys::email_hash_index_key(hmac_hex);
        let value = user_id.value().to_string().into_bytes();

        let ops = vec![Operation::SetEntity {
            key: key.clone(),
            value,
            condition: Some(inferadb_ledger_types::SetCondition::MustNotExist),
            expires_at: None,
        }];

        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).map_err(|e| {
            if matches!(e, StateError::PreconditionFailed { .. }) {
                SystemError::AlreadyExists { entity: format!("email_hash:{hmac_hex}") }
            } else {
                SystemError::State { source: Box::new(e) }
            }
        })?;

        Ok(())
    }

    /// Looks up a user ID by email HMAC hash.
    ///
    /// Returns `None` if no user is registered with the given HMAC.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the read fails.
    pub fn get_email_hash(&self, hmac_hex: &str) -> Result<Option<UserId>> {
        let key = SystemKeys::email_hash_index_key(hmac_hex);

        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let id_str = String::from_utf8_lossy(&entity.value);
                let id: i64 = id_str.parse().map_err(|_| SystemError::NotFound {
                    entity: format!("email_hash:{hmac_hex}"),
                })?;
                Ok(Some(UserId::new(id)))
            },
            None => Ok(None),
        }
    }

    /// Removes an email HMAC hash from the global index.
    ///
    /// No-op if the HMAC doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the delete fails.
    pub fn remove_email_hash(&self, hmac_hex: &str) -> Result<()> {
        let key = SystemKeys::email_hash_index_key(hmac_hex);

        let ops = vec![Operation::DeleteEntity { key }];

        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(())
    }

    // ========================================================================
    // Blinding Key Metadata (GLOBAL control plane)
    // ========================================================================

    /// Returns the active email blinding key version, or `None` if not yet set.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the read fails.
    pub fn get_blinding_key_version(&self) -> Result<Option<u32>> {
        let entity_opt = self
            .state
            .get_entity(SYSTEM_VAULT_ID, SystemKeys::BLINDING_KEY_VERSION_KEY.as_bytes())
            .context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let version_str = String::from_utf8_lossy(&entity.value);
                let version: u32 = version_str.parse().map_err(|_| SystemError::NotFound {
                    entity: "blinding_key_version (corrupt)".to_string(),
                })?;
                Ok(Some(version))
            },
            None => Ok(None),
        }
    }

    /// Sets the active email blinding key version.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the write fails.
    pub fn set_blinding_key_version(&self, version: u32) -> Result<()> {
        let ops = vec![Operation::SetEntity {
            key: SystemKeys::BLINDING_KEY_VERSION_KEY.to_string(),
            value: version.to_string().into_bytes(),
            condition: None,
            expires_at: None,
        }];

        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(())
    }

    /// Returns the rehash progress (entries completed) for a region.
    ///
    /// Returns `None` if no rehash is in progress for that region.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the read fails.
    pub fn get_rehash_progress(&self, region: Region) -> Result<Option<u64>> {
        let key = SystemKeys::rehash_progress_key(region);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let count_str = String::from_utf8_lossy(&entity.value);
                let count: u64 = count_str.parse().map_err(|_| SystemError::NotFound {
                    entity: format!("rehash_progress:{}", region.as_str()),
                })?;
                Ok(Some(count))
            },
            None => Ok(None),
        }
    }

    /// Updates the rehash progress for a region.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the write fails.
    pub fn set_rehash_progress(&self, region: Region, entries_rehashed: u64) -> Result<()> {
        let key = SystemKeys::rehash_progress_key(region);
        let ops = vec![Operation::SetEntity {
            key,
            value: entries_rehashed.to_string().into_bytes(),
            condition: None,
            expires_at: None,
        }];

        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(())
    }

    /// Clears the rehash progress for a region (rotation complete for that region).
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the delete fails.
    pub fn clear_rehash_progress(&self, region: Region) -> Result<()> {
        let key = SystemKeys::rehash_progress_key(region);
        let ops = vec![Operation::DeleteEntity { key }];

        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(())
    }

    /// Checks whether a blinding key rotation is in progress.
    ///
    /// A rotation is in progress if any `_meta:blinding_key_rehash_progress:*`
    /// entries exist. This is used during registration to determine whether
    /// dual-key checking is required.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the scan fails.
    pub fn is_rotation_in_progress(&self) -> Result<bool> {
        let entities = self
            .state
            .list_entities(
                SYSTEM_VAULT_ID,
                Some(SystemKeys::REHASH_PROGRESS_PREFIX),
                None,
                1, // Only need to know if any exist
            )
            .context(StateSnafu)?;

        Ok(!entities.is_empty())
    }

    // ========================================================================
    // Crypto-Shredding Erasure (GLOBAL control plane)
    // ========================================================================

    /// Erases a user's PII via crypto-shredding finalization sequence.
    ///
    /// Forward-only, idempotent on crash-resume. Each step is safe to re-execute:
    /// 1. Read directory entry to capture slug (needed for index removal).
    /// 2. Mark directory entry as `Deleted` (tombstone minimization).
    /// 3. Remove global email hash index entries for this user.
    /// 4. Delete per-subject encryption key.
    /// 5. Write erasure audit record (insert-if-absent for idempotency).
    ///
    /// After erasure, the `UserDirectoryEntry` retains only `user: UserId` and
    /// `status: Deleted`. The subject key is destroyed, rendering all
    /// subject-encrypted PII cryptographically unrecoverable.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::NotFound`] if the user directory entry does not
    /// exist, or [`SystemError::Codec`] / [`SystemError::State`] if any
    /// underlying operation fails.
    pub fn erase_user(&self, user_id: UserId, erased_by: &str, region: Region) -> Result<()> {
        // Step 1: Read directory entry (may already be Deleted on re-execution).
        let entry_opt = self.get_user_directory(user_id)?;

        // Step 2: Mark directory entry as Deleted (idempotent — already-Deleted
        // is handled gracefully). Tombstone minimization clears slug, region,
        // updated_at and removes slug index entry.
        if let Some(entry) = &entry_opt
            && entry.status != UserDirectoryStatus::Deleted
        {
            self.update_user_directory_status(user_id, UserDirectoryStatus::Deleted)?;
        }

        // Step 3: Remove global email hash index entries for this user.
        // Scan all email hash entries and delete those pointing to this user.
        let email_hashes = self
            .state
            .list_entities(
                SYSTEM_VAULT_ID,
                Some(SystemKeys::EMAIL_HASH_INDEX_PREFIX),
                None,
                MAX_EMAIL_HASH_SCAN,
            )
            .context(StateSnafu)?;

        let user_id_bytes = user_id.value().to_string().into_bytes();
        let mut delete_ops: Vec<Operation> = Vec::new();
        for entity in &email_hashes {
            if entity.value == user_id_bytes {
                let key = String::from_utf8_lossy(&entity.key).to_string();
                delete_ops.push(Operation::DeleteEntity { key });
            }
        }
        if !delete_ops.is_empty() {
            self.state.apply_operations(SYSTEM_VAULT_ID, &delete_ops, 0).context(StateSnafu)?;
        }

        // Step 4: Delete per-subject encryption key.
        // B-tree delete removes the logical entry; underlying bytes are encrypted
        // ciphertext (via EncryptedBackend), rendered unrecoverable.
        let subject_key_key = SystemKeys::subject_key(user_id);
        let delete_key_ops = vec![Operation::DeleteEntity { key: subject_key_key }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &delete_key_ops, 0).context(StateSnafu)?;

        // Step 5: Write erasure audit record (insert-if-absent for idempotency).
        let audit_record = ErasureAuditRecord {
            user_id,
            erased_at: Utc::now(),
            erased_by: erased_by.to_string(),
            region,
        };
        let audit_key = SystemKeys::erasure_audit_key(user_id);
        let audit_value = encode(&audit_record).context(CodecSnafu)?;

        // Use SetEntity without condition — overwrites are idempotent since
        // the audit record contains the same user_id and region.
        let audit_ops = vec![Operation::SetEntity {
            key: audit_key,
            value: audit_value,
            condition: None,
            expires_at: None,
        }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &audit_ops, 0).context(StateSnafu)?;

        Ok(())
    }

    /// Returns the erasure audit record for a user, if one exists.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the read fails, or
    /// [`SystemError::Codec`] if deserialization fails.
    pub fn get_erasure_audit(&self, user_id: UserId) -> Result<Option<ErasureAuditRecord>> {
        let key = SystemKeys::erasure_audit_key(user_id);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let record: ErasureAuditRecord = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(record))
            },
            None => Ok(None),
        }
    }

    /// Stores a per-subject encryption key for a user.
    ///
    /// The key is stored in the system vault, encrypted at rest by the
    /// region's RMK via `EncryptedBackend`.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::Codec`] or [`SystemError::State`] if the
    /// write fails.
    pub fn store_subject_key(&self, user_id: UserId, key_bytes: &[u8; 32]) -> Result<()> {
        let subject_key = SubjectKey { user_id, key: *key_bytes, created_at: Utc::now() };
        let storage_key = SystemKeys::subject_key(user_id);
        let value = encode(&subject_key).context(CodecSnafu)?;

        let ops = vec![Operation::SetEntity {
            key: storage_key,
            value,
            condition: None,
            expires_at: None,
        }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(())
    }

    /// Returns a per-subject encryption key for a user, if one exists.
    ///
    /// Returns `None` if the key has been erased (crypto-shredding).
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the read fails, or
    /// [`SystemError::Codec`] if deserialization fails.
    pub fn get_subject_key(&self, user_id: UserId) -> Result<Option<SubjectKey>> {
        let key = SystemKeys::subject_key(user_id);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let subject_key: SubjectKey = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(subject_key))
            },
            None => Ok(None),
        }
    }

    /// Returns the set of erased user IDs by scanning erasure audit records.
    ///
    /// Used to populate snapshot tombstone sets and `ShardManager` erased
    /// user tracking at startup.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the scan fails.
    pub fn list_erased_user_ids(&self) -> Result<std::collections::HashSet<UserId>> {
        let entities = self
            .state
            .list_entities(
                SYSTEM_VAULT_ID,
                Some(SystemKeys::ERASURE_AUDIT_PREFIX),
                None,
                MAX_LIST_ERASURE_AUDITS,
            )
            .context(StateSnafu)?;

        let mut erased = std::collections::HashSet::new();
        for entity in &entities {
            let key_str = String::from_utf8_lossy(&entity.key);
            if let Some(user_id) = SystemKeys::parse_erasure_audit_key(&key_str) {
                erased.insert(user_id);
            }
        }
        Ok(erased)
    }

    // ========================================================================
    // User Migration (flat _system → regional)
    // ========================================================================

    /// Migrates pre-computed user entries from flat `_system` store to
    /// regional directory structure.
    ///
    /// For each entry in `entries`:
    /// 1. Creates `UserDirectoryEntry` in GLOBAL (`_sys:user:{id}`).
    /// 2. Creates slug index (`_idx:user:slug:{slug}`).
    /// 3. Creates email hash index (`_idx:email_hash:{hmac}`).
    /// 4. Stores per-subject encryption key (`_key:user:{id}`).
    /// 5. Removes old plaintext email index (`_idx:email:{email}`) for email records belonging to
    ///    this user.
    ///
    /// The caller (admin handler) pre-computes HMACs and subject keys so the
    /// blinding key never enters the Raft log.
    ///
    /// Returns a [`MigrationSummary`] counting total, migrated, skipped, errors.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] or [`SystemError::Codec`] if underlying
    /// operations fail.
    pub fn migrate_existing_users(
        &self,
        entries: &[UserMigrationEntry],
    ) -> Result<MigrationSummary> {
        let mut migrated: u64 = 0;
        let mut skipped: u64 = 0;
        let mut errors: u64 = 0;

        for entry in entries {
            // Idempotency: skip if directory entry already exists.
            match self.get_user_directory(entry.user) {
                Ok(Some(_)) => {
                    skipped += 1;
                    continue;
                },
                Ok(None) => {},
                Err(_) => {
                    errors += 1;
                    continue;
                },
            }

            // Step 1+2: Create directory entry and slug index.
            let directory_entry = UserDirectoryEntry {
                user: entry.user,
                slug: Some(entry.slug),
                region: Some(entry.region),
                status: UserDirectoryStatus::Active,
                updated_at: Some(Utc::now()),
            };
            if self.register_user_directory(&directory_entry).is_err() {
                errors += 1;
                continue;
            }

            // Step 3: Create email hash index (CAS for uniqueness).
            // AlreadyExists means another user owns this email — count as error.
            if self.register_email_hash(&entry.hmac, entry.user).is_err() {
                errors += 1;
                continue;
            }

            // Step 4: Store per-subject encryption key.
            if self.store_subject_key(entry.user, &entry.bytes).is_err() {
                errors += 1;
                continue;
            }

            // Step 5: Remove old plaintext email index entries for this user.
            // Non-fatal: directory/key entries already created.
            let _ = self.remove_plaintext_email_indexes_for_user(entry.user);

            migrated += 1;
        }

        Ok(MigrationSummary { users: entries.len() as u64, migrated, skipped, errors })
    }

    /// Removes plaintext `_idx:email:*` entries that reference emails belonging
    /// to the given user.
    ///
    /// Reads the `_idx:user_emails:{user_id}` index to find the user's email
    /// IDs, then reads each `user_email:{id}` record to get the email address,
    /// and deletes the corresponding `_idx:email:{email}` entries.
    fn remove_plaintext_email_indexes_for_user(&self, user_id: UserId) -> Result<()> {
        // Read the user_emails index to find email IDs.
        let emails_index_key = SystemKeys::user_emails_index_key(user_id);
        let emails_index_entity = self
            .state
            .get_entity(SYSTEM_VAULT_ID, emails_index_key.as_bytes())
            .context(StateSnafu)?;

        let Some(emails_entity) = emails_index_entity else {
            return Ok(());
        };

        // The index value is a list of email IDs (serialized).
        // Try to decode as a Vec<UserEmailId> first, fall back to
        // reading individual email records by scanning.
        let email_ids: Vec<inferadb_ledger_types::UserEmailId> = match decode(&emails_entity.value)
        {
            Ok(ids) => ids,
            Err(_) => return Ok(()), // Index format unrecognizable, skip cleanup.
        };

        let mut delete_ops: Vec<Operation> = Vec::new();
        for email_id in &email_ids {
            let email_key = SystemKeys::user_email_key(*email_id);
            if let Ok(Some(email_entity)) =
                self.state.get_entity(SYSTEM_VAULT_ID, email_key.as_bytes())
            {
                let user_email: UserEmail = match decode(&email_entity.value) {
                    Ok(ue) => ue,
                    Err(_) => continue,
                };
                let idx_key = SystemKeys::email_index_key(&user_email.email);
                delete_ops.push(Operation::DeleteEntity { key: idx_key });
            }
        }

        if !delete_ops.is_empty() {
            self.state.apply_operations(SYSTEM_VAULT_ID, &delete_ops, 0).context(StateSnafu)?;
        }

        Ok(())
    }

    /// Lists all user records from the flat `_system` store.
    ///
    /// Scans `user:*` keys in the system vault and deserializes each as a
    /// [`User`] record. Used by the migration admin handler to enumerate
    /// users that need migration. Returns at most 100,000 records.
    pub fn list_flat_users(&self) -> Result<Vec<User>> {
        let entities = self
            .state
            .list_entities(SYSTEM_VAULT_ID, Some(SystemKeys::USER_PREFIX), None, MAX_LIST_USERS)
            .context(StateSnafu)?;

        let mut users = Vec::new();
        for entity in &entities {
            let key_str = String::from_utf8_lossy(&entity.key);
            // Skip non-user keys that happen to share the prefix.
            if SystemKeys::parse_user_key(&key_str).is_none() {
                continue;
            }
            match decode::<User>(&entity.value) {
                Ok(user) => users.push(user),
                Err(e) => warn!(error = %e, "Skipping corrupt User entry during list_flat_users"),
            }
        }

        Ok(users)
    }

    /// Reads a single user email record by its email ID.
    ///
    /// Returns the [`UserEmail`] if found, `None` otherwise.
    pub fn get_user_email(
        &self,
        email_id: inferadb_ledger_types::UserEmailId,
    ) -> Result<Option<UserEmail>> {
        let key = SystemKeys::user_email_key(email_id);
        let entity = self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;

        match entity {
            Some(e) => {
                let user_email: UserEmail = decode(&e.value).context(CodecSnafu)?;
                Ok(Some(user_email))
            },
            None => Ok(None),
        }
    }

    // =========================================================================
    // User CRUD Operations
    // =========================================================================

    /// Creates a user and their primary email record atomically.
    ///
    /// Writes the user record, email record, user-to-emails index, email
    /// uniqueness index, and directory entry. The email uniqueness index uses
    /// `MustNotExist` to prevent duplicate email addresses.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::AlreadyExists`] if the email address is already
    /// registered, [`SystemError::Codec`] if serialization fails, or
    /// [`SystemError::State`] for other storage failures.
    pub fn create_user(
        &self,
        name: &str,
        email: &str,
        region: Region,
        role: UserRole,
        slug: UserSlug,
    ) -> Result<(UserId, UserEmailId)> {
        let user_id = self.next_sequence(SystemKeys::USER_SEQ_KEY, 1).map(UserId::new)?;
        let email_id =
            self.next_sequence(SystemKeys::USER_EMAIL_SEQ_KEY, 1).map(UserEmailId::new)?;

        let now = Utc::now();
        let user = User {
            id: user_id,
            slug,
            region,
            name: name.to_string(),
            email: email_id,
            status: UserStatus::Active,
            role,
            created_at: now,
            updated_at: now,
            deleted_at: None,
            version: TokenVersion::default(),
        };
        let user_key = SystemKeys::user_key(user_id);
        let user_value = encode(&user).context(CodecSnafu)?;

        let email_lower = email.to_lowercase();
        let user_email = UserEmail {
            id: email_id,
            user: user_id,
            email: email_lower.clone(),
            created_at: now,
            verified_at: None,
        };
        let email_key = SystemKeys::user_email_key(email_id);
        let email_value = encode(&user_email).context(CodecSnafu)?;

        let emails_index_key = SystemKeys::user_emails_index_key(user_id);
        let email_ids: Vec<UserEmailId> = vec![email_id];
        let emails_index_value = encode(&email_ids).context(CodecSnafu)?;

        let email_idx_key = SystemKeys::email_index_key(&email_lower);
        let email_idx_value = email_id.value().to_string().into_bytes();

        let ops = vec![
            Operation::SetEntity {
                key: user_key,
                value: user_value,
                condition: None,
                expires_at: None,
            },
            Operation::SetEntity {
                key: email_key,
                value: email_value,
                condition: None,
                expires_at: None,
            },
            Operation::SetEntity {
                key: emails_index_key,
                value: emails_index_value,
                condition: None,
                expires_at: None,
            },
            Operation::SetEntity {
                key: email_idx_key,
                value: email_idx_value,
                condition: Some(SetCondition::MustNotExist),
                expires_at: None,
            },
        ];

        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).map_err(|e| {
            if matches!(e, StateError::PreconditionFailed { .. }) {
                SystemError::AlreadyExists { entity: format!("email:{email_lower}") }
            } else {
                SystemError::State { source: Box::new(e) }
            }
        })?;

        let dir_entry = UserDirectoryEntry {
            user: user_id,
            slug: Some(slug),
            region: Some(region),
            status: UserDirectoryStatus::Active,
            updated_at: Some(now),
        };
        self.register_user_directory(&dir_entry)?;

        Ok((user_id, email_id))
    }

    /// Returns a user by internal ID.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the read fails, or
    /// [`SystemError::Codec`] if deserialization fails.
    pub fn get_user(&self, user_id: UserId) -> Result<Option<User>> {
        let key = SystemKeys::user_key(user_id);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let user: User = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(user))
            },
            None => Ok(None),
        }
    }

    /// Returns all email records for a user.
    ///
    /// Reads the user-to-emails index, then fetches each email record by ID.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if any read fails, or
    /// [`SystemError::Codec`] if deserialization fails.
    pub fn get_user_emails(&self, user_id: UserId) -> Result<Vec<UserEmail>> {
        let email_ids = self.get_user_email_ids(user_id)?;
        let mut emails = Vec::new();
        for email_id in email_ids {
            if let Some(email) = self.get_user_email(email_id)? {
                emails.push(email);
            }
        }
        Ok(emails)
    }

    /// Partially updates a user record.
    ///
    /// Only fields with `Some` values are updated. When changing the primary
    /// email, verifies that the email belongs to this user.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::NotFound`] if the user does not exist or the
    /// specified email is not owned by the user, [`SystemError::Codec`] if
    /// serialization fails, or [`SystemError::State`] for storage failures.
    pub fn update_user(
        &self,
        user_id: UserId,
        name: Option<&str>,
        role: Option<UserRole>,
        primary_email: Option<UserEmailId>,
    ) -> Result<User> {
        let mut user = self
            .get_user(user_id)?
            .ok_or_else(|| SystemError::NotFound { entity: format!("user:{user_id}") })?;

        if let Some(name) = name {
            user.name = name.to_string();
        }
        if let Some(role) = role {
            user.role = role;
        }
        if let Some(email_id) = primary_email {
            let email = self.get_user_email(email_id)?.ok_or_else(|| SystemError::NotFound {
                entity: format!("user_email:{email_id}"),
            })?;
            if email.user != user_id {
                return Err(SystemError::NotFound {
                    entity: format!("user_email:{email_id} not owned by user:{user_id}"),
                });
            }
            user.email = email_id;
        }
        user.updated_at = Utc::now();

        let key = SystemKeys::user_key(user_id);
        let value = encode(&user).context(CodecSnafu)?;
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(user)
    }

    /// Initiates soft-delete by setting the user status to `Deleting`.
    ///
    /// Records `deleted_at` timestamp. Rejects users already in `Deleting`
    /// or `Deleted` state.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::NotFound`] if the user does not exist,
    /// [`SystemError::AlreadyExists`] if already in a deletion state,
    /// [`SystemError::Codec`] if serialization fails, or
    /// [`SystemError::State`] for storage failures.
    pub fn soft_delete_user(&self, user_id: UserId) -> Result<User> {
        let mut user = self
            .get_user(user_id)?
            .ok_or_else(|| SystemError::NotFound { entity: format!("user:{user_id}") })?;

        if user.status == UserStatus::Deleting || user.status == UserStatus::Deleted {
            return Err(SystemError::AlreadyExists {
                entity: format!("user:{user_id} is already in deletion state"),
            });
        }

        let now = Utc::now();
        user.status = UserStatus::Deleting;
        user.deleted_at = Some(now);
        user.updated_at = now;

        let key = SystemKeys::user_key(user_id);
        let value = encode(&user).context(CodecSnafu)?;
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(user)
    }

    /// Lists users with optional pagination.
    ///
    /// Scans user records by prefix, filtering out non-user keys that share
    /// the prefix.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the scan fails.
    pub fn list_users(&self, start_after: Option<&str>, limit: usize) -> Result<Vec<User>> {
        let entities = self
            .state
            .list_entities(SYSTEM_VAULT_ID, Some(SystemKeys::USER_PREFIX), start_after, limit)
            .context(StateSnafu)?;

        let mut users = Vec::new();
        for entity in &entities {
            let key_str = String::from_utf8_lossy(&entity.key);
            if SystemKeys::parse_user_key(&key_str).is_none() {
                continue;
            }
            match decode::<User>(&entity.value) {
                Ok(user) => users.push(user),
                Err(e) => warn!(error = %e, "Skipping corrupt User entry during list_erased_users"),
            }
        }
        Ok(users)
    }

    /// Searches for a user by email address.
    ///
    /// Looks up the email uniqueness index, then reads the email record to
    /// find the owning user.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the read fails, or
    /// [`SystemError::Codec`] if deserialization fails.
    pub fn search_users_by_email(&self, email: &str) -> Result<Option<User>> {
        let email_lower = email.to_lowercase();
        let idx_key = SystemKeys::email_index_key(&email_lower);

        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, idx_key.as_bytes()).context(StateSnafu)?;
        let Some(entity) = entity_opt else {
            return Ok(None);
        };

        let email_id_str = String::from_utf8_lossy(&entity.value);
        let email_id: i64 = email_id_str
            .parse()
            .map_err(|_| SystemError::NotFound { entity: format!("email_index:{email_lower}") })?;
        let email_record = self.get_user_email(UserEmailId::new(email_id))?;

        match email_record {
            Some(email_rec) => self.get_user(email_rec.user),
            None => Ok(None),
        }
    }

    /// Adds an email address to an existing user.
    ///
    /// Creates the email record, email uniqueness index (with CAS), and
    /// updates the user-to-emails index.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::NotFound`] if the user does not exist,
    /// [`SystemError::State`] for storage failures (including duplicate
    /// email via CAS rejection), or [`SystemError::Codec`] if serialization
    /// fails.
    pub fn create_user_email_record(&self, user_id: UserId, email: &str) -> Result<UserEmailId> {
        let _ = self
            .get_user(user_id)?
            .ok_or_else(|| SystemError::NotFound { entity: format!("user:{user_id}") })?;

        let email_id =
            self.next_sequence(SystemKeys::USER_EMAIL_SEQ_KEY, 1).map(UserEmailId::new)?;
        let now = Utc::now();
        let email_lower = email.to_lowercase();

        let user_email = UserEmail {
            id: email_id,
            user: user_id,
            email: email_lower.clone(),
            created_at: now,
            verified_at: None,
        };
        let email_key = SystemKeys::user_email_key(email_id);
        let email_value = encode(&user_email).context(CodecSnafu)?;

        let idx_key = SystemKeys::email_index_key(&email_lower);
        let idx_value = email_id.value().to_string().into_bytes();

        let ops = vec![
            Operation::SetEntity {
                key: email_key,
                value: email_value,
                condition: None,
                expires_at: None,
            },
            Operation::SetEntity {
                key: idx_key,
                value: idx_value,
                condition: Some(SetCondition::MustNotExist),
                expires_at: None,
            },
        ];

        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).map_err(|e| {
            if matches!(e, StateError::PreconditionFailed { .. }) {
                SystemError::AlreadyExists { entity: format!("email:{email_lower}") }
            } else {
                SystemError::State { source: Box::new(e) }
            }
        })?;

        // Update user-to-emails index.
        let mut email_ids = self.get_user_email_ids(user_id)?;
        email_ids.push(email_id);
        let index_key = SystemKeys::user_emails_index_key(user_id);
        let index_value = encode(&email_ids).context(CodecSnafu)?;
        let index_ops = vec![Operation::SetEntity {
            key: index_key,
            value: index_value,
            condition: None,
            expires_at: None,
        }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &index_ops, 0).context(StateSnafu)?;

        Ok(email_id)
    }

    /// Removes a non-primary email address from a user.
    ///
    /// Deletes the email record, email uniqueness index, and updates the
    /// user-to-emails index. Refuses to delete the user's primary email.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::AlreadyExists`] if attempting to delete the
    /// primary email, [`SystemError::NotFound`] if the user or email does not
    /// exist or is not owned by this user, [`SystemError::Codec`] if
    /// serialization fails, or [`SystemError::State`] for storage failures.
    pub fn delete_user_email_record(&self, user_id: UserId, email_id: UserEmailId) -> Result<()> {
        let user = self
            .get_user(user_id)?
            .ok_or_else(|| SystemError::NotFound { entity: format!("user:{user_id}") })?;

        if user.email == email_id {
            return Err(SystemError::AlreadyExists {
                entity: format!("user_email:{email_id} is the primary email and cannot be deleted"),
            });
        }

        let email = self
            .get_user_email(email_id)?
            .ok_or_else(|| SystemError::NotFound { entity: format!("user_email:{email_id}") })?;

        if email.user != user_id {
            return Err(SystemError::NotFound {
                entity: format!("user_email:{email_id} not owned by user:{user_id}"),
            });
        }

        let email_key = SystemKeys::user_email_key(email_id);
        let idx_key = SystemKeys::email_index_key(&email.email);
        let ops = vec![
            Operation::DeleteEntity { key: email_key },
            Operation::DeleteEntity { key: idx_key },
        ];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        // Update user-to-emails index.
        let mut email_ids = self.get_user_email_ids(user_id)?;
        email_ids.retain(|id| *id != email_id);
        let index_key = SystemKeys::user_emails_index_key(user_id);
        let index_value = encode(&email_ids).context(CodecSnafu)?;
        let index_ops = vec![Operation::SetEntity {
            key: index_key,
            value: index_value,
            condition: None,
            expires_at: None,
        }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &index_ops, 0).context(StateSnafu)?;

        Ok(())
    }

    /// Sets the `verified_at` timestamp on an email record.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::NotFound`] if the email record does not exist,
    /// [`SystemError::Codec`] if serialization fails, or
    /// [`SystemError::State`] for storage failures.
    pub fn verify_user_email_record(&self, email_id: UserEmailId) -> Result<UserEmail> {
        let mut email = self
            .get_user_email(email_id)?
            .ok_or_else(|| SystemError::NotFound { entity: format!("user_email:{email_id}") })?;

        email.verified_at = Some(Utc::now());

        let key = SystemKeys::user_email_key(email_id);
        let value = encode(&email).context(CodecSnafu)?;
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(email)
    }

    /// Reads the user-to-emails index for a given user.
    fn get_user_email_ids(&self, user_id: UserId) -> Result<Vec<UserEmailId>> {
        let key = SystemKeys::user_emails_index_key(user_id);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let ids: Vec<UserEmailId> = decode(&entity.value).context(CodecSnafu)?;
                Ok(ids)
            },
            None => Ok(Vec::new()),
        }
    }

    // ========================================================================
    // Verification Token Lookups
    // ========================================================================

    /// Looks up a verification token by hashing the plaintext token and
    /// querying the hash index.
    ///
    /// Returns the full [`EmailVerificationToken`] record if found, or `None`
    /// if no token matches the hash.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] for storage failures or
    /// [`SystemError::Codec`] if deserialization fails.
    pub fn get_verification_token_by_hash(
        &self,
        plaintext_token: &str,
    ) -> Result<Option<EmailVerificationToken>> {
        let token_hash = inferadb_ledger_types::sha256(plaintext_token.as_bytes());
        let hash_hex = {
            use std::fmt::Write;
            token_hash.iter().fold(String::with_capacity(64), |mut acc, b| {
                let _ = write!(acc, "{b:02x}");
                acc
            })
        };
        let index_key = SystemKeys::email_verify_hash_index_key(&hash_hex);

        let index_entity =
            self.state.get_entity(SYSTEM_VAULT_ID, index_key.as_bytes()).context(StateSnafu)?;

        let Some(entity) = index_entity else {
            return Ok(None);
        };

        let token_id: EmailVerifyTokenId = decode(&entity.value).context(CodecSnafu)?;
        let token_key = SystemKeys::email_verify_key(token_id);

        let token_entity =
            self.state.get_entity(SYSTEM_VAULT_ID, token_key.as_bytes()).context(StateSnafu)?;

        match token_entity {
            Some(entity) => {
                let token: EmailVerificationToken = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(token))
            },
            None => Ok(None),
        }
    }

    // ========================================================================
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::net::SocketAddr;

    use chrono::Utc;
    use inferadb_ledger_types::{Region, UserRole, UserSlug, UserStatus};

    use super::*;
    use crate::engine::InMemoryStorageEngine;

    fn create_test_service() -> SystemOrganizationService<inferadb_ledger_store::InMemoryBackend> {
        let engine = InMemoryStorageEngine::open().unwrap();
        let state = Arc::new(StateLayer::new(engine.db()));
        SystemOrganizationService::new(state)
    }

    #[test]
    fn test_next_organization_id() {
        let svc = create_test_service();

        let id1 = svc.next_organization_id().unwrap();
        let id2 = svc.next_organization_id().unwrap();
        let id3 = svc.next_organization_id().unwrap();

        assert_eq!(id1, OrganizationId::new(1)); // Starts at 1, 0 is reserved
        assert_eq!(id2, OrganizationId::new(2));
        assert_eq!(id3, OrganizationId::new(3));
    }

    #[test]
    fn test_register_and_get_node() {
        let svc = create_test_service();

        let node = NodeInfo {
            node_id: NodeId::new("node-1"),
            addresses: vec!["10.0.0.1:5000".parse::<SocketAddr>().unwrap()],
            grpc_port: 5001,
            region: Region::US_EAST_VA,
            last_heartbeat: Utc::now(),
            joined_at: Utc::now(),
        };

        svc.register_node(&node).unwrap();

        let retrieved = svc.get_node(&NodeId::new("node-1")).unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().node_id, NodeId::new("node-1"));
    }

    #[test]
    fn test_list_nodes() {
        let svc = create_test_service();

        for i in 1..=3 {
            let node = NodeInfo {
                node_id: NodeId::new(format!("node-{}", i)),
                addresses: vec![format!("10.0.0.{}:5000", i).parse::<SocketAddr>().unwrap()],
                grpc_port: 5001,
                region: Region::US_EAST_VA,
                last_heartbeat: Utc::now(),
                joined_at: Utc::now(),
            };
            svc.register_node(&node).unwrap();
        }

        let nodes = svc.list_nodes().unwrap();
        assert_eq!(nodes.len(), 3);
    }

    #[test]
    fn test_register_node_persists_region() {
        let svc = create_test_service();

        let node = NodeInfo {
            node_id: NodeId::new("node-eu"),
            addresses: vec!["10.0.0.1:5000".parse::<SocketAddr>().unwrap()],
            grpc_port: 5001,
            region: Region::IE_EAST_DUBLIN,
            last_heartbeat: Utc::now(),
            joined_at: Utc::now(),
        };

        svc.register_node(&node).unwrap();

        let retrieved = svc.get_node(&NodeId::new("node-eu")).unwrap().unwrap();
        assert_eq!(retrieved.region, Region::IE_EAST_DUBLIN);
    }

    #[test]
    fn test_list_nodes_returns_regions() {
        let svc = create_test_service();

        let regions = [Region::US_EAST_VA, Region::IE_EAST_DUBLIN, Region::JP_EAST_TOKYO];

        for (i, region) in regions.iter().enumerate() {
            let node = NodeInfo {
                node_id: NodeId::new(format!("node-{}", i)),
                addresses: vec![format!("10.0.0.{}:5000", i + 1).parse::<SocketAddr>().unwrap()],
                grpc_port: 5001,
                region: *region,
                last_heartbeat: Utc::now(),
                joined_at: Utc::now(),
            };
            svc.register_node(&node).unwrap();
        }

        let nodes = svc.list_nodes().unwrap();
        assert_eq!(nodes.len(), 3);

        // Verify each node has the correct region
        for (i, region) in regions.iter().enumerate() {
            let node = svc.get_node(&NodeId::new(format!("node-{}", i))).unwrap().unwrap();
            assert_eq!(node.region, *region);
        }
    }

    #[test]
    fn test_register_and_get_organization() {
        let svc = create_test_service();

        let registry = OrganizationRegistry {
            organization_id: OrganizationId::new(1),
            region: Region::GLOBAL,
            member_nodes: vec![NodeId::new("node-1"), NodeId::new("node-2")],
            status: OrganizationStatus::Active,
            config_version: 1,
            created_at: Utc::now(),
            deleted_at: None,
        };

        let slug = OrganizationSlug::new(9999);
        svc.register_organization(&registry, slug).unwrap();

        // Get by ID
        let retrieved = svc.get_organization(OrganizationId::new(1)).unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().organization_id, OrganizationId::new(1));

        // Get by slug
        let by_slug = svc.get_organization_by_slug(slug).unwrap();
        assert!(by_slug.is_some());
        assert_eq!(by_slug.unwrap().organization_id, OrganizationId::new(1));
    }

    #[test]
    fn test_list_organizations() {
        let svc = create_test_service();

        for i in 1..=3 {
            let registry = OrganizationRegistry {
                organization_id: OrganizationId::new(i),
                region: Region::GLOBAL,
                member_nodes: vec![],
                status: OrganizationStatus::Active,
                config_version: 1,
                created_at: Utc::now(),
                deleted_at: None,
            };
            svc.register_organization(&registry, OrganizationSlug::new(1000 + i as u64)).unwrap();
        }

        let organizations = svc.list_organizations().unwrap();
        assert_eq!(organizations.len(), 3);
    }

    #[test]
    fn test_update_organization_status() {
        let svc = create_test_service();

        let slug = OrganizationSlug::new(5555);
        let registry = OrganizationRegistry {
            organization_id: OrganizationId::new(1),
            region: Region::GLOBAL,
            member_nodes: vec![],
            status: OrganizationStatus::Active,
            config_version: 1,
            created_at: Utc::now(),
            deleted_at: None,
        };
        svc.register_organization(&registry, slug).unwrap();

        svc.update_organization_status(OrganizationId::new(1), slug, OrganizationStatus::Suspended)
            .unwrap();

        let updated = svc.get_organization(OrganizationId::new(1)).unwrap().unwrap();
        assert_eq!(updated.status, OrganizationStatus::Suspended);
        assert_eq!(updated.config_version, 2); // Incremented
    }

    #[test]
    fn test_register_and_get_vault_slug() {
        let svc = create_test_service();

        let slug = VaultSlug::new(12345);
        let vault_id = VaultId::new(1);

        svc.register_vault_slug(slug, vault_id).unwrap();

        let result = svc.get_vault_id_by_slug(slug).unwrap();
        assert_eq!(result, Some(vault_id));
    }

    #[test]
    fn test_get_vault_slug_not_found() {
        let svc = create_test_service();

        let result = svc.get_vault_id_by_slug(VaultSlug::new(99999)).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_remove_vault_slug() {
        let svc = create_test_service();

        let slug = VaultSlug::new(11111);
        let vault_id = VaultId::new(5);

        svc.register_vault_slug(slug, vault_id).unwrap();
        assert!(svc.get_vault_id_by_slug(slug).unwrap().is_some());

        svc.remove_vault_slug(slug).unwrap();
        assert_eq!(svc.get_vault_id_by_slug(slug).unwrap(), None);
    }

    #[test]
    fn test_multiple_vault_slugs() {
        let svc = create_test_service();

        let slug1 = VaultSlug::new(100);
        let slug2 = VaultSlug::new(200);
        let vault1 = VaultId::new(1);
        let vault2 = VaultId::new(2);

        svc.register_vault_slug(slug1, vault1).unwrap();
        svc.register_vault_slug(slug2, vault2).unwrap();

        assert_eq!(svc.get_vault_id_by_slug(slug1).unwrap(), Some(vault1));
        assert_eq!(svc.get_vault_id_by_slug(slug2).unwrap(), Some(vault2));
    }

    #[test]
    fn test_vault_and_org_slugs_independent() {
        let svc = create_test_service();

        // Register an organization and vault with the same numeric slug value
        let organization = OrganizationSlug::new(42);
        let vault = VaultSlug::new(42);
        let vault_id = VaultId::new(7);

        let registry = OrganizationRegistry {
            organization_id: OrganizationId::new(1),
            region: Region::GLOBAL,
            member_nodes: vec![],
            status: OrganizationStatus::Active,
            config_version: 1,
            created_at: Utc::now(),
            deleted_at: None,
        };

        svc.register_organization(&registry, organization).unwrap();
        svc.register_vault_slug(vault, vault_id).unwrap();

        // Org slug lookup returns org, not vault
        let org = svc.get_organization_by_slug(organization).unwrap();
        assert!(org.is_some());
        assert_eq!(org.unwrap().organization_id, OrganizationId::new(1));

        // Vault slug lookup returns vault, not org
        let resolved = svc.get_vault_id_by_slug(vault).unwrap();
        assert_eq!(resolved, Some(vault_id));
    }

    // =========================================================================
    // User Directory Tests
    // =========================================================================

    fn make_directory_entry(user_id: i64, slug: u64, region: Region) -> UserDirectoryEntry {
        UserDirectoryEntry {
            user: UserId::new(user_id),
            slug: Some(UserSlug::new(slug)),
            region: Some(region),
            status: UserDirectoryStatus::Active,
            updated_at: Some(Utc::now()),
        }
    }

    #[test]
    fn test_register_and_get_user_directory() {
        let svc = create_test_service();

        let entry = make_directory_entry(1, 10001, Region::US_EAST_VA);

        svc.register_user_directory(&entry).unwrap();

        let retrieved = svc.get_user_directory(UserId::new(1)).unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.user, UserId::new(1));
        assert_eq!(retrieved.slug, Some(UserSlug::new(10001)));
        assert_eq!(retrieved.region, Some(Region::US_EAST_VA));
        assert_eq!(retrieved.status, UserDirectoryStatus::Active);
    }

    #[test]
    fn test_get_user_directory_not_found() {
        let svc = create_test_service();
        let result = svc.get_user_directory(UserId::new(999)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_user_directory_slug_lookup() {
        let svc = create_test_service();

        let entry = make_directory_entry(1, 10001, Region::IE_EAST_DUBLIN);
        let slug = UserSlug::new(10001);

        svc.register_user_directory(&entry).unwrap();

        // Slug → UserId
        let user_id = svc.get_user_id_by_slug(slug).unwrap();
        assert_eq!(user_id, Some(UserId::new(1)));

        // Slug → full entry
        let by_slug = svc.get_user_directory_by_slug(slug).unwrap();
        assert!(by_slug.is_some());
        assert_eq!(by_slug.unwrap().region, Some(Region::IE_EAST_DUBLIN));
    }

    #[test]
    fn test_user_directory_slug_not_found() {
        let svc = create_test_service();
        let result = svc.get_user_id_by_slug(UserSlug::new(99999)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_update_user_directory_status_to_migrating() {
        let svc = create_test_service();

        let entry = make_directory_entry(1, 10001, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();

        svc.update_user_directory_status(UserId::new(1), UserDirectoryStatus::Migrating).unwrap();

        let updated = svc.get_user_directory(UserId::new(1)).unwrap().unwrap();
        assert_eq!(updated.status, UserDirectoryStatus::Migrating);
        // Slug and region still present during migration
        assert!(updated.slug.is_some());
        assert!(updated.region.is_some());
    }

    #[test]
    fn test_update_user_directory_status_to_deleted() {
        let svc = create_test_service();

        let entry = make_directory_entry(1, 10001, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();

        svc.update_user_directory_status(UserId::new(1), UserDirectoryStatus::Deleted).unwrap();

        let tombstone = svc.get_user_directory(UserId::new(1)).unwrap().unwrap();
        assert_eq!(tombstone.status, UserDirectoryStatus::Deleted);
        // Tombstone minimization: optional fields cleared
        assert_eq!(tombstone.slug, None);
        assert_eq!(tombstone.region, None);
        assert_eq!(tombstone.updated_at, None);
        // UserId persists
        assert_eq!(tombstone.user, UserId::new(1));

        // Slug index removed on deletion
        let slug_lookup = svc.get_user_id_by_slug(UserSlug::new(10001)).unwrap();
        assert!(slug_lookup.is_none());
    }

    #[test]
    fn test_update_user_directory_status_not_found() {
        let svc = create_test_service();
        let result =
            svc.update_user_directory_status(UserId::new(999), UserDirectoryStatus::Deleted);
        assert!(result.is_err());
    }

    #[test]
    fn test_deleted_user_directory_rejects_status_change() {
        let svc = create_test_service();

        let entry = make_directory_entry(1, 10001, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();

        // Delete the user
        svc.update_user_directory_status(UserId::new(1), UserDirectoryStatus::Deleted).unwrap();

        // Deleted → Active rejected (permanent tombstone)
        let result = svc.update_user_directory_status(UserId::new(1), UserDirectoryStatus::Active);
        assert!(result.is_err());

        // Deleted → Migrating rejected
        let result =
            svc.update_user_directory_status(UserId::new(1), UserDirectoryStatus::Migrating);
        assert!(result.is_err());

        // Deleted → Deleted rejected
        let result = svc.update_user_directory_status(UserId::new(1), UserDirectoryStatus::Deleted);
        assert!(result.is_err());
    }

    #[test]
    fn test_deleted_user_directory_rejects_region_change() {
        let svc = create_test_service();

        let entry = make_directory_entry(1, 10001, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();

        svc.update_user_directory_status(UserId::new(1), UserDirectoryStatus::Deleted).unwrap();

        // Cannot update region on deleted entry
        let result = svc.update_user_directory_region(UserId::new(1), Region::IE_EAST_DUBLIN);
        assert!(result.is_err());
    }

    #[test]
    fn test_update_user_directory_region() {
        let svc = create_test_service();

        let entry = make_directory_entry(1, 10001, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();

        svc.update_user_directory_region(UserId::new(1), Region::IE_EAST_DUBLIN).unwrap();

        let updated = svc.get_user_directory(UserId::new(1)).unwrap().unwrap();
        assert_eq!(updated.region, Some(Region::IE_EAST_DUBLIN));
    }

    #[test]
    fn test_update_user_directory_region_not_found() {
        let svc = create_test_service();
        let result = svc.update_user_directory_region(UserId::new(999), Region::IE_EAST_DUBLIN);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_user_directory() {
        let svc = create_test_service();

        for i in 1..=3 {
            let entry = make_directory_entry(i, 10000 + i as u64, Region::US_EAST_VA);
            svc.register_user_directory(&entry).unwrap();
        }

        let entries = svc.list_user_directory().unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_user_directory_migration_lifecycle() {
        // Active → Migrating → Active (new region)
        let svc = create_test_service();

        let entry = make_directory_entry(1, 10001, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();

        // Start migration
        svc.update_user_directory_status(UserId::new(1), UserDirectoryStatus::Migrating).unwrap();

        let migrating = svc.get_user_directory(UserId::new(1)).unwrap().unwrap();
        assert_eq!(migrating.status, UserDirectoryStatus::Migrating);

        // Update region
        svc.update_user_directory_region(UserId::new(1), Region::IE_EAST_DUBLIN).unwrap();

        // Complete migration
        svc.update_user_directory_status(UserId::new(1), UserDirectoryStatus::Active).unwrap();

        let completed = svc.get_user_directory(UserId::new(1)).unwrap().unwrap();
        assert_eq!(completed.status, UserDirectoryStatus::Active);
        assert_eq!(completed.region, Some(Region::IE_EAST_DUBLIN));
        // Slug preserved through migration
        assert_eq!(completed.slug, Some(UserSlug::new(10001)));
    }

    #[test]
    fn test_user_directory_erasure_lifecycle() {
        // Active → Deleted (tombstone)
        let svc = create_test_service();

        let entry = make_directory_entry(1, 10001, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();

        svc.update_user_directory_status(UserId::new(1), UserDirectoryStatus::Deleted).unwrap();

        // Entry still exists as tombstone
        let tombstone = svc.get_user_directory(UserId::new(1)).unwrap();
        assert!(tombstone.is_some());
        let tombstone = tombstone.unwrap();
        assert_eq!(tombstone.status, UserDirectoryStatus::Deleted);

        // Slug is unreachable after erasure
        assert!(svc.get_user_id_by_slug(UserSlug::new(10001)).unwrap().is_none());
        assert!(svc.get_user_directory_by_slug(UserSlug::new(10001)).unwrap().is_none());
    }

    #[test]
    fn test_user_directory_does_not_collide_with_user_records() {
        // Directory entries use _sys:user: prefix, user records use user: prefix
        // Both can coexist in the same B-tree
        let svc = create_test_service();

        let entry = make_directory_entry(1, 10001, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();

        // User directory list should not pick up entries from user: prefix
        let entries = svc.list_user_directory().unwrap();
        assert_eq!(entries.len(), 1);

        // Node list should not pick up directory entries
        let nodes = svc.list_nodes().unwrap();
        assert!(nodes.is_empty());
    }

    #[test]
    fn test_multiple_user_directories_different_regions() {
        let svc = create_test_service();

        let regions = [Region::US_EAST_VA, Region::IE_EAST_DUBLIN, Region::JP_EAST_TOKYO];
        for (i, region) in regions.iter().enumerate() {
            let entry = make_directory_entry((i + 1) as i64, 10000 + (i + 1) as u64, *region);
            svc.register_user_directory(&entry).unwrap();
        }

        // Each slug resolves to the correct user
        for (i, _region) in regions.iter().enumerate() {
            let user_id = svc.get_user_id_by_slug(UserSlug::new(10000 + (i + 1) as u64)).unwrap();
            assert_eq!(user_id, Some(UserId::new((i + 1) as i64)));
        }

        // Each directory entry has the correct region
        for (i, region) in regions.iter().enumerate() {
            let entry = svc.get_user_directory(UserId::new((i + 1) as i64)).unwrap().unwrap();
            assert_eq!(entry.region, Some(*region));
        }
    }

    // ========================================================================
    // Email Hash Index Tests
    // ========================================================================

    #[test]
    fn test_register_and_get_email_hash() {
        let svc = create_test_service();
        let hmac = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";
        let user_id = UserId::new(42);

        svc.register_email_hash(hmac, user_id).unwrap();

        let result = svc.get_email_hash(hmac).unwrap();
        assert_eq!(result, Some(user_id));
    }

    #[test]
    fn test_get_email_hash_not_found() {
        let svc = create_test_service();
        let result = svc.get_email_hash("nonexistent_hmac").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_register_email_hash_duplicate_rejected() {
        let svc = create_test_service();
        let hmac = "aaaa1111bbbb2222cccc3333dddd4444eeee5555ffff6666aaaa1111bbbb2222";

        svc.register_email_hash(hmac, UserId::new(1)).unwrap();

        // Second registration with same HMAC must fail
        let result = svc.register_email_hash(hmac, UserId::new(2));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SystemError::AlreadyExists { .. }));

        // Original mapping preserved
        assert_eq!(svc.get_email_hash(hmac).unwrap(), Some(UserId::new(1)));
    }

    #[test]
    fn test_remove_email_hash() {
        let svc = create_test_service();
        let hmac = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

        svc.register_email_hash(hmac, UserId::new(99)).unwrap();
        assert!(svc.get_email_hash(hmac).unwrap().is_some());

        svc.remove_email_hash(hmac).unwrap();
        assert_eq!(svc.get_email_hash(hmac).unwrap(), None);
    }

    #[test]
    fn test_remove_email_hash_nonexistent_is_noop() {
        let svc = create_test_service();
        // Should not error
        svc.remove_email_hash("does_not_exist").unwrap();
    }

    #[test]
    fn test_email_hash_independent_per_hmac() {
        let svc = create_test_service();
        let hmac_a = "aaaa".repeat(16);
        let hmac_b = "bbbb".repeat(16);

        svc.register_email_hash(&hmac_a, UserId::new(1)).unwrap();
        svc.register_email_hash(&hmac_b, UserId::new(2)).unwrap();

        assert_eq!(svc.get_email_hash(&hmac_a).unwrap(), Some(UserId::new(1)));
        assert_eq!(svc.get_email_hash(&hmac_b).unwrap(), Some(UserId::new(2)));
    }

    #[test]
    fn test_register_email_hash_then_remove_allows_reregistration() {
        let svc = create_test_service();
        let hmac = "deadbeef".repeat(8);

        svc.register_email_hash(&hmac, UserId::new(1)).unwrap();
        svc.remove_email_hash(&hmac).unwrap();

        // After removal, the same HMAC can be claimed by a different user
        svc.register_email_hash(&hmac, UserId::new(2)).unwrap();
        assert_eq!(svc.get_email_hash(&hmac).unwrap(), Some(UserId::new(2)));
    }

    // ========================================================================
    // Blinding Key Metadata Tests
    // ========================================================================

    #[test]
    fn test_blinding_key_version_not_set() {
        let svc = create_test_service();
        assert_eq!(svc.get_blinding_key_version().unwrap(), None);
    }

    #[test]
    fn test_set_and_get_blinding_key_version() {
        let svc = create_test_service();
        svc.set_blinding_key_version(1).unwrap();
        assert_eq!(svc.get_blinding_key_version().unwrap(), Some(1));

        svc.set_blinding_key_version(2).unwrap();
        assert_eq!(svc.get_blinding_key_version().unwrap(), Some(2));
    }

    #[test]
    fn test_rehash_progress_lifecycle() {
        let svc = create_test_service();
        let region = Region::US_EAST_VA;

        // Initially no progress
        assert_eq!(svc.get_rehash_progress(region).unwrap(), None);

        // Set progress
        svc.set_rehash_progress(region, 50).unwrap();
        assert_eq!(svc.get_rehash_progress(region).unwrap(), Some(50));

        // Update progress
        svc.set_rehash_progress(region, 100).unwrap();
        assert_eq!(svc.get_rehash_progress(region).unwrap(), Some(100));

        // Clear progress
        svc.clear_rehash_progress(region).unwrap();
        assert_eq!(svc.get_rehash_progress(region).unwrap(), None);
    }

    #[test]
    fn test_rehash_progress_per_region() {
        let svc = create_test_service();

        svc.set_rehash_progress(Region::US_EAST_VA, 10).unwrap();
        svc.set_rehash_progress(Region::IE_EAST_DUBLIN, 20).unwrap();

        assert_eq!(svc.get_rehash_progress(Region::US_EAST_VA).unwrap(), Some(10));
        assert_eq!(svc.get_rehash_progress(Region::IE_EAST_DUBLIN).unwrap(), Some(20));
        assert_eq!(svc.get_rehash_progress(Region::JP_EAST_TOKYO).unwrap(), None);
    }

    #[test]
    fn test_is_rotation_in_progress() {
        let svc = create_test_service();

        // No rotation initially
        assert!(!svc.is_rotation_in_progress().unwrap());

        // Start a rotation
        svc.set_rehash_progress(Region::US_EAST_VA, 0).unwrap();
        assert!(svc.is_rotation_in_progress().unwrap());

        // Clear the region's progress
        svc.clear_rehash_progress(Region::US_EAST_VA).unwrap();
        assert!(!svc.is_rotation_in_progress().unwrap());
    }

    #[test]
    fn test_is_rotation_in_progress_multiple_regions() {
        let svc = create_test_service();

        svc.set_rehash_progress(Region::US_EAST_VA, 100).unwrap();
        svc.set_rehash_progress(Region::IE_EAST_DUBLIN, 50).unwrap();

        assert!(svc.is_rotation_in_progress().unwrap());

        // Clear one — still in progress because the other remains
        svc.clear_rehash_progress(Region::US_EAST_VA).unwrap();
        assert!(svc.is_rotation_in_progress().unwrap());

        // Clear the last
        svc.clear_rehash_progress(Region::IE_EAST_DUBLIN).unwrap();
        assert!(!svc.is_rotation_in_progress().unwrap());
    }

    // =========================================================================
    // Crypto-Shredding Erasure Tests
    // =========================================================================

    #[test]
    fn test_store_and_get_subject_key() {
        let svc = create_test_service();
        let user_id = UserId::new(42);
        let key_bytes = [0xABu8; 32];

        svc.store_subject_key(user_id, &key_bytes).unwrap();

        let retrieved = svc.get_subject_key(user_id).unwrap().unwrap();
        assert_eq!(retrieved.user_id, user_id);
        assert_eq!(retrieved.key, key_bytes);
    }

    #[test]
    fn test_subject_key_not_found_after_erasure() {
        let svc = create_test_service();
        let user_id = UserId::new(42);
        let key_bytes = [0xCDu8; 32];

        // Store a subject key
        svc.store_subject_key(user_id, &key_bytes).unwrap();
        assert!(svc.get_subject_key(user_id).unwrap().is_some());

        // Register directory entry (erase_user needs it)
        let entry = make_directory_entry(42, 10042, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();

        // Erase user
        svc.erase_user(user_id, "admin@test.com", Region::US_EAST_VA).unwrap();

        // Subject key is destroyed
        assert!(svc.get_subject_key(user_id).unwrap().is_none());
    }

    #[test]
    fn test_erase_user_tombstone_minimization() {
        let svc = create_test_service();
        let user_id = UserId::new(55);

        let entry = make_directory_entry(55, 10055, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();

        svc.erase_user(user_id, "gdpr-bot", Region::US_EAST_VA).unwrap();

        // Tombstone retains only user_id and status=Deleted
        let tombstone = svc.get_user_directory(user_id).unwrap().unwrap();
        assert_eq!(tombstone.status, UserDirectoryStatus::Deleted);
        assert!(tombstone.slug.is_none(), "slug should be cleared");
        assert!(tombstone.region.is_none(), "region should be cleared");
        assert!(tombstone.updated_at.is_none(), "updated_at should be cleared");

        // Slug index removed
        assert!(
            svc.get_user_id_by_slug(inferadb_ledger_types::UserSlug::new(10055)).unwrap().is_none()
        );
    }

    #[test]
    fn test_erase_user_removes_email_hash_entries() {
        let svc = create_test_service();
        let user_id = UserId::new(77);

        let entry = make_directory_entry(77, 10077, Region::IE_EAST_DUBLIN);
        svc.register_user_directory(&entry).unwrap();

        // Register email hashes for this user
        svc.register_email_hash("hash_alpha", user_id).unwrap();
        svc.register_email_hash("hash_beta", user_id).unwrap();

        // Verify they exist
        assert!(svc.get_email_hash("hash_alpha").unwrap().is_some());
        assert!(svc.get_email_hash("hash_beta").unwrap().is_some());

        // Erase user
        svc.erase_user(user_id, "compliance-officer", Region::IE_EAST_DUBLIN).unwrap();

        // Email hashes removed
        assert!(svc.get_email_hash("hash_alpha").unwrap().is_none());
        assert!(svc.get_email_hash("hash_beta").unwrap().is_none());
    }

    #[test]
    fn test_erase_user_creates_audit_record() {
        let svc = create_test_service();
        let user_id = UserId::new(88);

        let entry = make_directory_entry(88, 10088, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();

        svc.erase_user(user_id, "dpo@company.com", Region::US_EAST_VA).unwrap();

        let audit = svc.get_erasure_audit(user_id).unwrap().unwrap();
        assert_eq!(audit.user_id, user_id);
        assert_eq!(audit.erased_by, "dpo@company.com");
        assert_eq!(audit.region, Region::US_EAST_VA);
    }

    #[test]
    fn test_erase_user_idempotent() {
        let svc = create_test_service();
        let user_id = UserId::new(99);

        let entry = make_directory_entry(99, 10099, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();
        svc.store_subject_key(user_id, &[0x11u8; 32]).unwrap();
        svc.register_email_hash("idempotent_hash", user_id).unwrap();

        // First erasure
        svc.erase_user(user_id, "admin", Region::US_EAST_VA).unwrap();

        // Second erasure (idempotent — should not error)
        svc.erase_user(user_id, "admin", Region::US_EAST_VA).unwrap();

        // State is the same after both calls
        assert!(svc.get_subject_key(user_id).unwrap().is_none());
        assert!(svc.get_email_hash("idempotent_hash").unwrap().is_none());

        let tombstone = svc.get_user_directory(user_id).unwrap().unwrap();
        assert_eq!(tombstone.status, UserDirectoryStatus::Deleted);
    }

    #[test]
    fn test_erase_user_without_directory_entry() {
        let svc = create_test_service();
        let user_id = UserId::new(111);

        // No directory entry — erase should still succeed (idempotent,
        // handles the case where directory was never created or already
        // cleaned up).
        svc.erase_user(user_id, "system", Region::US_EAST_VA).unwrap();

        // Audit record still created
        assert!(svc.get_erasure_audit(user_id).unwrap().is_some());
    }

    #[test]
    fn test_erase_user_other_users_unaffected() {
        let svc = create_test_service();

        // Set up two users
        let entry1 = make_directory_entry(1, 10001, Region::US_EAST_VA);
        let entry2 = make_directory_entry(2, 10002, Region::US_EAST_VA);
        svc.register_user_directory(&entry1).unwrap();
        svc.register_user_directory(&entry2).unwrap();

        svc.store_subject_key(UserId::new(1), &[0xAAu8; 32]).unwrap();
        svc.store_subject_key(UserId::new(2), &[0xBBu8; 32]).unwrap();

        svc.register_email_hash("user1_hash", UserId::new(1)).unwrap();
        svc.register_email_hash("user2_hash", UserId::new(2)).unwrap();

        // Erase user 1 only
        svc.erase_user(UserId::new(1), "admin", Region::US_EAST_VA).unwrap();

        // User 2 is completely unaffected
        let user2 = svc.get_user_directory(UserId::new(2)).unwrap().unwrap();
        assert_eq!(user2.status, UserDirectoryStatus::Active);
        assert!(svc.get_subject_key(UserId::new(2)).unwrap().is_some());
        assert!(svc.get_email_hash("user2_hash").unwrap().is_some());
    }

    #[test]
    fn test_list_erased_user_ids() {
        let svc = create_test_service();

        // Initially empty
        let erased = svc.list_erased_user_ids().unwrap();
        assert!(erased.is_empty());

        // Erase two users
        let entry1 = make_directory_entry(10, 10010, Region::US_EAST_VA);
        let entry2 = make_directory_entry(20, 10020, Region::IE_EAST_DUBLIN);
        svc.register_user_directory(&entry1).unwrap();
        svc.register_user_directory(&entry2).unwrap();

        svc.erase_user(UserId::new(10), "admin", Region::US_EAST_VA).unwrap();
        svc.erase_user(UserId::new(20), "admin", Region::IE_EAST_DUBLIN).unwrap();

        let erased = svc.list_erased_user_ids().unwrap();
        assert_eq!(erased.len(), 2);
        assert!(erased.contains(&UserId::new(10)));
        assert!(erased.contains(&UserId::new(20)));
    }

    #[test]
    fn test_erase_user_end_to_end() {
        // Full lifecycle: create → store key → register email → erase → verify all gone
        let svc = create_test_service();
        let user_id = UserId::new(42);

        // Create user directory
        let entry = make_directory_entry(42, 10042, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();

        // Store subject key
        let key_bytes = [0xFFu8; 32];
        svc.store_subject_key(user_id, &key_bytes).unwrap();

        // Register email hash
        svc.register_email_hash("user42_email_hash", user_id).unwrap();

        // Verify everything exists pre-erasure
        assert!(svc.get_user_directory(user_id).unwrap().is_some());
        assert!(svc.get_subject_key(user_id).unwrap().is_some());
        assert!(svc.get_email_hash("user42_email_hash").unwrap().is_some());
        assert!(
            svc.get_user_id_by_slug(inferadb_ledger_types::UserSlug::new(10042)).unwrap().is_some()
        );
        assert!(svc.get_erasure_audit(user_id).unwrap().is_none());

        // Erase
        svc.erase_user(user_id, "gdpr-request-123", Region::US_EAST_VA).unwrap();

        // Verify everything is gone/tombstoned
        let tombstone = svc.get_user_directory(user_id).unwrap().unwrap();
        assert_eq!(tombstone.status, UserDirectoryStatus::Deleted);
        assert!(tombstone.slug.is_none());
        assert!(tombstone.region.is_none());
        assert!(tombstone.updated_at.is_none());

        assert!(svc.get_subject_key(user_id).unwrap().is_none());
        assert!(svc.get_email_hash("user42_email_hash").unwrap().is_none());
        assert!(
            svc.get_user_id_by_slug(inferadb_ledger_types::UserSlug::new(10042)).unwrap().is_none()
        );

        // Audit record exists
        let audit = svc.get_erasure_audit(user_id).unwrap().unwrap();
        assert_eq!(audit.erased_by, "gdpr-request-123");
        assert_eq!(audit.region, Region::US_EAST_VA);

        // Listed in erased users
        let erased = svc.list_erased_user_ids().unwrap();
        assert!(erased.contains(&user_id));
    }

    // ========================================================================
    // User Migration Tests
    // ========================================================================

    /// Helper: write a flat User record into the system vault.
    fn write_flat_user(
        svc: &SystemOrganizationService<inferadb_ledger_store::InMemoryBackend>,
        user: &User,
    ) {
        let key = SystemKeys::user_key(user.id);
        let value = encode(user).unwrap();
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        svc.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).unwrap();
    }

    /// Helper: write a flat UserEmail record into the system vault.
    fn write_flat_user_email(
        svc: &SystemOrganizationService<inferadb_ledger_store::InMemoryBackend>,
        email: &UserEmail,
    ) {
        let key = SystemKeys::user_email_key(email.id);
        let value = encode(email).unwrap();
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        svc.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).unwrap();
    }

    /// Helper: write a user_emails index entry (Vec<UserEmailId>).
    fn write_user_emails_index(
        svc: &SystemOrganizationService<inferadb_ledger_store::InMemoryBackend>,
        user_id: UserId,
        email_ids: &[inferadb_ledger_types::UserEmailId],
    ) {
        let key = SystemKeys::user_emails_index_key(user_id);
        let value = encode(&email_ids.to_vec()).unwrap();
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        svc.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).unwrap();
    }

    /// Helper: write a plaintext email index entry.
    fn write_email_index(
        svc: &SystemOrganizationService<inferadb_ledger_store::InMemoryBackend>,
        email: &str,
        email_id: inferadb_ledger_types::UserEmailId,
    ) {
        let key = SystemKeys::email_index_key(email);
        let value = email_id.value().to_string().into_bytes();
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        svc.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).unwrap();
    }

    fn make_test_user(id: i64, slug: u64, region: Region) -> User {
        User {
            id: UserId::new(id),
            slug: UserSlug::new(slug),
            region,
            name: format!("User {id}"),
            email: inferadb_ledger_types::UserEmailId::new(id * 100),
            status: UserStatus::default(),
            role: UserRole::default(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            deleted_at: None,
            version: TokenVersion::default(),
        }
    }

    fn make_test_user_email(id: i64, user_id: i64, email: &str) -> UserEmail {
        UserEmail {
            id: inferadb_ledger_types::UserEmailId::new(id),
            user: UserId::new(user_id),
            email: email.to_string(),
            created_at: Utc::now(),
            verified_at: Some(Utc::now()),
        }
    }

    #[test]
    fn test_list_flat_users_empty() {
        let svc = create_test_service();
        let users = svc.list_flat_users().unwrap();
        assert!(users.is_empty());
    }

    #[test]
    fn test_list_flat_users() {
        let svc = create_test_service();
        let user1 = make_test_user(1, 10001, Region::US_EAST_VA);
        let user2 = make_test_user(2, 10002, Region::IE_EAST_DUBLIN);
        write_flat_user(&svc, &user1);
        write_flat_user(&svc, &user2);

        let users = svc.list_flat_users().unwrap();
        assert_eq!(users.len(), 2);
        let ids: Vec<i64> = users.iter().map(|u| u.id.value()).collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
    }

    #[test]
    fn test_get_user_email() {
        let svc = create_test_service();
        let email_id = inferadb_ledger_types::UserEmailId::new(100);
        let email = make_test_user_email(100, 1, "alice@example.com");
        write_flat_user_email(&svc, &email);

        let result = svc.get_user_email(email_id).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().email, "alice@example.com");
    }

    #[test]
    fn test_get_user_email_not_found() {
        let svc = create_test_service();
        let result = svc.get_user_email(inferadb_ledger_types::UserEmailId::new(999)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_migrate_existing_users_single() {
        let svc = create_test_service();

        let entries = vec![UserMigrationEntry {
            user: UserId::new(1),
            slug: UserSlug::new(10001),
            region: Region::US_EAST_VA,
            hmac: "abc123def456".to_string(),
            bytes: [0xAAu8; 32],
        }];

        let summary = svc.migrate_existing_users(&entries).unwrap();
        assert_eq!(summary.users, 1);
        assert_eq!(summary.migrated, 1);
        assert_eq!(summary.skipped, 0);
        assert_eq!(summary.errors, 0);

        // Verify directory entry created.
        let dir = svc.get_user_directory(UserId::new(1)).unwrap().unwrap();
        assert_eq!(dir.user, UserId::new(1));
        assert_eq!(dir.slug, Some(UserSlug::new(10001)));
        assert_eq!(dir.region, Some(Region::US_EAST_VA));
        assert_eq!(dir.status, UserDirectoryStatus::Active);

        // Verify slug index created.
        let found_id = svc.get_user_id_by_slug(UserSlug::new(10001)).unwrap();
        assert_eq!(found_id, Some(UserId::new(1)));

        // Verify email hash index created.
        let email_hash_user = svc.get_email_hash("abc123def456").unwrap();
        assert_eq!(email_hash_user, Some(UserId::new(1)));

        // Verify subject key stored.
        let subject_key = svc.get_subject_key(UserId::new(1)).unwrap().unwrap();
        assert_eq!(subject_key.key, [0xAAu8; 32]);
    }

    #[test]
    fn test_migrate_existing_users_idempotent() {
        let svc = create_test_service();

        let entries = vec![UserMigrationEntry {
            user: UserId::new(1),
            slug: UserSlug::new(10001),
            region: Region::US_EAST_VA,
            hmac: "abc123".to_string(),
            bytes: [0xBBu8; 32],
        }];

        // First run: migrates.
        let summary1 = svc.migrate_existing_users(&entries).unwrap();
        assert_eq!(summary1.migrated, 1);
        assert_eq!(summary1.skipped, 0);

        // Second run: skips (directory entry exists).
        let summary2 = svc.migrate_existing_users(&entries).unwrap();
        assert_eq!(summary2.migrated, 0);
        assert_eq!(summary2.skipped, 1);
    }

    #[test]
    fn test_migrate_existing_users_empty() {
        let svc = create_test_service();
        let summary = svc.migrate_existing_users(&[]).unwrap();
        assert_eq!(summary.users, 0);
        assert_eq!(summary.migrated, 0);
        assert_eq!(summary.skipped, 0);
        assert_eq!(summary.errors, 0);
    }

    #[test]
    fn test_migrate_existing_users_multiple() {
        let svc = create_test_service();

        let entries = vec![
            UserMigrationEntry {
                user: UserId::new(1),
                slug: UserSlug::new(10001),
                region: Region::US_EAST_VA,
                hmac: "hash1".to_string(),
                bytes: [0x11u8; 32],
            },
            UserMigrationEntry {
                user: UserId::new(2),
                slug: UserSlug::new(10002),
                region: Region::IE_EAST_DUBLIN,
                hmac: "hash2".to_string(),
                bytes: [0x22u8; 32],
            },
            UserMigrationEntry {
                user: UserId::new(3),
                slug: UserSlug::new(10003),
                region: Region::JP_EAST_TOKYO,
                hmac: "hash3".to_string(),
                bytes: [0x33u8; 32],
            },
        ];

        let summary = svc.migrate_existing_users(&entries).unwrap();
        assert_eq!(summary.users, 3);
        assert_eq!(summary.migrated, 3);
        assert_eq!(summary.skipped, 0);
        assert_eq!(summary.errors, 0);

        // Verify all directory entries.
        for (i, entry) in entries.iter().enumerate() {
            let dir = svc.get_user_directory(entry.user).unwrap().unwrap();
            assert_eq!(dir.region, Some(entry.region), "user {} region mismatch", i);
        }
    }

    #[test]
    fn test_migrate_existing_users_mixed_skip_and_migrate() {
        let svc = create_test_service();

        // Pre-register user 1's directory entry.
        let existing = make_directory_entry(1, 10001, Region::US_EAST_VA);
        svc.register_user_directory(&existing).unwrap();

        let entries = vec![
            UserMigrationEntry {
                user: UserId::new(1),
                slug: UserSlug::new(10001),
                region: Region::US_EAST_VA,
                hmac: "hash1".to_string(),
                bytes: [0x11u8; 32],
            },
            UserMigrationEntry {
                user: UserId::new(2),
                slug: UserSlug::new(10002),
                region: Region::IE_EAST_DUBLIN,
                hmac: "hash2".to_string(),
                bytes: [0x22u8; 32],
            },
        ];

        let summary = svc.migrate_existing_users(&entries).unwrap();
        assert_eq!(summary.users, 2);
        assert_eq!(summary.migrated, 1);
        assert_eq!(summary.skipped, 1);
        assert_eq!(summary.errors, 0);

        // User 2 should be migrated.
        assert!(svc.get_user_directory(UserId::new(2)).unwrap().is_some());
    }

    #[test]
    fn test_remove_plaintext_email_indexes_for_user() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let email_id = inferadb_ledger_types::UserEmailId::new(100);

        // Write a flat user email record.
        let user_email = make_test_user_email(100, 1, "alice@example.com");
        write_flat_user_email(&svc, &user_email);

        // Write the user_emails index.
        write_user_emails_index(&svc, user_id, &[email_id]);

        // Write the plaintext email index.
        write_email_index(&svc, "alice@example.com", email_id);

        // Verify the index exists.
        let idx_key = SystemKeys::email_index_key("alice@example.com");
        let before = svc.state.get_entity(SYSTEM_VAULT_ID, idx_key.as_bytes()).unwrap();
        assert!(before.is_some());

        // Remove plaintext email indexes.
        svc.remove_plaintext_email_indexes_for_user(user_id).unwrap();

        // Verify the index is gone.
        let after = svc.state.get_entity(SYSTEM_VAULT_ID, idx_key.as_bytes()).unwrap();
        assert!(after.is_none());
    }

    #[test]
    fn test_remove_plaintext_email_indexes_no_index() {
        let svc = create_test_service();
        // No user_emails index exists — should be a no-op.
        svc.remove_plaintext_email_indexes_for_user(UserId::new(999)).unwrap();
    }

    // =========================================================================
    // User CRUD Tests
    // =========================================================================

    #[test]
    fn test_create_user_and_get() {
        let svc = create_test_service();
        let slug = UserSlug::new(100);
        let (user_id, email_id) = svc
            .create_user("Alice", "alice@example.com", Region::US_EAST_VA, UserRole::User, slug)
            .unwrap();

        let user = svc.get_user(user_id).unwrap().unwrap();
        assert_eq!(user.name, "Alice");
        assert_eq!(user.slug, slug);
        assert_eq!(user.email, email_id);
        assert_eq!(user.status, UserStatus::Active);
        assert!(user.deleted_at.is_none());
    }

    #[test]
    fn test_create_user_creates_email() {
        let svc = create_test_service();
        let (user_id, email_id) = svc
            .create_user(
                "Bob",
                "bob@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(200),
            )
            .unwrap();

        let emails = svc.get_user_emails(user_id).unwrap();
        assert_eq!(emails.len(), 1);
        assert_eq!(emails[0].email, "bob@example.com");
        assert_eq!(emails[0].id, email_id);
    }

    #[test]
    fn test_create_user_creates_directory_entry() {
        let svc = create_test_service();
        let slug = UserSlug::new(300);
        let (user_id, _) = svc
            .create_user(
                "Charlie",
                "charlie@example.com",
                Region::IE_EAST_DUBLIN,
                UserRole::User,
                slug,
            )
            .unwrap();

        let dir = svc.get_user_directory(user_id).unwrap().unwrap();
        assert_eq!(dir.slug, Some(slug));
        assert_eq!(dir.region, Some(Region::IE_EAST_DUBLIN));
        assert_eq!(dir.status, UserDirectoryStatus::Active);
    }

    #[test]
    fn test_create_user_duplicate_email_fails() {
        let svc = create_test_service();
        svc.create_user(
            "Alice",
            "same@example.com",
            Region::US_EAST_VA,
            UserRole::User,
            UserSlug::new(100),
        )
        .unwrap();

        let result = svc.create_user(
            "Bob",
            "same@example.com",
            Region::US_EAST_VA,
            UserRole::User,
            UserSlug::new(200),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_update_user_name() {
        let svc = create_test_service();
        let (user_id, _) = svc
            .create_user(
                "Alice",
                "alice@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(100),
            )
            .unwrap();

        let updated = svc.update_user(user_id, Some("Alicia"), None, None).unwrap();
        assert_eq!(updated.name, "Alicia");
    }

    #[test]
    fn test_update_user_role() {
        let svc = create_test_service();
        let (user_id, _) = svc
            .create_user(
                "Alice",
                "alice@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(100),
            )
            .unwrap();

        let updated = svc.update_user(user_id, None, Some(UserRole::Admin), None).unwrap();
        assert_eq!(updated.role, UserRole::Admin);
    }

    #[test]
    fn test_update_user_primary_email() {
        let svc = create_test_service();
        let (user_id, _) = svc
            .create_user(
                "Alice",
                "alice@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(100),
            )
            .unwrap();

        let new_email_id = svc.create_user_email_record(user_id, "alice2@example.com").unwrap();
        let updated = svc.update_user(user_id, None, None, Some(new_email_id)).unwrap();
        assert_eq!(updated.email, new_email_id);
    }

    #[test]
    fn test_update_user_primary_email_wrong_owner() {
        let svc = create_test_service();
        let (user_id, _) = svc
            .create_user(
                "Alice",
                "alice@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(100),
            )
            .unwrap();
        let (_, other_email_id) = svc
            .create_user(
                "Bob",
                "bob@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(200),
            )
            .unwrap();

        let result = svc.update_user(user_id, None, None, Some(other_email_id));
        assert!(result.is_err());
    }

    #[test]
    fn test_soft_delete_user() {
        let svc = create_test_service();
        let (user_id, _) = svc
            .create_user(
                "Alice",
                "alice@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(100),
            )
            .unwrap();

        let deleted = svc.soft_delete_user(user_id).unwrap();
        assert_eq!(deleted.status, UserStatus::Deleting);
        assert!(deleted.deleted_at.is_some());
    }

    #[test]
    fn test_soft_delete_already_deleting() {
        let svc = create_test_service();
        let (user_id, _) = svc
            .create_user(
                "Alice",
                "alice@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(100),
            )
            .unwrap();

        svc.soft_delete_user(user_id).unwrap();
        let result = svc.soft_delete_user(user_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_users() {
        let svc = create_test_service();
        svc.create_user(
            "Alice",
            "alice@example.com",
            Region::US_EAST_VA,
            UserRole::User,
            UserSlug::new(100),
        )
        .unwrap();
        svc.create_user(
            "Bob",
            "bob@example.com",
            Region::US_EAST_VA,
            UserRole::User,
            UserSlug::new(200),
        )
        .unwrap();

        let users = svc.list_users(None, 100).unwrap();
        assert_eq!(users.len(), 2);
    }

    #[test]
    fn test_search_users_by_email() {
        let svc = create_test_service();
        svc.create_user(
            "Alice",
            "alice@example.com",
            Region::US_EAST_VA,
            UserRole::User,
            UserSlug::new(100),
        )
        .unwrap();

        let found = svc.search_users_by_email("alice@example.com").unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "Alice");
    }

    #[test]
    fn test_search_users_by_email_not_found() {
        let svc = create_test_service();
        let found = svc.search_users_by_email("nobody@example.com").unwrap();
        assert!(found.is_none());
    }

    #[test]
    fn test_create_user_email_record() {
        let svc = create_test_service();
        let (user_id, _) = svc
            .create_user(
                "Alice",
                "alice@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(100),
            )
            .unwrap();

        let email_id = svc.create_user_email_record(user_id, "alice2@example.com").unwrap();
        let emails = svc.get_user_emails(user_id).unwrap();
        assert_eq!(emails.len(), 2);
        assert!(emails.iter().any(|e| e.id == email_id && e.email == "alice2@example.com"));
    }

    #[test]
    fn test_delete_user_email_record() {
        let svc = create_test_service();
        let (user_id, _) = svc
            .create_user(
                "Alice",
                "alice@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(100),
            )
            .unwrap();

        let email_id = svc.create_user_email_record(user_id, "alice2@example.com").unwrap();
        svc.delete_user_email_record(user_id, email_id).unwrap();

        let emails = svc.get_user_emails(user_id).unwrap();
        assert_eq!(emails.len(), 1); // Only primary remains
    }

    #[test]
    fn test_delete_primary_email_fails() {
        let svc = create_test_service();
        let (user_id, primary_email_id) = svc
            .create_user(
                "Alice",
                "alice@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(100),
            )
            .unwrap();

        let result = svc.delete_user_email_record(user_id, primary_email_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_user_email() {
        let svc = create_test_service();
        let (_, email_id) = svc
            .create_user(
                "Alice",
                "alice@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(100),
            )
            .unwrap();

        let email = svc.get_user_email(email_id).unwrap().unwrap();
        assert!(email.verified_at.is_none());

        let verified = svc.verify_user_email_record(email_id).unwrap();
        assert!(verified.verified_at.is_some());
    }

    #[test]
    fn test_search_users_by_email_case_insensitive() {
        let svc = create_test_service();
        svc.create_user(
            "Alice",
            "Alice@Example.COM",
            Region::US_EAST_VA,
            UserRole::User,
            UserSlug::new(100),
        )
        .unwrap();

        let found = svc.search_users_by_email("alice@example.com").unwrap();
        assert!(found.is_some());
    }

    #[test]
    fn test_get_user_directory_by_slug() {
        let svc = create_test_service();
        let slug = UserSlug::new(500);
        let (user_id, _) = svc
            .create_user("Alice", "alice@example.com", Region::US_EAST_VA, UserRole::User, slug)
            .unwrap();

        let dir = svc.get_user_directory_by_slug(slug).unwrap().unwrap();
        assert_eq!(dir.user, user_id);
    }
}
