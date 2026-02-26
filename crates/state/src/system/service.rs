//! Service layer for `_system` organization operations.
//!
//! Provides high-level operations on the _system organization:
//! - Node registration and discovery
//! - Organization routing table management
//! - Sequence counter management for ID generation
//!
//! The _system organization uses organization_id = 0 and vault_id = 0.

use std::sync::Arc;

use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{
    NodeId, Operation, OrganizationId, OrganizationSlug, ShardId, VaultId, VaultSlug, decode,
    encode,
};
use snafu::{ResultExt, Snafu};

use super::{
    keys::SystemKeys,
    types::{NodeInfo, OrganizationRegistry, OrganizationStatus},
};
use crate::state::{StateError, StateLayer};

/// The reserved organization ID for _system.
pub const SYSTEM_ORGANIZATION_ID: OrganizationId = OrganizationId::new(0);

/// The reserved vault ID for _system entities.
pub const SYSTEM_VAULT_ID: VaultId = VaultId::new(0);

/// Errors from system organization operations.
#[derive(Debug, Snafu)]
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
}

/// Result type for system organization operations.
pub type Result<T> = std::result::Result<T, SystemError>;

/// Service for reading from and writing to the `_system` organization.
///
/// All _system data is stored in organization_id=0, vault_id=0.
/// StateLayer is internally thread-safe via inferadb-ledger-store's MVCC.
pub struct SystemOrganizationService<B: StorageBackend> {
    state: Arc<StateLayer<B>>,
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
            .list_entities(SYSTEM_VAULT_ID, Some(SystemKeys::NODE_PREFIX), None, 1000)
            .context(StateSnafu)?;

        let mut nodes = Vec::new();
        for entity in entities {
            if let Ok(node) = decode::<NodeInfo>(&entity.value) {
                nodes.push(node);
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
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the read fails, or
    /// [`SystemError::Codec`] if deserialization fails.
    pub fn get_organization(
        &self,
        organization_id: OrganizationId,
    ) -> Result<Option<OrganizationRegistry>> {
        let key = SystemKeys::organization_key(organization_id);

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
            .list_entities(SYSTEM_VAULT_ID, Some(SystemKeys::ORG_PREFIX), None, 10000)
            .context(StateSnafu)?;

        let mut organizations = Vec::new();
        for entity in entities {
            if let Ok(registry) = decode::<OrganizationRegistry>(&entity.value) {
                organizations.push(registry);
            }
        }

        Ok(organizations)
    }

    /// Updates organization status.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::NotFound`] if the organization does not exist, or
    /// [`SystemError::Codec`] / [`SystemError::State`] if the update fails.
    pub fn update_organization_status(
        &self,
        organization_id: OrganizationId,
        slug: OrganizationSlug,
        status: OrganizationStatus,
    ) -> Result<()> {
        // Get existing registry
        let mut registry = self.get_organization(organization_id)?.ok_or_else(|| {
            SystemError::NotFound { entity: format!("organization:{}", organization_id) }
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

    /// Returns the shard ID for an organization.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] or [`SystemError::Codec`] if the
    /// organization lookup fails.
    pub fn get_shard_for_organization(
        &self,
        organization_id: OrganizationId,
    ) -> Result<Option<ShardId>> {
        self.get_organization(organization_id).map(|opt| opt.map(|r| r.shard_id))
    }

    /// Assigns an organization to a shard.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::NotFound`] if the organization does not exist, or
    /// [`SystemError::Codec`] / [`SystemError::State`] if the update fails.
    pub fn assign_organization_to_shard(
        &self,
        organization_id: OrganizationId,
        slug: OrganizationSlug,
        shard_id: ShardId,
        member_nodes: Vec<NodeId>,
    ) -> Result<()> {
        let mut registry = self.get_organization(organization_id)?.ok_or_else(|| {
            SystemError::NotFound { entity: format!("organization:{}", organization_id) }
        })?;

        registry.shard_id = shard_id;
        registry.member_nodes = member_nodes;
        registry.config_version += 1;

        self.register_organization(&registry, slug)
    }

    // ========================================================================
    // Vault Slug Index
    // ========================================================================

    /// Registers a vault slug → internal ID mapping.
    ///
    /// Stores the mapping in the system vault for persistent slug resolution.
    ///
    /// # Errors
    ///
    /// Returns [`SystemError::State`] if the write operation fails.
    pub fn register_vault_slug(&self, slug: VaultSlug, vault_id: VaultId) -> Result<()> {
        let key = SystemKeys::vault_slug_key(slug);
        let value = vault_id.value().to_string().into_bytes();

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
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::net::SocketAddr;

    use chrono::Utc;

    use super::{super::types::NodeRole, *};
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
            node_id: "node-1".to_string(),
            addresses: vec!["10.0.0.1:5000".parse::<SocketAddr>().unwrap()],
            grpc_port: 5001,
            role: NodeRole::Voter,
            last_heartbeat: Utc::now(),
            joined_at: Utc::now(),
        };

        svc.register_node(&node).unwrap();

        let retrieved = svc.get_node(&"node-1".to_string()).unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().node_id, "node-1");
    }

    #[test]
    fn test_list_nodes() {
        let svc = create_test_service();

        for i in 1..=3 {
            let node = NodeInfo {
                node_id: format!("node-{}", i),
                addresses: vec![format!("10.0.0.{}:5000", i).parse::<SocketAddr>().unwrap()],
                grpc_port: 5001,
                role: NodeRole::Voter,
                last_heartbeat: Utc::now(),
                joined_at: Utc::now(),
            };
            svc.register_node(&node).unwrap();
        }

        let nodes = svc.list_nodes().unwrap();
        assert_eq!(nodes.len(), 3);
    }

    #[test]
    fn test_register_and_get_organization() {
        let svc = create_test_service();

        let registry = OrganizationRegistry {
            organization_id: OrganizationId::new(1),
            name: "acme-corp".to_string(),
            shard_id: ShardId::new(1),
            member_nodes: vec!["node-1".to_string(), "node-2".to_string()],
            status: OrganizationStatus::Active,
            config_version: 1,
            created_at: Utc::now(),
        };

        let slug = OrganizationSlug::new(9999);
        svc.register_organization(&registry, slug).unwrap();

        // Get by ID
        let retrieved = svc.get_organization(OrganizationId::new(1)).unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, "acme-corp");

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
                name: format!("org-{}", i),
                shard_id: ShardId::new(1),
                member_nodes: vec![],
                status: OrganizationStatus::Active,
                config_version: 1,
                created_at: Utc::now(),
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
            name: "test-org".to_string(),
            shard_id: ShardId::new(1),
            member_nodes: vec![],
            status: OrganizationStatus::Active,
            config_version: 1,
            created_at: Utc::now(),
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
            name: "test-org".to_string(),
            shard_id: ShardId::new(1),
            member_nodes: vec![],
            status: OrganizationStatus::Active,
            config_version: 1,
            created_at: Utc::now(),
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
}
