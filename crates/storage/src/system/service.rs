//! Service layer for `_system` namespace operations.
//!
//! Provides high-level operations on the _system namespace:
//! - Node registration and discovery
//! - Namespace routing table management
//! - Sequence counter management for ID generation
//!
//! The _system namespace uses namespace_id = 0 and vault_id = 0.

use std::sync::Arc;

use inkwell::StorageBackend;
use snafu::{ResultExt, Snafu};

use ledger_types::{NamespaceId, NodeId, Operation, ShardId, VaultId};

use crate::state::{StateError, StateLayer};

use super::keys::SystemKeys;
use super::types::{NamespaceRegistry, NamespaceStatus, NodeInfo};

/// The reserved namespace ID for _system.
pub const SYSTEM_NAMESPACE_ID: NamespaceId = 0;

/// The reserved vault ID for _system entities.
pub const SYSTEM_VAULT_ID: VaultId = 0;

/// Errors from system namespace operations.
#[derive(Debug, Snafu)]
pub enum SystemError {
    /// Underlying state layer operation failed.
    #[snafu(display("State layer error: {source}"))]
    State {
        /// The underlying state layer error.
        #[snafu(source(from(StateError, Box::new)))]
        source: Box<StateError>,
    },

    /// Serialization or deserialization failed.
    #[snafu(display("Serialization error: {message}"))]
    Serialization {
        /// Description of the serialization failure.
        message: String,
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

pub type Result<T> = std::result::Result<T, SystemError>;

/// Service for reading from and writing to the `_system` namespace.
///
/// All _system data is stored in namespace_id=0, vault_id=0.
/// StateLayer is internally thread-safe via inkwell's MVCC.
pub struct SystemNamespaceService<B: StorageBackend> {
    state: Arc<StateLayer<B>>,
}

impl<B: StorageBackend> SystemNamespaceService<B> {
    /// Create a new system namespace service.
    pub fn new(state: Arc<StateLayer<B>>) -> Self {
        Self { state }
    }

    // =========================================================================
    // Sequence Counters
    // =========================================================================

    /// Get the next value from a sequence counter and increment it.
    ///
    /// If the counter doesn't exist, initializes it to `start_value`.
    pub fn next_sequence(&self, key: &str, start_value: i64) -> Result<i64> {
        // StateLayer is internally thread-safe via inkwell MVCC
        // Read current value
        let current = match self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()) {
            Ok(Some(entity)) => {
                let value_str = String::from_utf8_lossy(&entity.value);
                value_str.parse::<i64>().unwrap_or(start_value)
            }
            Ok(None) => start_value,
            Err(e) => {
                return Err(SystemError::State {
                    source: Box::new(e),
                });
            }
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

    /// Get the next namespace ID.
    pub fn next_namespace_id(&self) -> Result<NamespaceId> {
        // Start at 1 because 0 is reserved for _system
        self.next_sequence(SystemKeys::NAMESPACE_SEQ_KEY, 1)
    }

    /// Get the next vault ID.
    pub fn next_vault_id(&self) -> Result<VaultId> {
        self.next_sequence(SystemKeys::VAULT_SEQ_KEY, 1)
    }

    // =========================================================================
    // Node Operations
    // =========================================================================

    /// Register a node in the cluster.
    pub fn register_node(&self, node: &NodeInfo) -> Result<()> {
        let key = SystemKeys::node_key(&node.node_id);
        let value = postcard::to_allocvec(node).map_err(|e| SystemError::Serialization {
            message: e.to_string(),
        })?;

        let ops = vec![Operation::SetEntity {
            key,
            value,
            condition: None,
            expires_at: None,
        }];

        self.state
            .apply_operations(SYSTEM_VAULT_ID, &ops, 0)
            .context(StateSnafu)?;

        Ok(())
    }

    /// Get a node by ID.
    pub fn get_node(&self, node_id: &NodeId) -> Result<Option<NodeInfo>> {
        let key = SystemKeys::node_key(node_id);

        match self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()) {
            Ok(Some(entity)) => {
                let node: NodeInfo = postcard::from_bytes(&entity.value).map_err(|e| {
                    SystemError::Serialization {
                        message: e.to_string(),
                    }
                })?;
                Ok(Some(node))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(SystemError::State {
                source: Box::new(e),
            }),
        }
    }

    /// List all registered nodes.
    pub fn list_nodes(&self) -> Result<Vec<NodeInfo>> {
        let entities = self
            .state
            .list_entities(SYSTEM_VAULT_ID, Some(SystemKeys::NODE_PREFIX), None, 1000)
            .context(StateSnafu)?;

        let mut nodes = Vec::new();
        for entity in entities {
            if let Ok(node) = postcard::from_bytes::<NodeInfo>(&entity.value) {
                nodes.push(node);
            }
        }

        Ok(nodes)
    }

    /// Remove a node from the cluster.
    pub fn remove_node(&self, node_id: &NodeId) -> Result<bool> {
        let key = SystemKeys::node_key(node_id);
        let ops = vec![Operation::DeleteEntity { key }];

        let statuses = self
            .state
            .apply_operations(SYSTEM_VAULT_ID, &ops, 0)
            .context(StateSnafu)?;

        Ok(matches!(
            statuses.first(),
            Some(ledger_types::WriteStatus::Deleted)
        ))
    }

    // =========================================================================
    // Namespace Registry Operations
    // =========================================================================

    /// Register a new namespace.
    pub fn register_namespace(&self, registry: &NamespaceRegistry) -> Result<()> {
        let key = SystemKeys::namespace_key(registry.namespace_id);
        let value = postcard::to_allocvec(registry).map_err(|e| SystemError::Serialization {
            message: e.to_string(),
        })?;

        // Also create the name index
        let name_index_key = SystemKeys::namespace_name_index_key(&registry.name);
        let name_index_value = registry.namespace_id.to_string().into_bytes();

        let ops = vec![
            Operation::SetEntity {
                key,
                value,
                condition: None,
                expires_at: None,
            },
            Operation::SetEntity {
                key: name_index_key,
                value: name_index_value,
                condition: None,
                expires_at: None,
            },
        ];

        self.state
            .apply_operations(SYSTEM_VAULT_ID, &ops, 0)
            .context(StateSnafu)?;

        Ok(())
    }

    /// Get a namespace by ID.
    pub fn get_namespace(&self, namespace_id: NamespaceId) -> Result<Option<NamespaceRegistry>> {
        let key = SystemKeys::namespace_key(namespace_id);

        match self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()) {
            Ok(Some(entity)) => {
                let registry: NamespaceRegistry =
                    postcard::from_bytes(&entity.value).map_err(|e| {
                        SystemError::Serialization {
                            message: e.to_string(),
                        }
                    })?;
                Ok(Some(registry))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(SystemError::State {
                source: Box::new(e),
            }),
        }
    }

    /// Get a namespace by name.
    pub fn get_namespace_by_name(&self, name: &str) -> Result<Option<NamespaceRegistry>> {
        let index_key = SystemKeys::namespace_name_index_key(name);

        // First, look up the namespace ID from the index
        let namespace_id = match self.state.get_entity(SYSTEM_VAULT_ID, index_key.as_bytes()) {
            Ok(Some(entity)) => {
                let id_str = String::from_utf8_lossy(&entity.value);
                id_str.parse::<NamespaceId>().ok()
            }
            Ok(None) => None,
            Err(e) => {
                return Err(SystemError::State {
                    source: Box::new(e),
                });
            }
        };

        match namespace_id {
            Some(id) => self.get_namespace(id),
            None => Ok(None),
        }
    }

    /// List all namespaces.
    pub fn list_namespaces(&self) -> Result<Vec<NamespaceRegistry>> {
        let entities = self
            .state
            .list_entities(
                SYSTEM_VAULT_ID,
                Some(SystemKeys::NAMESPACE_PREFIX),
                None,
                10000,
            )
            .context(StateSnafu)?;

        let mut namespaces = Vec::new();
        for entity in entities {
            if let Ok(registry) = postcard::from_bytes::<NamespaceRegistry>(&entity.value) {
                namespaces.push(registry);
            }
        }

        Ok(namespaces)
    }

    /// Update namespace status.
    pub fn update_namespace_status(
        &self,
        namespace_id: NamespaceId,
        status: NamespaceStatus,
    ) -> Result<()> {
        // Get existing registry
        let mut registry =
            self.get_namespace(namespace_id)?
                .ok_or_else(|| SystemError::NotFound {
                    entity: format!("namespace:{}", namespace_id),
                })?;

        // Update status
        registry.status = status;
        registry.config_version += 1;

        // Save
        self.register_namespace(&registry)
    }

    // =========================================================================
    // Shard Routing
    // =========================================================================

    /// Get the shard ID for a namespace.
    pub fn get_shard_for_namespace(&self, namespace_id: NamespaceId) -> Result<Option<ShardId>> {
        self.get_namespace(namespace_id)
            .map(|opt| opt.map(|r| r.shard_id))
    }

    /// Assign a namespace to a shard.
    pub fn assign_namespace_to_shard(
        &self,
        namespace_id: NamespaceId,
        shard_id: ShardId,
        member_nodes: Vec<NodeId>,
    ) -> Result<()> {
        let mut registry =
            self.get_namespace(namespace_id)?
                .ok_or_else(|| SystemError::NotFound {
                    entity: format!("namespace:{}", namespace_id),
                })?;

        registry.shard_id = shard_id;
        registry.member_nodes = member_nodes;
        registry.config_version += 1;

        self.register_namespace(&registry)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::net::SocketAddr;

    use chrono::Utc;

    use crate::engine::InMemoryStorageEngine;

    use super::super::types::NodeRole;
    use super::*;

    fn create_test_service() -> SystemNamespaceService<inkwell::InMemoryBackend> {
        let engine = InMemoryStorageEngine::open().unwrap();
        let state = Arc::new(StateLayer::new(engine.db()));
        SystemNamespaceService::new(state)
    }

    #[test]
    fn test_next_namespace_id() {
        let svc = create_test_service();

        let id1 = svc.next_namespace_id().unwrap();
        let id2 = svc.next_namespace_id().unwrap();
        let id3 = svc.next_namespace_id().unwrap();

        assert_eq!(id1, 1); // Starts at 1, 0 is reserved
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
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
    fn test_register_and_get_namespace() {
        let svc = create_test_service();

        let registry = NamespaceRegistry {
            namespace_id: 1,
            name: "acme-corp".to_string(),
            shard_id: 1,
            member_nodes: vec!["node-1".to_string(), "node-2".to_string()],
            status: NamespaceStatus::Active,
            config_version: 1,
            created_at: Utc::now(),
        };

        svc.register_namespace(&registry).unwrap();

        // Get by ID
        let retrieved = svc.get_namespace(1).unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, "acme-corp");

        // Get by name
        let by_name = svc.get_namespace_by_name("acme-corp").unwrap();
        assert!(by_name.is_some());
        assert_eq!(by_name.unwrap().namespace_id, 1);

        // Case-insensitive name lookup
        let by_name_upper = svc.get_namespace_by_name("ACME-CORP").unwrap();
        assert!(by_name_upper.is_some());
    }

    #[test]
    fn test_list_namespaces() {
        let svc = create_test_service();

        for i in 1..=3 {
            let registry = NamespaceRegistry {
                namespace_id: i,
                name: format!("ns-{}", i),
                shard_id: 1,
                member_nodes: vec![],
                status: NamespaceStatus::Active,
                config_version: 1,
                created_at: Utc::now(),
            };
            svc.register_namespace(&registry).unwrap();
        }

        let namespaces = svc.list_namespaces().unwrap();
        assert_eq!(namespaces.len(), 3);
    }

    #[test]
    fn test_update_namespace_status() {
        let svc = create_test_service();

        let registry = NamespaceRegistry {
            namespace_id: 1,
            name: "test-ns".to_string(),
            shard_id: 1,
            member_nodes: vec![],
            status: NamespaceStatus::Active,
            config_version: 1,
            created_at: Utc::now(),
        };
        svc.register_namespace(&registry).unwrap();

        svc.update_namespace_status(1, NamespaceStatus::Suspended)
            .unwrap();

        let updated = svc.get_namespace(1).unwrap().unwrap();
        assert_eq!(updated.status, NamespaceStatus::Suspended);
        assert_eq!(updated.config_version, 2); // Incremented
    }
}
