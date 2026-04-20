//! Node registration and discovery operations.

use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{NodeId, Operation, decode, encode};
use snafu::ResultExt;
use tracing::warn;

use super::{
    CodecSnafu, Result, SYSTEM_VAULT_ID, StateSnafu, SystemKeys, SystemOrganizationService,
};
use crate::system::types::NodeInfo;

/// Maximum number of nodes returned by `list_nodes`.
const MAX_LIST_NODES: usize = 1_000;

impl<B: StorageBackend> SystemOrganizationService<B> {
    /// Registers a node in the cluster.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::Codec`] if serialization fails, or
    /// [`super::SystemError::State`] if the write operation fails.
    pub fn register_node(&self, node: &NodeInfo) -> Result<()> {
        let key = SystemKeys::node_key(&node.node_id);
        let value = encode(node).context(CodecSnafu)?;

        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];

        self.state.apply_operations_lazy(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(())
    }

    /// Returns a node by ID.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::State`] if the read fails, or
    /// [`super::SystemError::Codec`] if deserialization fails.
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
    /// Returns [`super::SystemError::State`] if the underlying list operation fails.
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
    /// Returns [`super::SystemError::State`] if the delete operation fails.
    pub fn remove_node(&self, node_id: &NodeId) -> Result<bool> {
        let key = SystemKeys::node_key(node_id);
        let ops = vec![Operation::DeleteEntity { key }];

        let statuses =
            self.state.apply_operations_lazy(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(matches!(statuses.first(), Some(inferadb_ledger_types::WriteStatus::Deleted)))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::net::SocketAddr;

    use chrono::Utc;
    use inferadb_ledger_types::{NodeId, Region};

    use crate::system::{service::SystemOrganizationService, types::NodeInfo};

    fn create_test_service() -> SystemOrganizationService<inferadb_ledger_store::InMemoryBackend> {
        crate::system::create_test_service()
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
        for (i, region) in regions.iter().enumerate() {
            let node = svc.get_node(&NodeId::new(format!("node-{}", i))).unwrap().unwrap();
            assert_eq!(node.region, *region);
        }
    }
}
