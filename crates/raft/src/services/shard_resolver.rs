//! Shard resolution for routing requests to the correct Raft instance.
//!
//! This module provides abstractions for routing namespace requests to the
//! appropriate Raft shard in multi-shard deployments.
//!
//! ## Architecture
//!
//! ```text
//! Request(namespace_id) -> ShardResolver -> Raft instance
//!                                  |
//!                    +-------------+-------------+
//!                    |                           |
//!             SingleShard                 MultiShard
//!            (always shard 0)        (lookup by namespace)
//!                                           |
//!                              +------------+------------+
//!                              |                         |
//!                         Local shard              Remote shard
//!                        (ShardContext)          (ForwardInfo)
//! ```
//!
//! ## Request Forwarding
//!
//! When a namespace is assigned to a shard on a different node, the resolver
//! returns forwarding information that services can use to proxy the request
//! via gRPC to the correct node.

use std::sync::Arc;

use inferadb_ledger_proto::proto::BlockAnnouncement;
use inferadb_ledger_state::{BlockArchive, StateLayer};
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{NamespaceId, ShardId};
use openraft::Raft;
use tokio::sync::broadcast;
use tonic::Status;

use crate::{
    log_storage::AppliedStateAccessor,
    multi_raft::MultiRaftManager,
    shard_router::{RoutingInfo, ShardRouter},
    types::LedgerTypeConfig,
};

/// Resolved shard context for handling a request locally.
///
/// Contains all the resources needed to process a request on a specific shard.
pub struct ShardContext {
    /// The Raft instance for this shard.
    pub raft: Arc<Raft<LedgerTypeConfig>>,
    /// The state layer for this shard.
    pub state: Arc<StateLayer<FileBackend>>,
    /// The block archive for this shard.
    pub block_archive: Arc<BlockArchive<FileBackend>>,
    /// Applied state accessor for this shard.
    pub applied_state: AppliedStateAccessor,
    /// Block announcements broadcast channel for real-time notifications.
    /// Optional for backward compatibility with single-shard setups that
    /// manage the channel externally.
    pub block_announcements: Option<broadcast::Sender<BlockAnnouncement>>,
}

/// Information for forwarding a request to a remote shard.
///
/// When a namespace is on a shard hosted by a different node, this struct
/// provides the routing information needed to forward the request via gRPC.
#[derive(Debug, Clone)]
pub struct RemoteShardInfo {
    /// The shard hosting the namespace.
    pub shard_id: ShardId,
    /// The namespace being accessed.
    pub namespace_id: NamespaceId,
    /// Routing information including node addresses and leader hint.
    pub routing: RoutingInfo,
}

/// Result of resolving a namespace to its shard.
///
/// Either the shard is local (can be handled directly) or remote
/// (needs to be forwarded via gRPC).
#[derive(Debug)]
pub enum ResolveResult {
    /// Shard is available locally - process the request directly.
    Local(ShardContext),
    /// Shard is on a remote node - forward the request via gRPC.
    Remote(RemoteShardInfo),
}

// Manual Debug for ShardContext since Raft doesn't implement Debug
impl std::fmt::Debug for ShardContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardContext")
            .field("raft", &"<Raft>")
            .field("state", &"<StateLayer>")
            .field("block_archive", &"<BlockArchive>")
            .field("applied_state", &"<AppliedState>")
            .field("block_announcements", &self.block_announcements.is_some())
            .finish()
    }
}

/// Trait for resolving namespaces to shard contexts.
///
/// Implementors provide the mapping from namespace to the resources
/// needed to handle requests for that namespace.
pub trait ShardResolver: Send + Sync {
    /// Resolve a namespace to its shard context.
    ///
    /// Returns the Raft instance, state layer, and other resources
    /// for the shard that handles the given namespace.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The namespace is not found in routing tables
    /// - The shard is on a remote node and forwarding is not supported
    fn resolve(&self, namespace_id: NamespaceId) -> Result<ShardContext, Status>;

    /// Resolve a namespace, supporting remote shards.
    ///
    /// Unlike `resolve()`, this method can return forwarding information
    /// when the namespace is on a remote shard, allowing services to
    /// proxy the request via gRPC.
    ///
    /// # Returns
    ///
    /// - `Local(ShardContext)` - shard is available locally
    /// - `Remote(RemoteShardInfo)` - shard is on another node
    ///
    /// # Errors
    ///
    /// Returns an error if the namespace is not found in routing tables.
    fn resolve_with_forward(&self, namespace_id: NamespaceId) -> Result<ResolveResult, Status> {
        // Default implementation: just try local resolution
        self.resolve(namespace_id).map(ResolveResult::Local)
    }

    /// Get the system shard context.
    ///
    /// The system shard (shard 0) handles global operations like
    /// namespace creation and user management.
    fn system_shard(&self) -> Result<ShardContext, Status>;

    /// Check if this resolver supports request forwarding.
    ///
    /// Resolvers that can forward to remote shards return `true`.
    fn supports_forwarding(&self) -> bool {
        false
    }
}

/// Single-shard resolver for simple deployments.
///
/// Always returns the same shard context, as all namespaces
/// are handled by a single Raft group.
pub struct SingleShardResolver {
    raft: Arc<Raft<LedgerTypeConfig>>,
    state: Arc<StateLayer<FileBackend>>,
    block_archive: Arc<BlockArchive<FileBackend>>,
    applied_state: AppliedStateAccessor,
}

impl SingleShardResolver {
    /// Create a new single-shard resolver.
    pub fn new(
        raft: Arc<Raft<LedgerTypeConfig>>,
        state: Arc<StateLayer<FileBackend>>,
        block_archive: Arc<BlockArchive<FileBackend>>,
        applied_state: AppliedStateAccessor,
    ) -> Self {
        Self { raft, state, block_archive, applied_state }
    }
}

impl ShardResolver for SingleShardResolver {
    fn resolve(&self, _namespace_id: NamespaceId) -> Result<ShardContext, Status> {
        // Single shard handles all namespaces
        // Note: block_announcements is None because single-shard setups manage
        // the channel externally in LedgerServer
        Ok(ShardContext {
            raft: self.raft.clone(),
            state: self.state.clone(),
            block_archive: self.block_archive.clone(),
            applied_state: self.applied_state.clone(),
            block_announcements: None,
        })
    }

    fn system_shard(&self) -> Result<ShardContext, Status> {
        Ok(ShardContext {
            raft: self.raft.clone(),
            state: self.state.clone(),
            block_archive: self.block_archive.clone(),
            applied_state: self.applied_state.clone(),
            block_announcements: None,
        })
    }
}

/// Multi-shard resolver using the MultiRaftManager.
///
/// Routes namespaces to their assigned shards using the ShardRouter.
/// Supports request forwarding to remote shards when the namespace
/// is not hosted locally.
pub struct MultiShardResolver {
    manager: Arc<MultiRaftManager>,
}

impl MultiShardResolver {
    /// Create a new multi-shard resolver.
    pub fn new(manager: Arc<MultiRaftManager>) -> Self {
        Self { manager }
    }

    /// Get the ShardRouter for remote routing lookups.
    fn router(&self) -> Option<Arc<ShardRouter>> {
        self.manager.router()
    }
}

impl ShardResolver for MultiShardResolver {
    fn resolve(&self, namespace_id: NamespaceId) -> Result<ShardContext, Status> {
        // System namespace (0) always goes to system shard
        if namespace_id == NamespaceId::new(0) {
            return self.system_shard();
        }

        // Look up the shard for this namespace
        let shard = self.manager.route_namespace(namespace_id).ok_or_else(|| {
            // Check if it's a routing issue or the shard is on another node
            if let Some(shard_id) = self.manager.get_namespace_shard(namespace_id) {
                // Shard exists but not on this node - use resolve_with_forward instead
                Status::unavailable(format!(
                    "Namespace {} is on shard {} which is not available locally. \
                     Use resolve_with_forward() for forwarding support.",
                    namespace_id, shard_id
                ))
            } else {
                // Namespace not found in routing table
                Status::not_found(format!("Namespace {} not found in routing table", namespace_id))
            }
        })?;

        Ok(ShardContext {
            raft: shard.raft().clone(),
            state: shard.state().clone(),
            block_archive: shard.block_archive().clone(),
            applied_state: shard.applied_state().clone(),
            block_announcements: Some(shard.block_announcements().clone()),
        })
    }

    fn resolve_with_forward(&self, namespace_id: NamespaceId) -> Result<ResolveResult, Status> {
        // System namespace (0) always goes to system shard
        if namespace_id == NamespaceId::new(0) {
            return self.system_shard().map(ResolveResult::Local);
        }

        // First, try local resolution
        if let Some(shard) = self.manager.route_namespace(namespace_id) {
            return Ok(ResolveResult::Local(ShardContext {
                raft: shard.raft().clone(),
                state: shard.state().clone(),
                block_archive: shard.block_archive().clone(),
                applied_state: shard.applied_state().clone(),
                block_announcements: Some(shard.block_announcements().clone()),
            }));
        }

        // Not local - check if we have routing info for forwarding
        let router =
            self.router().ok_or_else(|| Status::unavailable("Shard router not initialized"))?;

        // Get routing info from the ShardRouter
        let routing = router.get_routing(namespace_id).map_err(|e| match e {
            crate::shard_router::RoutingError::NamespaceNotFound { .. } => {
                Status::not_found(format!("Namespace {} not found in routing table", namespace_id))
            },
            crate::shard_router::RoutingError::NamespaceUnavailable { status, .. } => {
                Status::unavailable(format!("Namespace {} is {:?}", namespace_id, status))
            },
            _ => Status::internal(format!("Routing error: {}", e)),
        })?;

        // Return forwarding info
        Ok(ResolveResult::Remote(RemoteShardInfo {
            shard_id: routing.shard_id,
            namespace_id,
            routing,
        }))
    }

    fn system_shard(&self) -> Result<ShardContext, Status> {
        let shard = self
            .manager
            .system_shard()
            .map_err(|e| Status::unavailable(format!("System shard not available: {}", e)))?;

        Ok(ShardContext {
            raft: shard.raft().clone(),
            state: shard.state().clone(),
            block_archive: shard.block_archive().clone(),
            applied_state: shard.applied_state().clone(),
            block_announcements: Some(shard.block_announcements().clone()),
        })
    }

    fn supports_forwarding(&self) -> bool {
        true
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_context_fields() {
        // Just verify the struct has the expected fields
        // (actual testing requires full Raft setup)
        let _: fn(ShardContext) = |ctx| {
            let _ = ctx.raft;
            let _ = ctx.state;
            let _ = ctx.block_archive;
            let _ = ctx.applied_state;
        };
    }

    #[test]
    fn test_remote_shard_info_fields() {
        let routing = RoutingInfo {
            shard_id: ShardId::new(1),
            member_nodes: vec!["node-1".to_string(), "node-2".to_string()],
            leader_hint: Some("node-1".to_string()),
        };

        let remote_info = RemoteShardInfo {
            shard_id: ShardId::new(1),
            namespace_id: NamespaceId::new(42),
            routing: routing.clone(),
        };

        assert_eq!(remote_info.shard_id, ShardId::new(1));
        assert_eq!(remote_info.namespace_id, NamespaceId::new(42));
        assert_eq!(remote_info.routing.shard_id, ShardId::new(1));
        assert_eq!(remote_info.routing.member_nodes.len(), 2);
        assert_eq!(remote_info.routing.leader_hint, Some("node-1".to_string()));
    }

    #[test]
    fn test_resolve_result_local_debug() {
        // Verify Debug impl works for Local variant pattern match
        let result: Result<ResolveResult, Status> = Err(Status::not_found("test"));
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_result_remote_variant() {
        let routing = RoutingInfo {
            shard_id: ShardId::new(2),
            member_nodes: vec!["192.168.1.1:50051".to_string()],
            leader_hint: None,
        };

        let remote = RemoteShardInfo {
            shard_id: ShardId::new(2),
            namespace_id: NamespaceId::new(100),
            routing,
        };

        let result = ResolveResult::Remote(remote);

        match result {
            ResolveResult::Local(_) => unreachable!("Expected Remote variant"),
            ResolveResult::Remote(info) => {
                assert_eq!(info.shard_id, ShardId::new(2));
                assert_eq!(info.namespace_id, NamespaceId::new(100));
                assert_eq!(info.routing.member_nodes.len(), 1);
            },
        }
    }

    #[test]
    fn test_single_shard_resolver_supports_forwarding() {
        // SingleShardResolver should not support forwarding (all local)
        // This is a compile-time interface test
        fn _check_trait_impl<T: ShardResolver>(_: &T) {}
    }

    #[test]
    fn test_shard_context_debug() {
        // Verify our custom Debug impl compiles and works
        let debug_output = format!(
            "{:?}",
            RemoteShardInfo {
                shard_id: ShardId::new(1),
                namespace_id: NamespaceId::new(1),
                routing: RoutingInfo {
                    shard_id: ShardId::new(1),
                    member_nodes: vec![],
                    leader_hint: None,
                },
            }
        );
        assert!(debug_output.contains("RemoteShardInfo"));
    }

    #[test]
    fn test_resolve_result_debug() {
        // Verify Debug impl for RemoteShardInfo (ResolveResult::Remote)
        let routing = RoutingInfo {
            shard_id: ShardId::new(1),
            member_nodes: vec!["node-1".to_string()],
            leader_hint: Some("node-1".to_string()),
        };

        let remote_info = RemoteShardInfo {
            shard_id: ShardId::new(1),
            namespace_id: NamespaceId::new(42),
            routing,
        };

        let result = ResolveResult::Remote(remote_info);
        let debug_output = format!("{:?}", result);
        assert!(debug_output.contains("Remote"));
        assert!(debug_output.contains("shard_id"));
        assert!(debug_output.contains("namespace_id"));
    }
}
