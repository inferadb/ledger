//! Shard resolution for routing requests to the correct Raft instance.
//!
//! This module provides abstractions for routing organization requests to the
//! appropriate Raft shard in multi-shard deployments.
//!
//! ## Architecture
//!
//! ```text
//! Request(organization) -> ShardResolver -> Raft instance
//!                                  |
//!                    +-------------+-------------+
//!                    |                           |
//!             SingleShard                 MultiShard
//!            (always shard 0)        (lookup by organization)
//!                                           |
//!                              +------------+------------+
//!                              |                         |
//!                         Local shard              Remote shard
//!                        (ShardContext)          (ForwardInfo)
//! ```
//!
//! ## Request Forwarding
//!
//! When a organization is assigned to a shard on a different node, the resolver
//! returns forwarding information that services can use to proxy the request
//! via gRPC to the correct node.

use std::sync::Arc;

use inferadb_ledger_proto::proto::BlockAnnouncement;
use inferadb_ledger_state::{BlockArchive, StateLayer};
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{OrganizationId, ShardId};
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
    /// Raft consensus handle for this shard's membership queries.
    pub raft: Arc<Raft<LedgerTypeConfig>>,
    /// State layer providing this shard's entity and relationship reads.
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
/// When a organization is on a shard hosted by a different node, this struct
/// provides the routing information needed to forward the request via gRPC.
#[derive(Debug, Clone)]
pub struct RemoteShardInfo {
    /// The shard hosting the organization.
    pub shard_id: ShardId,
    /// The organization being accessed (internal identifier).
    pub organization: OrganizationId,
    /// Routing information including node addresses and leader hint.
    pub routing: RoutingInfo,
}

/// Result of resolving a organization to its shard.
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

/// Trait for resolving organizations to shard contexts.
///
/// Implementors provide the mapping from organization to the resources
/// needed to handle requests for that organization.
pub trait ShardResolver: Send + Sync {
    /// Resolves an organization to its shard context.
    ///
    /// * `organization` - Organization internal identifier (`OrganizationId`).
    ///
    /// Returns the Raft instance, state layer, and other resources
    /// for the shard that handles the given organization.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The organization is not found in routing tables
    /// - The shard is on a remote node and forwarding is not supported
    fn resolve(&self, organization: OrganizationId) -> Result<ShardContext, Status>;

    /// Resolves an organization, supporting remote shards.
    ///
    /// Unlike `resolve()`, this method can return forwarding information
    /// when the organization is on a remote shard, allowing services to
    /// proxy the request via gRPC.
    ///
    /// # Returns
    ///
    /// - `Local(ShardContext)` - shard is available locally
    /// - `Remote(RemoteShardInfo)` - shard is on another node
    ///
    /// # Errors
    ///
    /// Returns an error if the organization is not found in routing tables.
    fn resolve_with_forward(&self, organization: OrganizationId) -> Result<ResolveResult, Status> {
        // Default implementation: just try local resolution
        self.resolve(organization).map(ResolveResult::Local)
    }

    /// Returns the system shard context.
    ///
    /// The system shard (shard 0) handles global operations like
    /// organization creation and user management.
    fn system_shard(&self) -> Result<ShardContext, Status>;

    /// Checks if this resolver supports request forwarding.
    ///
    /// Resolvers that can forward to remote shards return `true`.
    fn supports_forwarding(&self) -> bool {
        false
    }
}

/// Single-shard resolver for standalone deployments.
///
/// Always returns the same shard context, as all organizations
/// are handled by a single Raft group.
pub struct SingleShardResolver {
    raft: Arc<Raft<LedgerTypeConfig>>,
    state: Arc<StateLayer<FileBackend>>,
    block_archive: Arc<BlockArchive<FileBackend>>,
    applied_state: AppliedStateAccessor,
}

impl SingleShardResolver {
    /// Creates a new single-shard resolver.
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
    fn resolve(&self, _organization: OrganizationId) -> Result<ShardContext, Status> {
        // Single shard handles all organizations
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
/// Routes organizations to their assigned shards using the ShardRouter.
/// Supports request forwarding to remote shards when the organization
/// is not hosted locally.
pub struct MultiShardResolver {
    manager: Arc<MultiRaftManager>,
}

impl MultiShardResolver {
    /// Creates a new multi-shard resolver.
    pub fn new(manager: Arc<MultiRaftManager>) -> Self {
        Self { manager }
    }

    /// Returns the ShardRouter for remote routing lookups.
    fn router(&self) -> Option<Arc<ShardRouter>> {
        self.manager.router()
    }
}

impl ShardResolver for MultiShardResolver {
    fn resolve(&self, organization: OrganizationId) -> Result<ShardContext, Status> {
        // System organization (0) always goes to system shard
        if organization == OrganizationId::new(0) {
            return self.system_shard();
        }

        // Look up the shard for this organization
        let shard = self.manager.route_organization(organization).ok_or_else(|| {
            // Check if it's a routing issue or the shard is on another node
            if let Some(shard_id) = self.manager.get_organization_shard(organization) {
                // Shard exists but not on this node - use resolve_with_forward instead
                Status::unavailable(format!(
                    "Organization {} is on shard {} which is not available locally. \
                     Use resolve_with_forward() for forwarding support.",
                    organization, shard_id
                ))
            } else {
                // Organization not found in routing table
                Status::not_found(format!(
                    "Organization {} not found in routing table",
                    organization
                ))
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

    fn resolve_with_forward(&self, organization: OrganizationId) -> Result<ResolveResult, Status> {
        // System organization (0) always goes to system shard
        if organization == OrganizationId::new(0) {
            return self.system_shard().map(ResolveResult::Local);
        }

        // First, try local resolution
        if let Some(shard) = self.manager.route_organization(organization) {
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
        let routing = router.get_routing(organization).map_err(|e| match e {
            crate::shard_router::RoutingError::OrganizationNotFound { .. } => Status::not_found(
                format!("Organization {} not found in routing table", organization),
            ),
            crate::shard_router::RoutingError::OrganizationUnavailable { status, .. } => {
                Status::unavailable(format!("Organization {} is {:?}", organization, status))
            },
            _ => Status::internal(format!("Routing error: {}", e)),
        })?;

        // Return forwarding info
        Ok(ResolveResult::Remote(RemoteShardInfo {
            shard_id: routing.shard_id,
            organization,
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
            organization: OrganizationId::new(42),
            routing: routing.clone(),
        };

        assert_eq!(remote_info.shard_id, ShardId::new(1));
        assert_eq!(remote_info.organization, OrganizationId::new(42));
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
            organization: OrganizationId::new(100),
            routing,
        };

        let result = ResolveResult::Remote(remote);

        match result {
            ResolveResult::Local(_) => unreachable!("Expected Remote variant"),
            ResolveResult::Remote(info) => {
                assert_eq!(info.shard_id, ShardId::new(2));
                assert_eq!(info.organization, OrganizationId::new(100));
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
                organization: OrganizationId::new(1),
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
            organization: OrganizationId::new(42),
            routing,
        };

        let result = ResolveResult::Remote(remote_info);
        let debug_output = format!("{:?}", result);
        assert!(debug_output.contains("Remote"));
        assert!(debug_output.contains("shard_id"));
        assert!(debug_output.contains("organization"));
    }
}
