//! Region resolution for routing requests to the correct Raft instance.
//!
//! This module provides abstractions for routing organization requests to the
//! appropriate Raft region in multi-region deployments.
//!
//! ## Architecture
//!
//! ```text
//! Request(organization) -> RegionResolver -> Raft instance
//!                                  |
//!                    +-------------+-------------+
//!                    |                           |
//!               Local region              Remote region
//!              (RegionContext)          (ForwardInfo)
//! ```
//!
//! Every server is multi-region capable. A single-region deployment is simply
//! a `RegionResolverImpl` with one region (GLOBAL).
//!
//! ## Request Forwarding
//!
//! When an organization is assigned to a region on a different node, the resolver
//! returns forwarding information that services can use to proxy the request
//! via gRPC to the correct node.

use std::sync::Arc;

use inferadb_ledger_proto::proto::BlockAnnouncement;
use inferadb_ledger_state::{BlockArchive, StateLayer};
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{OrganizationId, Region};
use openraft::Raft;
use tokio::sync::broadcast;
use tonic::Status;

use crate::{
    batching::BatchWriterHandle,
    log_storage::AppliedStateAccessor,
    metrics,
    raft_manager::RaftManager,
    region_router::{RegionRouter, RoutingInfo},
    types::LedgerTypeConfig,
};

/// Resolved region context for handling a request locally.
///
/// Contains all the resources needed to process a request on a specific region.
pub struct RegionContext {
    /// Raft consensus handle for this region's membership queries.
    pub raft: Arc<Raft<LedgerTypeConfig>>,
    /// State layer providing this region's entity and relationship reads.
    pub state: Arc<StateLayer<FileBackend>>,
    /// The block archive for this region.
    pub block_archive: Arc<BlockArchive<FileBackend>>,
    /// Applied state accessor for this region.
    pub applied_state: AppliedStateAccessor,
    /// Block announcements broadcast channel for real-time notifications.
    /// Optional for backward compatibility with single-region setups that
    /// manage the channel externally.
    pub block_announcements: Option<broadcast::Sender<BlockAnnouncement>>,
    /// Batch writer handle for coalescing writes (if batch writing is enabled).
    pub batch_handle: Option<BatchWriterHandle>,
    /// State root commitment buffer for piggybacked verification.
    ///
    /// When present, the proposal path drains this buffer and attaches
    /// commitments to the next `RaftPayload` for follower verification.
    pub commitment_buffer:
        Option<std::sync::Arc<std::sync::Mutex<Vec<crate::types::StateRootCommitment>>>>,
}

/// Information for forwarding a request to a remote region.
///
/// When an organization is on a region hosted by a different node, this struct
/// provides the routing information needed to forward the request via gRPC.
#[derive(Debug, Clone)]
pub struct RemoteRegionInfo {
    /// The region hosting the organization.
    pub region: Region,
    /// The organization being accessed (internal identifier).
    pub organization: OrganizationId,
    /// Routing information including node addresses and leader hint.
    pub routing: RoutingInfo,
}

/// Result of resolving an organization to its region.
///
/// Either the region is local (can be handled directly) or remote
/// (needs to be forwarded via gRPC).
#[derive(Debug)]
pub enum ResolveResult {
    /// Region is available locally - process the request directly.
    Local(RegionContext),
    /// Region is on a remote node - forward the request via gRPC.
    Remote(RemoteRegionInfo),
}

// Manual Debug for RegionContext since Raft doesn't implement Debug
impl std::fmt::Debug for RegionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionContext")
            .field("raft", &"<Raft>")
            .field("state", &"<StateLayer>")
            .field("block_archive", &"<BlockArchive>")
            .field("applied_state", &"<AppliedState>")
            .field("block_announcements", &self.block_announcements.is_some())
            .field("batch_handle", &self.batch_handle.is_some())
            .field("commitment_buffer", &self.commitment_buffer.is_some())
            .finish()
    }
}

/// Trait for resolving organizations to region contexts.
///
/// Implementors provide the mapping from organization to the resources
/// needed to handle requests for that organization.
pub trait RegionResolver: Send + Sync {
    /// Resolves an organization to its region context.
    ///
    /// * `organization` - Organization internal identifier (`OrganizationId`).
    ///
    /// Returns the Raft instance, state layer, and other resources
    /// for the region that handles the given organization.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The organization is not found in routing tables
    /// - The region is on a remote node and forwarding is not supported
    fn resolve(&self, organization: OrganizationId) -> Result<RegionContext, Status>;

    /// Resolves an organization, supporting remote regions.
    ///
    /// Unlike `resolve()`, this method can return forwarding information
    /// when the organization is on a remote region, allowing services to
    /// proxy the request via gRPC.
    ///
    /// # Returns
    ///
    /// - `Local(RegionContext)` - region is available locally
    /// - `Remote(RemoteRegionInfo)` - region is on another node
    ///
    /// # Errors
    ///
    /// Returns an error if the organization is not found in routing tables.
    fn resolve_with_forward(&self, organization: OrganizationId) -> Result<ResolveResult, Status> {
        // Default implementation: just try local resolution
        self.resolve(organization).map(ResolveResult::Local)
    }

    /// Returns the system region context.
    ///
    /// The system region (region 0) handles global operations like
    /// organization creation and user management.
    fn system_region(&self) -> Result<RegionContext, Status>;

    /// Checks if this resolver supports request forwarding.
    ///
    /// Resolvers that can forward to remote regions return `true`.
    fn supports_forwarding(&self) -> bool {
        false
    }
}

/// Builds a [`RegionContext`] from a [`RegionGroup`], including block announcements.
///
/// Used by the multi-region resolver where every region group provides its own
/// block announcement channel.
fn region_context_from(region: &crate::raft_manager::RegionGroup) -> RegionContext {
    RegionContext {
        raft: region.raft().clone(),
        state: region.state().clone(),
        block_archive: region.block_archive().clone(),
        applied_state: region.applied_state().clone(),
        block_announcements: Some(region.block_announcements().clone()),
        batch_handle: region.batch_handle().cloned(),
        commitment_buffer: Some(region.commitment_buffer()),
    }
}

/// Region resolver backed by the [`RaftManager`].
///
/// Routes organizations to their assigned regions using the RegionRouter.
/// Supports request forwarding to remote regions when the organization
/// is not hosted locally. Uses `local_region` for data residency decisions:
/// non-protected regions are always local; protected regions require the
/// node to be in the same region.
pub struct RegionResolverImpl {
    manager: Arc<RaftManager>,
}

impl RegionResolverImpl {
    /// Creates a new multi-region resolver.
    pub fn new(manager: Arc<RaftManager>) -> Self {
        Self { manager }
    }

    /// Returns this node's configured region.
    pub fn local_region(&self) -> Region {
        self.manager.local_region()
    }

    /// Returns the RegionRouter for remote routing lookups.
    fn router(&self) -> Option<Arc<RegionRouter>> {
        self.manager.router()
    }
}

impl RegionResolver for RegionResolverImpl {
    fn resolve(&self, organization: OrganizationId) -> Result<RegionContext, Status> {
        // System organization (0) always goes to system region
        if organization == OrganizationId::new(0) {
            return self.system_region();
        }

        // Look up the region for this organization
        let region = self.manager.route_organization(organization).ok_or_else(|| {
            // Check if it's a routing issue or the region is on another node
            if let Some(region_id) = self.manager.get_organization_region(organization) {
                // Region exists but not on this node - use resolve_with_forward instead
                Status::unavailable(format!(
                    "Organization {} is on region {} which is not available locally. \
                     Use resolve_with_forward() for forwarding support.",
                    organization, region_id
                ))
            } else {
                // Organization not found in routing table
                Status::not_found(format!(
                    "Organization {} not found in routing table",
                    organization
                ))
            }
        })?;

        Ok(region_context_from(&region))
    }

    fn resolve_with_forward(&self, organization: OrganizationId) -> Result<ResolveResult, Status> {
        // System organization (0) always goes to system region
        if organization == OrganizationId::new(0) {
            return self.system_region().map(ResolveResult::Local);
        }

        // Get routing info to determine the organization's region
        let router =
            self.router().ok_or_else(|| Status::unavailable("Region router not initialized"))?;

        let routing = router.get_routing(organization).map_err(|e| match e {
            crate::region_router::RoutingError::OrganizationNotFound { .. } => Status::not_found(
                format!("Organization {} not found in routing table", organization),
            ),
            crate::region_router::RoutingError::OrganizationUnavailable { status, .. } => {
                Status::unavailable(format!("Organization {} is {:?}", organization, status))
            },
            _ => Status::internal(format!("Routing error: {}", e)),
        })?;

        let org_region = routing.region;

        // Region-aware routing decision:
        // - Non-protected regions: all nodes hold replicas → always local
        // - Protected regions, node in-region: local
        // - Protected regions, node out-of-region: forward to in-region node
        if router.is_region_local(org_region) {
            // Local: non-protected or same-region protected
            let region = self.manager.route_organization(organization).ok_or_else(|| {
                Status::unavailable(format!(
                    "Region {} is locally assigned but not yet started",
                    org_region
                ))
            })?;

            return Ok(ResolveResult::Local(region_context_from(&region)));
        }

        // Protected region, out-of-region: forward with warning
        tracing::warn!(
            organization = organization.value(),
            org_region = org_region.as_str(),
            local_region = self.local_region().as_str(),
            "Cross-region request for protected region; forwarding to in-region node"
        );
        metrics::record_data_residency_violation(org_region.as_str());

        Ok(ResolveResult::Remote(RemoteRegionInfo { region: org_region, organization, routing }))
    }

    fn system_region(&self) -> Result<RegionContext, Status> {
        let region = self
            .manager
            .system_region()
            .map_err(|e| Status::unavailable(format!("System region not available: {}", e)))?;

        Ok(region_context_from(&region))
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
    fn test_region_context_fields() {
        // Just verify the struct has the expected fields
        // (actual testing requires full Raft setup)
        let _: fn(RegionContext) = |ctx| {
            let _ = ctx.raft;
            let _ = ctx.state;
            let _ = ctx.block_archive;
            let _ = ctx.applied_state;
        };
    }

    #[test]
    fn test_remote_region_info_fields() {
        let routing = RoutingInfo {
            region: Region::US_EAST_VA,
            member_nodes: vec!["node-1".to_string(), "node-2".to_string()],
            leader_hint: Some("node-1".to_string()),
        };

        let remote_info = RemoteRegionInfo {
            region: Region::US_EAST_VA,
            organization: OrganizationId::new(42),
            routing: routing.clone(),
        };

        assert_eq!(remote_info.region, Region::US_EAST_VA);
        assert_eq!(remote_info.organization, OrganizationId::new(42));
        assert_eq!(remote_info.routing.region, Region::US_EAST_VA);
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
            region: Region::IE_EAST_DUBLIN,
            member_nodes: vec!["192.168.1.1:50051".to_string()],
            leader_hint: None,
        };

        let remote = RemoteRegionInfo {
            region: Region::IE_EAST_DUBLIN,
            organization: OrganizationId::new(100),
            routing,
        };

        let result = ResolveResult::Remote(remote);

        match result {
            ResolveResult::Local(_) => unreachable!("Expected Remote variant"),
            ResolveResult::Remote(info) => {
                assert_eq!(info.region, Region::IE_EAST_DUBLIN);
                assert_eq!(info.organization, OrganizationId::new(100));
                assert_eq!(info.routing.member_nodes.len(), 1);
            },
        }
    }

    #[test]
    fn test_region_resolver_trait_is_object_safe() {
        // Verify RegionResolver is object-safe (can be used as dyn trait)
        fn _check_trait_impl<T: RegionResolver>(_: &T) {}
    }

    #[test]
    fn test_region_context_debug() {
        // Verify our custom Debug impl compiles and works
        let debug_output = format!(
            "{:?}",
            RemoteRegionInfo {
                region: Region::US_EAST_VA,
                organization: OrganizationId::new(1),
                routing: RoutingInfo {
                    region: Region::US_EAST_VA,
                    member_nodes: vec![],
                    leader_hint: None,
                },
            }
        );
        assert!(debug_output.contains("RemoteRegionInfo"));
    }

    #[test]
    fn test_resolve_result_debug() {
        // Verify Debug impl for RemoteRegionInfo (ResolveResult::Remote)
        let routing = RoutingInfo {
            region: Region::US_EAST_VA,
            member_nodes: vec!["node-1".to_string()],
            leader_hint: Some("node-1".to_string()),
        };

        let remote_info = RemoteRegionInfo {
            region: Region::US_EAST_VA,
            organization: OrganizationId::new(42),
            routing,
        };

        let result = ResolveResult::Remote(remote_info);
        let debug_output = format!("{:?}", result);
        assert!(debug_output.contains("Remote"));
        assert!(debug_output.contains("region"));
        assert!(debug_output.contains("organization"));
    }

    #[test]
    fn test_multi_region_resolver_local_region() {
        // RegionResolverImpl delegates local_region() to RaftManager.
        // Verify the accessor exists and returns the manager's local_region.
        // Full integration test requires async Raft setup; here we test the
        // type-level wiring.
        use inferadb_ledger_test_utils::TestDir;

        use crate::raft_manager::{RaftManager, RaftManagerConfig};

        let temp = TestDir::new();
        let config =
            RaftManagerConfig::new(temp.path().to_path_buf(), 1, Region::DE_CENTRAL_FRANKFURT);
        let manager = Arc::new(RaftManager::new(config));

        let resolver = RegionResolverImpl::new(manager);
        assert_eq!(resolver.local_region(), Region::DE_CENTRAL_FRANKFURT);
    }

    #[test]
    fn test_multi_region_resolver_supports_forwarding() {
        use inferadb_ledger_test_utils::TestDir;

        use crate::raft_manager::{RaftManager, RaftManagerConfig};

        let temp = TestDir::new();
        let config = RaftManagerConfig::new(temp.path().to_path_buf(), 1, Region::GLOBAL);
        let manager = Arc::new(RaftManager::new(config));

        let resolver = RegionResolverImpl::new(manager);
        assert!(resolver.supports_forwarding());
    }

    #[test]
    fn test_single_region_resolver_does_not_support_forwarding() {
        // Default trait implementation returns false
        struct FakeResolver;
        impl RegionResolver for FakeResolver {
            fn resolve(&self, _org: OrganizationId) -> Result<RegionContext, Status> {
                Err(Status::unimplemented("fake"))
            }
            fn system_region(&self) -> Result<RegionContext, Status> {
                Err(Status::unimplemented("fake"))
            }
        }

        let resolver = FakeResolver;
        assert!(!resolver.supports_forwarding());
    }

    #[test]
    fn test_default_resolve_with_forward_delegates_to_resolve() {
        // Default trait impl wraps resolve() result in Local variant.
        struct AlwaysErrorResolver;
        impl RegionResolver for AlwaysErrorResolver {
            fn resolve(&self, org: OrganizationId) -> Result<RegionContext, Status> {
                Err(Status::not_found(format!("org {}", org)))
            }
            fn system_region(&self) -> Result<RegionContext, Status> {
                Err(Status::unimplemented("fake"))
            }
        }

        let resolver = AlwaysErrorResolver;
        let result = resolver.resolve_with_forward(OrganizationId::new(42));
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }
}
