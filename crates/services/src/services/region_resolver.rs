//! Region resolution for routing requests to the correct Raft instance.
//!
//! Abstractions for routing organization requests to the
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
//!              (RegionContext)           (RedirectInfo)
//! ```
//!
//! Every server is multi-region capable. A single-region deployment is simply
//! a `RegionResolverService` with one region (GLOBAL).
//!
//! ## Cross-region redirects
//!
//! When an organization is assigned to a region hosted on a different node,
//! the resolver returns [`RedirectInfo`]. Services translate this into a
//! `NotLeader` `Status` so the SDK reconnects directly to an in-region node
//! — the server never proxies client requests across regions.

use std::sync::Arc;

use inferadb_ledger_proto::proto::BlockAnnouncement;
use inferadb_ledger_raft::{
    ConsensusHandle, LeaderLease,
    batching::BatchWriterHandle,
    log_storage::{AppliedStateAccessor, IdempotencyCheckResult},
    metrics,
    raft_manager::RaftManager,
};
use inferadb_ledger_state::{BlockArchive, StateLayer, system::OrganizationStatus};
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{OrganizationId, Region, VaultId};
use tokio::sync::{broadcast, watch};
use tonic::Status;

use super::error_classify;

/// Routing information for an organization.
///
/// Identifies the region hosting the organization. Used in [`RedirectInfo`]
/// to surface the addressing information SDK clients need to retry against
/// the correct node.
#[derive(Debug, Clone)]
pub struct RoutingInfo {
    /// Region hosting this organization.
    pub region: Region,
    /// Hint for current leader (may be stale).
    ///
    /// Always `None` for cross-region redirects: this node has no
    /// authoritative view of the remote region's leadership. Clients that
    /// receive a `NotLeader` carrying this routing info must fall back to
    /// `ResolveRegionLeader` / `WatchLeader` on an in-region node to learn
    /// the current leader.
    pub leader_hint: Option<String>,
}

/// Resolved region context for handling a request locally.
///
/// Contains all the resources needed to process a request on a specific region.
pub struct RegionContext {
    /// The region this context is bound to.
    pub region: Region,
    /// Consensus handle for this region's leadership and proposal operations.
    pub handle: Arc<ConsensusHandle>,
    /// State layer providing this region's entity and relationship reads.
    pub state: Arc<StateLayer<FileBackend>>,
    /// The block archive for this region.
    pub block_archive: Arc<BlockArchive<FileBackend>>,
    /// Per-organization applied-state accessor for this region.
    ///
    /// Carries org-scoped indices (slug indices, organizations, vault
    /// metadata, sequence counters, etc.) populated by the parent
    /// `OrganizationGroup`'s apply pipeline. Per-vault apply progress
    /// (`vault_heights`, `client_sequences`, etc.) lives on
    /// [`vault_applied_state`](Self::vault_applied_state) instead and is
    /// the authoritative source for vault-scoped reads when present.
    pub applied_state: AppliedStateAccessor,
    /// Per-vault applied-state accessor when the request resolves to a
    /// specific vault and a per-vault Raft group is live on this node.
    ///
    /// Populated by `ReadService::resolve_org_vault` and equivalent helpers
    /// when [`RaftManager::get_vault_group`] returns a live vault group for
    /// the resolved `(region, org, vault)` triple.
    /// The accessor is backed by the per-vault
    /// [`RaftLogStore`](inferadb_ledger_raft::log_storage::RaftLogStore)'s
    /// own `applied_state` `ArcSwap`, so reads see the per-vault apply
    /// pipeline's writes (vault height, client sequences, idempotency
    /// entries) without going through the org-scoped accessor that
    /// never observes per-vault writes under the multi-Raft model.
    ///
    /// `None` when the request is org-scoped (no `vault_id` resolved) or
    /// when the vault group is not yet live on this node (e.g. before
    /// `start_vault_group` runs after `CreateVault`); callers fall back
    /// to [`applied_state`](Self::applied_state) for org-scoped reads.
    /// See [`vault_height`](Self::vault_height) and
    /// [`client_sequence`](Self::client_sequence) for the routing
    /// helpers that prefer the per-vault accessor when present.
    pub vault_applied_state: Option<AppliedStateAccessor>,
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
    pub commitment_buffer: Option<
        std::sync::Arc<std::sync::Mutex<Vec<inferadb_ledger_raft::types::StateRootCommitment>>>,
    >,
    /// Leader lease for fast linearizable reads on the leader.
    ///
    /// When the lease is valid, the leader can serve linearizable reads
    /// without a quorum round-trip (~50ns check).
    pub leader_lease: Option<Arc<LeaderLease>>,
    /// Watch channel receiver for applied index (ReadIndex protocol).
    ///
    /// Followers use this to wait until their local applied index catches up
    /// to the leader's committed index during linearizable reads.
    pub applied_index_rx: Option<watch::Receiver<u64>>,
    /// Per-vault applied-index watch when the request resolves to a
    /// specific vault and a per-vault Raft group is live on this node.
    ///
    /// Populated by `ReadService::resolve_org_vault` and equivalent helpers
    /// when [`RaftManager::get_vault_group`] returns a live vault group for
    /// the resolved `(region, org, vault)` triple. The watch is the
    /// per-vault Raft group's own applied-index channel, driven by the
    /// per-vault apply pipeline. Per-vault apply advances this watch
    /// independently of the parent organization group's `applied_index_rx`,
    /// which never observes per-vault entity writes under the multi-Raft
    /// model.
    ///
    /// `None` when the request is org-scoped (no `vault_id` resolved) or
    /// when the vault group is not yet live on this node (e.g. before
    /// `start_vault_group` runs after `CreateVault`). When present,
    /// follower-side LINEARIZABLE reads on per-vault entity data wait on
    /// this watch instead of [`applied_index_rx`](Self::applied_index_rx)
    /// so the wait targets the same Raft group whose committed index the
    /// leader returned for the vault scope. The selection logic lives in
    /// `ReadService::follower_read_index`.
    pub vault_applied_index_rx: Option<watch::Receiver<u64>>,
}

/// Information for redirecting a request to a remote region.
///
/// When an organization is on a region hosted by a different node, this struct
/// provides the routing information services use to build a `NotLeader` hint
/// so the SDK reconnects directly to an in-region node.
#[derive(Debug, Clone)]
pub struct RedirectInfo {
    /// The region hosting the organization.
    pub region: Region,
    /// The organization being accessed (internal identifier).
    pub organization: OrganizationId,
    /// Routing information including the target region and leader hint.
    pub routing: RoutingInfo,
}

/// Result of resolving an organization to its region.
///
/// Either the region is local (can be handled directly) or remote
/// (the SDK must reconnect to an in-region node).
#[derive(Debug)]
pub enum ResolveResult {
    /// Region is available locally. Process the request directly.
    Local(RegionContext),
    /// Region is on a remote node. Return `NotLeader` + hint; SDK reconnects directly.
    Redirect(RedirectInfo),
}

impl RegionContext {
    /// Attaches the per-vault accessors from a live `VaultGroup`. Must be
    /// called whenever the request resolves to a specific
    /// `(organization, vault)` triple and a vault group is live for that
    /// triple on this node.
    ///
    /// Pairs the per-vault `applied_state` accessor (used by read paths to
    /// see vault-scoped writes — `vault_heights`, `client_sequences`,
    /// `vault_health`, idempotency entries) with the per-vault
    /// `applied_index_rx` (used by follower-side LINEARIZABLE reads to
    /// wait on the correct apply pointer). The two are inseparable: a
    /// follower that has one but not the other can silently violate
    /// consistency — see `ReadService::follower_read_index` for the
    /// watch-selection rule that depends on this pairing.
    ///
    /// Does **not** swap `block_archive`. That's only needed by read
    /// paths (`ReadService`), not write paths, so callers that need it
    /// continue to do the swap explicitly at the call site to keep the
    /// helper minimal and out of the write-side hot path.
    pub fn attach_vault_group(
        &mut self,
        vault_group: &inferadb_ledger_raft::raft_manager::VaultGroup,
    ) {
        self.vault_applied_state = Some(vault_group.applied_state().clone());
        self.vault_applied_index_rx = Some(vault_group.applied_index_watch());
    }

    /// Returns the current block height for `(organization, vault)`,
    /// preferring the per-vault accessor when one is attached.
    ///
    /// Per-vault Raft groups own their own `AppliedState`; the parent
    /// org-scoped accessor never observes vault-scoped writes
    /// (`vault_heights`, `client_sequences`, etc.). When
    /// [`vault_applied_state`](Self::vault_applied_state) is populated
    /// (i.e. a vault group is live for this triple), reads route there;
    /// otherwise the org-scoped fallback is used so org-only request
    /// paths and pre-vault-group-live reads still produce a value.
    pub fn vault_height(&self, organization: OrganizationId, vault: VaultId) -> u64 {
        if let Some(vault_state) = self.vault_applied_state.as_ref() {
            return vault_state.vault_height(organization, vault);
        }
        self.applied_state.vault_height(organization, vault)
    }

    /// Returns the health status for `(organization, vault)`, merging the
    /// per-vault and org-scoped accessors.
    ///
    /// Both accessors observe writes via different paths under the
    /// multi-Raft model:
    ///
    /// - Per-vault apply (Write / BatchWrite) writes `vault_health` into the per-vault Raft group's
    ///   `AppliedState`. The vault marks itself `Diverged` when its own apply detects a state-root
    ///   mismatch.
    /// - Admin-path operations (`SimulateDivergence`, `RecoverVault`) write through the parent
    ///   org's `OrganizationGroup` and the GLOBAL system group, not through the per-vault group, so
    ///   the per-vault accessor never observes those updates.
    ///
    /// `Diverged` is the conservative status — once any source declares
    /// the vault diverged, reads must fail until recovery completes.
    /// This helper returns the first non-`Healthy` status it finds across
    /// the two accessors so divergence remains visible regardless of
    /// which Raft group recorded it.
    pub fn vault_health(
        &self,
        organization: OrganizationId,
        vault: VaultId,
    ) -> inferadb_ledger_raft::log_storage::VaultHealthStatus {
        use inferadb_ledger_raft::log_storage::VaultHealthStatus;
        if let Some(vault_state) = self.vault_applied_state.as_ref() {
            let per_vault = vault_state.vault_health(organization, vault);
            if !matches!(per_vault, VaultHealthStatus::Healthy) {
                return per_vault;
            }
        }
        self.applied_state.vault_health(organization, vault)
    }

    /// Returns the last committed sequence for a `(organization, vault, client_id)`
    /// triple, preferring the per-vault accessor when one is attached.
    pub fn client_sequence(
        &self,
        organization: OrganizationId,
        vault: VaultId,
        client_id: &str,
    ) -> u64 {
        if let Some(vault_state) = self.vault_applied_state.as_ref() {
            return vault_state.client_sequence(organization, vault, client_id);
        }
        self.applied_state.client_sequence(organization, vault, client_id)
    }

    /// Cross-failover idempotency check, preferring the per-vault
    /// accessor when one is attached.
    ///
    /// Per-vault apply records `ClientSequenceEntry` in the per-vault
    /// `AppliedState`; the org-scoped accessor never sees those writes
    /// under the multi-Raft model.
    pub fn client_idempotency_check(
        &self,
        organization: OrganizationId,
        vault: VaultId,
        client_id: &str,
        idempotency_key: &[u8; 16],
        request_hash: u64,
    ) -> IdempotencyCheckResult {
        if let Some(vault_state) = self.vault_applied_state.as_ref() {
            return vault_state.client_idempotency_check(
                organization,
                vault,
                client_id,
                idempotency_key,
                request_hash,
            );
        }
        self.applied_state.client_idempotency_check(
            organization,
            vault,
            client_id,
            idempotency_key,
            request_hash,
        )
    }
}

impl std::fmt::Debug for RegionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionContext")
            .field("region", &self.region)
            .field("handle", &"<ConsensusHandle>")
            .field("state", &"<StateLayer>")
            .field("block_archive", &"<BlockArchive>")
            .field("applied_state", &"<AppliedState>")
            .field("block_announcements", &self.block_announcements.is_some())
            .field("batch_handle", &self.batch_handle.is_some())
            .field("commitment_buffer", &self.commitment_buffer.is_some())
            .field("leader_lease", &self.leader_lease.is_some())
            .field("applied_index_rx", &self.applied_index_rx.is_some())
            .field("vault_applied_index_rx", &self.vault_applied_index_rx.is_some())
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
    /// - The region is on a remote node and cross-region redirects are not supported
    fn resolve(&self, organization: OrganizationId) -> Result<RegionContext, Status>;

    /// Resolves an organization, supporting cross-region redirects.
    ///
    /// Unlike `resolve()`, this method can return redirect information
    /// when the organization is on a remote region, allowing services to
    /// return a `NotLeader` hint so the SDK reconnects directly.
    ///
    /// # Returns
    ///
    /// - `Local(RegionContext)` - region is available locally
    /// - `Redirect(RedirectInfo)` - region is on another node; SDK must reconnect
    ///
    /// # Errors
    ///
    /// Returns an error if the organization is not found in routing tables.
    fn resolve_with_redirect(&self, organization: OrganizationId) -> Result<ResolveResult, Status> {
        // Default implementation: just try local resolution
        self.resolve(organization).map(ResolveResult::Local)
    }

    /// Returns the system region context.
    ///
    /// The system region (region 0) handles global operations like
    /// organization creation and user management.
    fn system_region(&self) -> Result<RegionContext, Status>;

    /// Checks if this resolver can emit cross-region redirects.
    ///
    /// Resolvers that recognise remote regions and return
    /// [`ResolveResult::Redirect`] return `true`.
    fn supports_forwarding(&self) -> bool {
        false
    }
}

/// Builds a [`RegionContext`] from an [`InnerGroup`], including block
/// announcements.
///
/// Uses the untyped [`InnerGroup`] because [`RegionContext`] is a shared
/// carrier that downstream resolvers populate regardless of tier. Callers
/// pass `.inner()` from their typed wrapper. This is a documented
/// cross-tier escape hatch — the tier discipline is still enforced at the
/// caller's type boundary, because the caller must have held an
/// `Arc<SystemGroup>`, `Arc<RegionGroup>`, or `Arc<OrganizationGroup>` to
/// reach the inner.
fn region_context_from(
    region: &inferadb_ledger_raft::raft_manager::InnerGroup,
) -> Result<RegionContext, tonic::Status> {
    Ok(RegionContext {
        region: region.region(),
        handle: region.handle().clone(),
        state: region.state().clone(),
        block_archive: region.block_archive().clone(),
        applied_state: region.applied_state().clone(),
        // Populated by callers that resolve a specific vault (e.g.
        // [`crate::services::ReadService::resolve_org_vault`]); the bare
        // org-scoped resolver path leaves this empty so org-only reads
        // continue to fall back to the org-scoped accessor.
        vault_applied_state: None,
        block_announcements: Some(region.block_announcements().clone()),
        batch_handle: region.batch_handle().cloned(),
        commitment_buffer: Some(region.commitment_buffer()),
        leader_lease: Some(region.leader_lease().clone()),
        applied_index_rx: Some(region.applied_index_watch()),
        // Populated alongside `vault_applied_state` by callers that
        // resolve a specific vault. Org-only resolutions leave this
        // empty so follower LINEARIZABLE reads fall back to
        // `applied_index_rx` for the org-scoped path.
        vault_applied_index_rx: None,
    })
}

/// Region resolver backed by the [`RaftManager`].
///
/// Routes organizations to their assigned regions by querying the `_system`
/// organization service directly. Emits cross-region redirects when the
/// organization is not hosted locally so the SDK can reconnect directly.
/// Uses `local_region` for data residency decisions: non-protected regions
/// are always local; protected regions require the node to be in the same region.
pub struct RegionResolverService {
    manager: Arc<RaftManager>,
}

impl RegionResolverService {
    /// Creates a new multi-region resolver.
    pub fn new(manager: Arc<RaftManager>) -> Self {
        Self { manager }
    }

    /// Returns this node's configured region.
    pub fn local_region(&self) -> Region {
        self.manager.local_region()
    }

    /// Whether the given region can be served locally by this node.
    ///
    /// Non-protected regions are replicated to all nodes, so they are
    /// always local. Protected regions are local only when this node's
    /// region matches. Residency is resolved from the GLOBAL region
    /// directory; an unknown region (no directory entry) is treated as
    /// non-protected — production paths write the entry first, so a
    /// missing entry implies a test fixture or transient race against
    /// the directory write.
    fn is_region_local(&self, region: Region) -> bool {
        if region.is_global() {
            return true;
        }
        let requires_residency = self
            .manager
            .system_region()
            .ok()
            .and_then(|sys| {
                inferadb_ledger_state::system::lookup_region_residency(sys.state(), region)
                    .ok()
                    .flatten()
            })
            .map(|r| r.requires_residency)
            .unwrap_or(false);
        if !requires_residency {
            return true;
        }
        self.local_region() == region
    }
}

impl RegionResolver for RegionResolverService {
    fn resolve(&self, organization: OrganizationId) -> Result<RegionContext, Status> {
        // System organization (0) always goes to system region
        if organization == OrganizationId::new(0) {
            return self.system_region();
        }

        // Look up the region for this organization
        let region = self.manager.route_organization(organization).ok_or_else(|| {
            // Check if it's a routing issue or the region is on another node
            if let Some(region_id) = self.manager.get_organization_region(organization) {
                // Region exists but not on this node - use resolve_with_redirect instead
                Status::unavailable(format!(
                    "Organization {} is on region {} which is not available locally. \
                     Use resolve_with_redirect() for cross-region redirect support.",
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

        region_context_from(region.inner())
    }

    fn resolve_with_redirect(&self, organization: OrganizationId) -> Result<ResolveResult, Status> {
        // System organization (0) always goes to system region
        if organization == OrganizationId::new(0) {
            return self.system_region().map(ResolveResult::Local);
        }

        // Look up organization registry directly from _system service.
        let system_state = self
            .manager
            .system_region()
            .map_err(|e| Status::unavailable(format!("System region not available: {}", e)))?;
        let sys = inferadb_ledger_state::system::SystemOrganizationService::new(
            system_state.state().clone(),
        );

        let registry = sys
            .get_organization(organization)
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "Organization {} not found in routing table",
                    organization
                ))
            })?;

        if registry.status != OrganizationStatus::Active {
            return Err(Status::unavailable(format!(
                "Organization {} is {:?}",
                organization, registry.status
            )));
        }

        let org_region = registry.region;
        let routing = RoutingInfo {
            region: org_region,
            // Cross-region resolver doesn't know the target region's leader
            // authoritatively. SDK falls back to ResolveRegionLeader /
            // WatchLeader on an in-region node to learn the real leader.
            leader_hint: None,
        };

        // Region-aware routing decision:
        // - Non-protected regions: all nodes hold replicas → always local
        // - Protected regions, node in-region: local
        // - Protected regions, node out-of-region: redirect to in-region node
        if self.is_region_local(org_region) {
            // Local: non-protected or same-region protected
            let region = self.manager.route_organization(organization).ok_or_else(|| {
                Status::unavailable(format!(
                    "Region {} is locally assigned but not yet started",
                    org_region
                ))
            })?;

            return Ok(ResolveResult::Local(region_context_from(region.inner())?));
        }

        // Protected region, out-of-region: redirect with warning
        tracing::warn!(
            organization = organization.value(),
            org_region = org_region.as_str(),
            local_region = self.local_region().as_str(),
            "Cross-region request for protected region; returning redirect to in-region node"
        );
        metrics::record_data_residency_violation(org_region.as_str());

        Ok(ResolveResult::Redirect(RedirectInfo { region: org_region, organization, routing }))
    }

    fn system_region(&self) -> Result<RegionContext, Status> {
        let region = self
            .manager
            .system_region()
            .map_err(|e| Status::unavailable(format!("System region not available: {}", e)))?;

        region_context_from(region.inner())
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
    fn redirect_info_stores_fields_correctly() {
        let routing =
            RoutingInfo { region: Region::US_EAST_VA, leader_hint: Some("node-1".to_string()) };

        let redirect_info = RedirectInfo {
            region: Region::US_EAST_VA,
            organization: OrganizationId::new(42),
            routing: routing.clone(),
        };

        assert_eq!(redirect_info.region, Region::US_EAST_VA);
        assert_eq!(redirect_info.organization, OrganizationId::new(42));
        assert_eq!(redirect_info.routing.region, Region::US_EAST_VA);
        assert_eq!(redirect_info.routing.leader_hint, Some("node-1".to_string()));
    }

    #[test]
    fn resolve_result_redirect_variant_stores_fields() {
        let routing = RoutingInfo { region: Region::IE_EAST_DUBLIN, leader_hint: None };

        let redirect = RedirectInfo {
            region: Region::IE_EAST_DUBLIN,
            organization: OrganizationId::new(100),
            routing,
        };

        let result = ResolveResult::Redirect(redirect);

        match result {
            ResolveResult::Local(_) => unreachable!("Expected Redirect variant"),
            ResolveResult::Redirect(info) => {
                assert_eq!(info.region, Region::IE_EAST_DUBLIN);
                assert_eq!(info.organization, OrganizationId::new(100));
                assert_eq!(info.routing.region, Region::IE_EAST_DUBLIN);
            },
        }
    }

    #[test]
    fn region_resolver_trait_is_object_safe() {
        // Verify RegionResolver is object-safe (can be used as dyn trait)
        fn _check_trait_impl<T: RegionResolver>(_: &T) {}
    }

    #[test]
    fn redirect_info_debug_output() {
        // Verify our custom Debug impl compiles and works
        let debug_output = format!(
            "{:?}",
            RedirectInfo {
                region: Region::US_EAST_VA,
                organization: OrganizationId::new(1),
                routing: RoutingInfo { region: Region::US_EAST_VA, leader_hint: None },
            }
        );
        assert!(debug_output.contains("RedirectInfo"));
    }

    #[test]
    fn resolve_result_debug_output() {
        // Verify Debug impl for RedirectInfo (ResolveResult::Redirect)
        let routing =
            RoutingInfo { region: Region::US_EAST_VA, leader_hint: Some("node-1".to_string()) };

        let redirect_info = RedirectInfo {
            region: Region::US_EAST_VA,
            organization: OrganizationId::new(42),
            routing,
        };

        let result = ResolveResult::Redirect(redirect_info);
        let debug_output = format!("{:?}", result);
        assert!(debug_output.contains("Redirect"));
        assert!(debug_output.contains("region"));
        assert!(debug_output.contains("organization"));
    }

    #[test]
    fn multi_region_resolver_returns_configured_local_region() {
        // RegionResolverService delegates local_region() to RaftManager.
        // Verify the accessor exists and returns the manager's local_region.
        // Full integration test requires async Raft setup; here we test the
        // type-level wiring.
        use inferadb_ledger_raft::raft_manager::{RaftManager, RaftManagerConfig};
        use inferadb_ledger_test_utils::TestDir;

        let temp = TestDir::new();
        let config =
            RaftManagerConfig::new(temp.path().to_path_buf(), 1, Region::DE_CENTRAL_FRANKFURT);
        let manager = Arc::new(RaftManager::new(
            config,
            Arc::new(inferadb_ledger_raft::node_registry::NodeConnectionRegistry::new()),
        ));

        let resolver = RegionResolverService::new(manager);
        assert_eq!(resolver.local_region(), Region::DE_CENTRAL_FRANKFURT);
    }

    #[test]
    fn multi_region_resolver_supports_forwarding() {
        use inferadb_ledger_raft::raft_manager::{RaftManager, RaftManagerConfig};
        use inferadb_ledger_test_utils::TestDir;

        let temp = TestDir::new();
        let config = RaftManagerConfig::new(temp.path().to_path_buf(), 1, Region::GLOBAL);
        let manager = Arc::new(RaftManager::new(
            config,
            Arc::new(inferadb_ledger_raft::node_registry::NodeConnectionRegistry::new()),
        ));

        let resolver = RegionResolverService::new(manager);
        assert!(resolver.supports_forwarding());
    }

    #[test]
    fn default_resolver_does_not_support_forwarding() {
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
    fn default_resolve_with_redirect_delegates_to_resolve() {
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
        let result = resolver.resolve_with_redirect(OrganizationId::new(42));
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    /// `region_context_from(...)` is the single construction site for
    /// [`RegionContext`] in the resolver path. The org-only resolution
    /// route (no `vault_id`) must leave `vault_applied_index_rx` empty so
    /// downstream LINEARIZABLE-read selection in
    /// `ReadService::follower_read_index` falls back to `applied_index_rx`
    /// for the parent organization group. A non-`None` default here
    /// would silently route org-scoped reads to a vault group's watch
    /// channel that never observes org-scoped applies.
    #[tokio::test]
    async fn region_context_from_defaults_vault_applied_index_rx_to_none() {
        use inferadb_ledger_raft::raft_manager::{RaftManager, RaftManagerConfig, RegionConfig};
        use inferadb_ledger_test_utils::TestDir;

        let temp = TestDir::new();
        let config = RaftManagerConfig::new(temp.path().to_path_buf(), 1, Region::GLOBAL);
        let manager = Arc::new(RaftManager::new(
            config,
            Arc::new(inferadb_ledger_raft::node_registry::NodeConnectionRegistry::new()),
        ));
        let region_config =
            RegionConfig::system(1, "127.0.0.1:50051".to_string()).without_background_jobs();
        manager.start_system_region(region_config).await.expect("start system region");

        let resolver = RegionResolverService::new(Arc::clone(&manager));
        let ctx = resolver.system_region().expect("system region context");

        // Bare-org resolution (system_region / route_organization paths)
        // must leave both vault-scoped fields empty — they are populated
        // only by callers that resolve a specific (region, org, vault).
        assert!(ctx.vault_applied_state.is_none());
        assert!(ctx.vault_applied_index_rx.is_none());

        // The org-scoped applied-index watch IS populated by
        // `region_context_from`, so the LINEARIZABLE-read fallback path
        // has a watch channel to wait on when no vault group is live.
        assert!(ctx.applied_index_rx.is_some());
    }

    /// The manual `Debug` impl on [`RegionContext`] feeds canonical log
    /// lines and panic messages. It must surface the per-vault watch
    /// presence flag — without it, debugging a stuck LINEARIZABLE read
    /// can't tell whether the request resolved to a vault group or the
    /// parent org. Mirrors the existing `redirect_info_debug_output` /
    /// `resolve_result_debug_output` style: build a context, format it,
    /// and assert the field key + boolean both appear.
    #[tokio::test]
    async fn region_context_debug_output_includes_watch_flags() {
        use inferadb_ledger_raft::raft_manager::{RaftManager, RaftManagerConfig, RegionConfig};
        use inferadb_ledger_test_utils::TestDir;

        let temp = TestDir::new();
        let config = RaftManagerConfig::new(temp.path().to_path_buf(), 1, Region::GLOBAL);
        let manager = Arc::new(RaftManager::new(
            config,
            Arc::new(inferadb_ledger_raft::node_registry::NodeConnectionRegistry::new()),
        ));
        let region_config =
            RegionConfig::system(1, "127.0.0.1:50051".to_string()).without_background_jobs();
        manager.start_system_region(region_config).await.expect("start system region");

        let resolver = RegionResolverService::new(Arc::clone(&manager));
        let ctx = resolver.system_region().expect("system region context");

        let debug_output = format!("{:?}", ctx);

        // Field name must appear so log scrapers / panic messages can
        // grep for the per-vault watch state alongside the org watch.
        assert!(
            debug_output.contains("applied_index_rx"),
            "Debug output missing `applied_index_rx`: {debug_output}"
        );
        assert!(
            debug_output.contains("vault_applied_index_rx"),
            "Debug output missing `vault_applied_index_rx`: {debug_output}"
        );

        // The boolean must reflect `is_some()` of each field. For an
        // org-only resolution (no vault_id), `applied_index_rx` is
        // populated and `vault_applied_index_rx` is empty.
        assert!(
            debug_output.contains("applied_index_rx: true"),
            "Expected `applied_index_rx: true` in: {debug_output}"
        );
        assert!(
            debug_output.contains("vault_applied_index_rx: false"),
            "Expected `vault_applied_index_rx: false` in: {debug_output}"
        );
    }

    /// Companion to `region_context_debug_output_includes_watch_flags` —
    /// covers the `Some(...)` branch by injecting a vault-scoped watch
    /// receiver directly. Guards against a future Debug-impl change that
    /// swaps `is_some()` for `Some(...)`-style formatting and silently
    /// changes the substring downstream tools grep for.
    #[tokio::test]
    async fn region_context_debug_output_shows_vault_watch_when_attached() {
        use inferadb_ledger_raft::raft_manager::{RaftManager, RaftManagerConfig, RegionConfig};
        use inferadb_ledger_test_utils::TestDir;

        let temp = TestDir::new();
        let config = RaftManagerConfig::new(temp.path().to_path_buf(), 1, Region::GLOBAL);
        let manager = Arc::new(RaftManager::new(
            config,
            Arc::new(inferadb_ledger_raft::node_registry::NodeConnectionRegistry::new()),
        ));
        let region_config =
            RegionConfig::system(1, "127.0.0.1:50052".to_string()).without_background_jobs();
        manager.start_system_region(region_config).await.expect("start system region");

        let resolver = RegionResolverService::new(Arc::clone(&manager));
        let mut ctx = resolver.system_region().expect("system region context");

        // Inject a synthetic per-vault watch receiver. The actual value
        // sent on the channel is irrelevant — Debug only inspects
        // `is_some()`. We bind the sender into `_tx` to keep the channel
        // alive for the duration of the assertion.
        let (_tx, rx) = tokio::sync::watch::channel::<u64>(0);
        ctx.vault_applied_index_rx = Some(rx);

        let debug_output = format!("{:?}", ctx);
        assert!(
            debug_output.contains("vault_applied_index_rx: true"),
            "Expected `vault_applied_index_rx: true` after attach, got: {debug_output}"
        );
    }
}
