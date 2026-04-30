//! Proposal service abstraction for Raft consensus.
//!
//! Decouples gRPC service handlers from the concrete `ConsensusHandle`
//! transport, enabling unit testing with [`MockProposalService`] while
//! production code uses [`RaftProposalService`].

use std::{sync::Arc, time::Duration};

use inferadb_ledger_raft::{
    ConsensusHandle, HandleError,
    raft_manager::RaftManager,
    types::{LedgerResponse, OrganizationRequest, RaftPayload},
};
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{OrganizationId, Region, VaultId};
use inferadb_ledger_wire::{ErrorCode, WireError};

use crate::services::{error_classify, wire_helpers};

/// Serializes a `RaftPayload<R>` to postcard bytes, returning a wire-shaped
/// error on failure.
///
/// Used by the wire-bound propose pipeline in
/// [`super::services::service_infra::ServiceContext`] so the path never
/// round-trips through `tonic::Status`.
pub(crate) fn serialize_payload_wire<R: serde::Serialize>(
    payload: RaftPayload<R>,
) -> Result<Vec<u8>, WireError> {
    postcard::to_allocvec(&payload).map_err(|e| {
        crate::services::wire_helpers::build_wire_error(
            ErrorCode::Internal,
            format!("payload serialization failed: {e}"),
            "",
            false,
            0,
            std::collections::BTreeMap::new(),
            "",
        )
    })
}

/// Simplified Raft metrics for vault genesis blocks and service context.
///
/// Exposes only the fields needed by downstream consumers (vault creation
/// populates genesis block headers).
pub struct LedgerRaftMetrics {
    /// Current leader node ID, if elected.
    pub(crate) current_leader: Option<u64>,
    /// Current Raft term.
    pub(crate) current_term: u64,
    /// This node's ID.
    pub(crate) id: u64,
}

/// Abstraction over Raft proposal submission.
///
/// Production code uses [`RaftProposalService`], which wraps the custom Raft
/// engine and [`RaftManager`]. Test code uses [`MockProposalService`], which
/// returns canned responses and captures proposals for assertion.
///
/// Exposed as `pub` (rather than `pub(crate)`) because `EventsService` (public)
/// accepts an `Arc<dyn ProposalService>` in its bon builder — the derived builder
/// setters inherit `pub` visibility and cannot reference a `pub(crate)` trait.
/// Consumers of the `services` crate should not implement this trait directly;
/// wire through [`RaftProposalService`].
///
/// ## Bytes-oriented interface
///
/// The primary proposal methods accept pre-serialized `postcard(RaftPayload<R>)`
/// bytes. Typed service helpers call [`serialize_payload_wire`] to produce
/// bytes from a `RaftPayload<SystemRequest>` / `RaftPayload<OrganizationRequest>`
/// etc., then call these methods. This decouples the serialization type from
/// the trait and avoids generic methods (which are incompatible with `dyn`
/// trait objects).
#[tonic::async_trait]
pub trait ProposalService: Send + Sync {
    /// Proposes pre-serialized `postcard(RaftPayload<R>)` bytes to the default
    /// (GLOBAL) Raft group.
    ///
    /// Submits bytes through Raft with the given `caller` stamped into the
    /// payload at serialization time by the caller (via [`serialize_payload_wire`]).
    /// Returns the committed response or a wire-shaped error.
    async fn propose_bytes(
        &self,
        bytes: Vec<u8>,
        timeout: Duration,
    ) -> Result<LedgerResponse, WireError>;

    /// Proposes pre-serialized `postcard(RaftPayload<R>)` bytes to a specific
    /// region's Raft group.
    ///
    /// Resolves `region` to an
    /// [`OrganizationGroup`](inferadb_ledger_raft::raft_manager::OrganizationGroup)
    /// via the [`RaftManager`], then proposes through that group's Raft instance.
    ///
    /// If the local node is not the leader for the target region, returns a
    /// `NotLeader` `Status` with leader-hint `ErrorDetails` attached. Callers
    /// should propagate the status to the client; the SDK uses the hint to
    /// retry directly against the within-region leader. This method does not
    /// itself perform any cross-node forwarding.
    async fn propose_to_region_bytes(
        &self,
        region: Region,
        bytes: Vec<u8>,
        timeout: Duration,
    ) -> Result<LedgerResponse, WireError>;

    /// Proposes pre-serialized `postcard(RaftPayload<OrganizationRequest>)` bytes
    /// to a specific organization's per-org Raft group.
    ///
    /// Resolves `organization` to its `OrganizationGroup` via
    /// [`RaftManager::route_organization`], then proposes through that group's
    /// consensus handle.
    ///
    /// Returns `UNAVAILABLE` when the organization is not yet placed on this
    /// node (transitional state between `CreateOrganization` apply and
    /// `start_organization_group`). The caller should propagate this status
    /// to the client so the SDK can retry on the correct leader.
    async fn propose_to_organization_bytes(
        &self,
        region: Region,
        organization: OrganizationId,
        bytes: Vec<u8>,
        timeout: Duration,
    ) -> Result<LedgerResponse, WireError>;

    /// Proposes pre-serialized `postcard(RaftPayload<OrganizationRequest>)` bytes
    /// to a specific vault's per-vault Raft shard.
    ///
    /// Looks up the [`VaultGroup`](inferadb_ledger_raft::raft_manager::VaultGroup)
    /// registered on this node for `(region, organization, vault_id)` and
    /// proposes through its vault-specific
    /// [`ConsensusHandle`](inferadb_ledger_raft::ConsensusHandle). Used by gRPC
    /// handlers that target vault-scoped variants (`Write`, `BatchWrite`,
    /// `IngestExternalEvents`) — the vault-scoped apply path on the vault
    /// shard runs the per-vault tier-validation gate.
    ///
    /// `vault_id` is the internal `VaultId` resolved at the handler via
    /// `SlugResolver`. It is the routing key used to locate the
    /// `VaultGroup`; the vault identity is also encoded inside the payload
    /// bytes (per-operation), so the apply pipeline still tier-validates.
    ///
    /// `organization_slug` and `vault_slug` are the external Snowflake slugs
    /// the gRPC handler received from the client. They are forwarded into
    /// the `NotLeader` `LeaderHint` so the SDK's `VaultLeaderCache` — keyed
    /// on `(OrganizationSlug, VaultSlug)` — can update without an extra
    /// resolve round-trip. Both `None` means the slugs aren't known at
    /// the call site (e.g. internal callers); the SDK falls through to the
    /// region path in that case.
    ///
    /// Returns `UNAVAILABLE` when the vault group is not registered on this
    /// node (not yet started, deleted, or migrated). The caller propagates
    /// the status so the SDK can retry on the correct leader.
    #[allow(clippy::too_many_arguments)]
    async fn propose_to_vault_bytes(
        &self,
        region: Region,
        organization: OrganizationId,
        vault_id: VaultId,
        organization_slug: Option<u64>,
        vault_slug: Option<u64>,
        bytes: Vec<u8>,
        timeout: Duration,
    ) -> Result<LedgerResponse, WireError>;

    /// Proposes a typed `OrganizationRequest` to a specific vault's per-vault
    /// Raft shard, automatically piggybacking any pending state-root
    /// commitments accumulated on the vault group.
    ///
    /// This is the typed sibling of [`Self::propose_to_vault_bytes`] used by
    /// `WriteService` and other handlers that need to integrate with the
    /// per-vault [`BatchWriterHandle`](inferadb_ledger_raft::BatchWriterHandle)
    /// (when present) and with the per-vault `commitment_buffer`. The bytes
    /// method does not — events ingestion does not piggyback commitments and
    /// has no batch handle — so the two methods coexist.
    ///
    /// Routing rules:
    /// - When the vault group exposes a [`BatchWriterHandle`], the request is handed to that batch
    ///   writer; it drains the commitment buffer and constructs the `RaftPayload` itself (see
    ///   `start_vault_group`'s `submit_fn` in `raft_manager.rs`). This preserves the per-fsync
    ///   amortization that keeps multi-vault throughput intact.
    /// - Otherwise, the call site path drains `vault_group.commitment_buffer()`, constructs a
    ///   one-shot `RaftPayload` (with `caller` and the drained commitments), and proposes through
    ///   `vault_group.handle().propose_and_wait`.
    ///
    /// `organization_slug` and `vault_slug` carry the external Snowflake
    /// slugs through to the `NotLeader` hint emitted on a leader miss. The
    /// SDK's `VaultLeaderCache` is keyed on `(OrganizationSlug, VaultSlug)`,
    /// so propagating slugs end-to-end keeps cache hits warm. Both `None`
    /// when the call site doesn't have slugs in scope.
    ///
    /// Returns:
    /// - `StaleRouting` (with `LeaderHint` context) when the vault group is not registered on this
    ///   node, or when the local node is not the leader for the vault.
    /// - `FailedPrecondition` on Raft proposal timeout (also stamps the
    ///   `record_raft_proposal_timeout` metric).
    /// - The classified [`WireError`] for any other consensus error (via
    ///   [`crate::services::error_classify::classify_raft_error_wire`]).
    #[allow(clippy::too_many_arguments)]
    async fn propose_organization_request_to_vault(
        &self,
        region: Region,
        organization: OrganizationId,
        vault_id: VaultId,
        organization_slug: Option<u64>,
        vault_slug: Option<u64>,
        request: OrganizationRequest,
        caller: u64,
        timeout: Duration,
    ) -> Result<LedgerResponse, WireError>;

    /// Returns the state layer for a specific region's Raft group.
    ///
    /// Enables direct reads from a region's state without proposing through
    /// Raft. Used by handlers that read onboarding accounts, user profiles,
    /// or other regional data.
    fn regional_state(&self, region: Region) -> Result<Arc<StateLayer<FileBackend>>, WireError>;

    /// Returns current Raft metrics for the GLOBAL group.
    ///
    /// Used by vault creation to populate genesis block headers with leader ID
    /// and term. Returns `None` when metrics are unavailable (e.g., in tests).
    fn raft_metrics(&self) -> Option<LedgerRaftMetrics> {
        None
    }
}

/// Forwards a GLOBAL Raft proposal to the actual GLOBAL leader via the
/// internal `RegionalProposal` RPC over the wire transport.
///
/// Used as a `NotLeader` recovery path inside [`RaftProposalService::propose_bytes`]
/// so multi-tier writes (e.g. `CreateVault`'s per-org propose followed by a
/// GLOBAL `RegisterVaultDirectoryEntry`) succeed when the gRPC handler runs
/// on a node that is the per-org leader but not the GLOBAL leader. The
/// receiving handler authenticates the caller via the cluster peer-address
/// map and proposes the bytes against its local GLOBAL group.
///
/// Returns a [`WireError`] mirroring what a local propose would have produced
/// — the calling handler is unaware whether the proposal committed locally
/// or through a forwarded round-trip.
async fn forward_global_proposal(
    manager: Option<&Arc<RaftManager>>,
    handle: &Arc<ConsensusHandle>,
    bytes: Vec<u8>,
    timeout: Duration,
) -> Result<LedgerResponse, WireError> {
    let Some(manager) = manager else {
        // Single-region setup with no manager wired — fall back to the
        // original NotLeader semantics so the SDK can retry on a leader
        // it discovers via `ResolveRegionLeader` / `WatchLeader`.
        return Err(consensus_error_to_wire_error(
            inferadb_ledger_consensus::ConsensusError::NotLeader,
        ));
    };

    let leader_id = handle.current_leader().ok_or_else(|| {
        wire_helpers::build_wire_error(
            ErrorCode::StaleRouting,
            "GLOBAL proposal: no known leader to forward to",
            "",
            false,
            0,
            std::collections::BTreeMap::new(),
            "",
        )
    })?;
    let leader_addr = manager.peer_addresses().get(leader_id).ok_or_else(|| {
        wire_helpers::build_wire_error(
            ErrorCode::StaleRouting,
            format!("GLOBAL proposal: no address for leader {leader_id} in peer registry"),
            "",
            false,
            0,
            std::collections::BTreeMap::new(),
            "",
        )
    })?;

    // Resolve the leader's wire client through the shared registry. The
    // wire-aware cache keys on `(node_id, addr)` and shares a single QUIC
    // connection across every subsystem (Raft replication, saga forwarding,
    // snapshots, etc.).
    let wire_client =
        manager.registry().wire_client_for(leader_id, &leader_addr).await.map_err(|e| {
            wire_helpers::build_wire_error(
                ErrorCode::StaleRouting,
                format!("register GLOBAL leader peer: {e}"),
                "",
                false,
                0,
                std::collections::BTreeMap::new(),
                "",
            )
        })?;
    let client = inferadb_ledger_wire_services::RaftServiceClient::new(wire_client);

    let rpc_request = inferadb_ledger_wire::services::raft::RegionalProposalRequest {
        region: Some(Region::GLOBAL.as_str().to_string()),
        request_payload: bytes::Bytes::from(bytes),
        caller: 0,
        timeout_ms: u32::try_from(timeout.as_millis()).unwrap_or(u32::MAX),
    };

    let result = client.regional_proposal(rpc_request, 0).await.map_err(|e| {
        wire_helpers::build_wire_error(
            ErrorCode::StaleRouting,
            format!("forward GLOBAL proposal: {e}"),
            "",
            false,
            0,
            std::collections::BTreeMap::new(),
            "",
        )
    })?;

    if result.status_code != 0 {
        let code = grpc_code_to_wire(result.status_code);
        return Err(wire_helpers::build_wire_error(
            code,
            result.error_message,
            "",
            false,
            0,
            std::collections::BTreeMap::new(),
            "",
        ));
    }

    inferadb_ledger_types::decode::<LedgerResponse>(&result.response_payload).map_err(|e| {
        wire_helpers::build_wire_error(
            ErrorCode::Internal,
            format!("decode forwarded GLOBAL response: {e}"),
            "",
            false,
            0,
            std::collections::BTreeMap::new(),
            "",
        )
    })
}

/// Converts a [`ConsensusError`](inferadb_ledger_consensus::ConsensusError) into
/// a [`WireError`] using the error's structured `grpc_code()` mapping.
///
/// Replaces the previous `ConsensusError` → `tonic::Status` → `WireError`
/// round-trip with a single direct hop.
pub(crate) fn consensus_error_to_wire_error(
    err: inferadb_ledger_consensus::ConsensusError,
) -> WireError {
    let message = format!("Raft error: {err}");
    let code = grpc_code_to_wire(err.grpc_code());
    wire_helpers::build_wire_error(
        code,
        message,
        "",
        false,
        0,
        std::collections::BTreeMap::new(),
        "",
    )
}

/// Maps a numeric gRPC status code (returned by
/// [`inferadb_ledger_consensus::ConsensusError::grpc_code`]) into the
/// closest wire [`ErrorCode`].
fn grpc_code_to_wire(code: i32) -> ErrorCode {
    match code {
        3 => ErrorCode::InvalidArgument,
        6 => ErrorCode::AlreadyExists,
        8 => ErrorCode::RateLimited,
        9 => ErrorCode::FailedPrecondition,
        14 => ErrorCode::StaleRouting,
        // 13 (Internal) and any unknown code map to Internal.
        _ => ErrorCode::Internal,
    }
}

/// Production [`ProposalService`] backed by [`ConsensusHandle`] and [`RaftManager`].
///
/// Owns the consensus handle and manager reference, delegating proposal
/// submission to the appropriate region's consensus group.
pub(crate) struct RaftProposalService {
    handle: Arc<ConsensusHandle>,
    manager: Option<Arc<RaftManager>>,
}

impl RaftProposalService {
    /// Creates a new `RaftProposalService`.
    ///
    /// `manager` is required for regional proposals. Pass `None` only
    /// in single-region setups where `propose_to_region` is never called.
    pub(crate) fn new(handle: Arc<ConsensusHandle>, manager: Option<Arc<RaftManager>>) -> Self {
        Self { handle, manager }
    }
}

/// Wire-shaped builder for the rich `NotLeader` hint payload. Delegates to
/// [`crate::services::metadata::not_leader_wire_error_from_handle`] so all
/// not-leader rejections share the same context-key shape the SDK reads.
fn not_leader_wire_error(
    handle: &inferadb_ledger_raft::ConsensusHandle,
    peer_addresses: Option<&inferadb_ledger_raft::PeerAddressMap>,
    message: impl Into<String>,
    leader_vault: Option<u64>,
    leader_organization_slug: Option<u64>,
    leader_vault_slug: Option<u64>,
) -> WireError {
    crate::services::metadata::not_leader_wire_error_from_handle(
        handle,
        peer_addresses,
        message,
        leader_vault,
        leader_organization_slug,
        leader_vault_slug,
    )
}

/// Wire-shaped builder for the `DEADLINE_EXCEEDED`-equivalent error returned
/// when a Raft proposal times out. Mirrors the previous
/// `Status::deadline_exceeded(...)` paths — the wire side maps timeouts to
/// [`ErrorCode::FailedPrecondition`] (the canonical-form image of
/// `Code::DeadlineExceeded` under `tonic_code_to_wire`).
fn raft_timeout_wire_error(message: impl Into<String>) -> WireError {
    wire_helpers::build_wire_error(
        ErrorCode::FailedPrecondition,
        message,
        "",
        false,
        0,
        std::collections::BTreeMap::new(),
        "",
    )
}

/// Wire-shaped builder for the `INTERNAL` error returned when a `HandleError`
/// surfaces a non-`Consensus`, non-`Timeout` failure (e.g. transport).
fn raft_internal_wire_error(error: impl std::fmt::Display) -> WireError {
    wire_helpers::build_wire_error(
        ErrorCode::Internal,
        error.to_string(),
        "",
        false,
        0,
        std::collections::BTreeMap::new(),
        "",
    )
}

#[tonic::async_trait]
impl ProposalService for RaftProposalService {
    async fn propose_bytes(
        &self,
        bytes: Vec<u8>,
        timeout: Duration,
    ) -> Result<LedgerResponse, WireError> {
        match self.handle.propose_bytes_and_wait(bytes.clone(), timeout).await {
            Ok(response) => Ok(response),
            Err(HandleError::Consensus { source, .. })
                if source.to_string().contains("Not the leader") =>
            {
                // Multi-tier leader coordination: a gRPC handler can run on
                // a node that is the per-org leader for step (a) but NOT the
                // GLOBAL leader for step (b) (e.g., `CreateVault`'s two-phase
                // write — per-org propose followed by GLOBAL slug-index
                // register). Without forwarding, step (b) returns
                // `NotLeader` and the SDK retries on the GLOBAL-leader node,
                // which then can't satisfy step (a) (it is not the per-org
                // leader). The two-phase write would never converge if
                // GLOBAL leader and per-org leader are different nodes.
                //
                // Forward the GLOBAL proposal to the actual GLOBAL leader
                // via the existing server-to-server `RegionalProposal` RPC
                // (which routes to `(GLOBAL, OrganizationId(0))` when the
                // request `region` field is GLOBAL).
                forward_global_proposal(self.manager.as_ref(), &self.handle, bytes, timeout).await
            },
            Err(HandleError::Consensus { source, .. }) => {
                Err(consensus_error_to_wire_error(source))
            },
            Err(HandleError::Timeout { .. }) => {
                inferadb_ledger_raft::metrics::record_raft_proposal_timeout();
                Err(raft_timeout_wire_error(format!(
                    "Raft proposal timed out after {}ms",
                    timeout.as_millis()
                )))
            },
            Err(e) => Err(raft_internal_wire_error(e)),
        }
    }

    async fn propose_to_region_bytes(
        &self,
        region: Region,
        bytes: Vec<u8>,
        timeout: Duration,
    ) -> Result<LedgerResponse, WireError> {
        let manager = self.manager.as_ref().ok_or_else(|| {
            wire_helpers::build_wire_error(
                ErrorCode::FailedPrecondition,
                "Regional proposals require RaftManager configuration",
                "",
                false,
                0,
                std::collections::BTreeMap::new(),
                "",
            )
        })?;

        // Look up the region group. If the region doesn't exist, return
        // FAILED_PRECONDITION — region creation is an explicit admin operation
        // (AdminService::create_data_region proposes CreateDataRegion to GLOBAL
        // Raft), not a lazy side-effect of the first data write.
        let region_group = match manager.get_region_group(region) {
            Ok(group) => group,
            Err(_) => {
                return Err(wire_helpers::build_wire_error(
                    ErrorCode::FailedPrecondition,
                    format!(
                        "Region {region} is not active on this node; create it explicitly via \
                         AdminService::create_data_region before proposing writes"
                    ),
                    "",
                    false,
                    0,
                    std::collections::BTreeMap::new(),
                    "",
                ));
            },
        };

        // Check leadership eagerly. If we're not the data region leader, return
        // a NotLeader hint up front — the SDK's region-leader cache uses this
        // hint to retry directly against the within-region leader without going
        // through a server-side forwarding hop.
        if !region_group.handle().is_leader() {
            return Err(not_leader_wire_error(
                region_group.handle().as_ref(),
                Some(manager.peer_addresses()),
                format!("Not the leader for region {region}"),
                None,
                None,
                None,
            ));
        }

        match region_group.handle().propose_bytes_and_wait(bytes, timeout).await {
            Ok(response) => Ok(response),
            Err(HandleError::Consensus { source, .. })
                if source.to_string().contains("Not the leader") =>
            {
                // Leadership changed between the check above and the propose.
                // Rare but possible — belt-and-suspenders to still surface the
                // NotLeader hint when the consensus engine rejects post-check.
                Err(not_leader_wire_error(
                    region_group.handle().as_ref(),
                    Some(manager.peer_addresses()),
                    format!("Not the leader for region {region} (lost leadership mid-propose)"),
                    None,
                    None,
                    None,
                ))
            },
            Err(HandleError::Consensus { source, .. }) => {
                Err(consensus_error_to_wire_error(source))
            },
            Err(HandleError::Timeout { .. }) => {
                inferadb_ledger_raft::metrics::record_raft_proposal_timeout();
                Err(raft_timeout_wire_error(format!(
                    "Regional Raft proposal timed out after {}ms (region: {region})",
                    timeout.as_millis()
                )))
            },
            Err(e) => Err(raft_internal_wire_error(e)),
        }
    }

    async fn propose_to_organization_bytes(
        &self,
        region: Region,
        organization: OrganizationId,
        bytes: Vec<u8>,
        timeout: Duration,
    ) -> Result<LedgerResponse, WireError> {
        let manager = self.manager.as_ref().ok_or_else(|| {
            wire_helpers::build_wire_error(
                ErrorCode::FailedPrecondition,
                "Per-organization proposals require RaftManager configuration",
                "",
                false,
                0,
                std::collections::BTreeMap::new(),
                "",
            )
        })?;

        let org_group = manager.route_organization(organization).ok_or_else(|| {
            wire_helpers::build_wire_error(
                ErrorCode::StaleRouting,
                format!(
                    "Organization {organization} is not active on this node in region {region}"
                ),
                "",
                false,
                0,
                std::collections::BTreeMap::new(),
                "",
            )
        })?;

        if !org_group.handle().is_leader() {
            return Err(not_leader_wire_error(
                org_group.handle().as_ref(),
                Some(manager.peer_addresses()),
                format!("Not the leader for organization {organization} in region {region}"),
                None,
                None,
                None,
            ));
        }

        match org_group.handle().propose_bytes_and_wait(bytes, timeout).await {
            Ok(response) => Ok(response),
            Err(HandleError::Consensus { source, .. })
                if source.to_string().contains("Not the leader") =>
            {
                Err(not_leader_wire_error(
                    org_group.handle().as_ref(),
                    Some(manager.peer_addresses()),
                    format!(
                        "Not the leader for organization {organization} in region {region} (lost \
                         leadership mid-propose)"
                    ),
                    None,
                    None,
                    None,
                ))
            },
            Err(HandleError::Consensus { source, .. }) => {
                Err(consensus_error_to_wire_error(source))
            },
            Err(HandleError::Timeout { .. }) => {
                inferadb_ledger_raft::metrics::record_raft_proposal_timeout();
                Err(raft_timeout_wire_error(format!(
                    "Org Raft proposal timed out after {}ms (org: {organization}, region: {region})",
                    timeout.as_millis()
                )))
            },
            Err(e) => Err(raft_internal_wire_error(e)),
        }
    }

    async fn propose_to_vault_bytes(
        &self,
        region: Region,
        organization: OrganizationId,
        vault_id: VaultId,
        organization_slug: Option<u64>,
        vault_slug: Option<u64>,
        bytes: Vec<u8>,
        timeout: Duration,
    ) -> Result<LedgerResponse, WireError> {
        let manager = self.manager.as_ref().ok_or_else(|| {
            wire_helpers::build_wire_error(
                ErrorCode::FailedPrecondition,
                "Per-vault proposals require RaftManager configuration",
                "",
                false,
                0,
                std::collections::BTreeMap::new(),
                "",
            )
        })?;

        let vault_group =
            manager.get_vault_group(region, organization, vault_id).map_err(|_| {
                wire_helpers::build_wire_error(
                    ErrorCode::StaleRouting,
                    format!(
                        "Vault {vault_id} (organization {organization}) is not active on this \
                         node in region {region}"
                    ),
                    "",
                    false,
                    0,
                    std::collections::BTreeMap::new(),
                    "",
                )
            })?;

        // Phase 7 / O1: record activity on the vault. Wakes a `Dormant`
        // vault back to `Active` synchronously (and fires the wake metric
        // + log line) before the proposal proceeds. Active vaults
        // fast-path through a relaxed atomic store, so the call is
        // cheap on the hot path.
        vault_group.touch_activity();

        // VaultId is i64 (Snowflake-positive); cast preserves the value.
        let vault_hint = Some(vault_id.value() as u64);
        if !vault_group.handle().is_leader() {
            return Err(not_leader_wire_error(
                vault_group.handle().as_ref(),
                Some(manager.peer_addresses()),
                format!(
                    "Not the leader for vault {vault_id} (organization {organization}) in region \
                     {region}"
                ),
                vault_hint,
                organization_slug,
                vault_slug,
            ));
        }

        match vault_group.handle().propose_bytes_and_wait(bytes, timeout).await {
            Ok(response) => Ok(response),
            Err(HandleError::Consensus { source, .. })
                if source.to_string().contains("Not the leader") =>
            {
                Err(not_leader_wire_error(
                    vault_group.handle().as_ref(),
                    Some(manager.peer_addresses()),
                    format!(
                        "Not the leader for vault {vault_id} (organization {organization}) in \
                         region {region} (lost leadership mid-propose)"
                    ),
                    vault_hint,
                    organization_slug,
                    vault_slug,
                ))
            },
            Err(HandleError::Consensus { source, .. }) => {
                Err(consensus_error_to_wire_error(source))
            },
            Err(HandleError::Timeout { .. }) => {
                inferadb_ledger_raft::metrics::record_raft_proposal_timeout();
                Err(raft_timeout_wire_error(format!(
                    "Vault Raft proposal timed out after {}ms (vault: {vault_id}, org: \
                     {organization}, region: {region})",
                    timeout.as_millis()
                )))
            },
            Err(e) => Err(raft_internal_wire_error(e)),
        }
    }

    async fn propose_organization_request_to_vault(
        &self,
        region: Region,
        organization: OrganizationId,
        vault_id: VaultId,
        organization_slug: Option<u64>,
        vault_slug: Option<u64>,
        request: OrganizationRequest,
        caller: u64,
        timeout: Duration,
    ) -> Result<LedgerResponse, WireError> {
        let manager = self.manager.as_ref().ok_or_else(|| {
            wire_helpers::build_wire_error(
                ErrorCode::FailedPrecondition,
                "Per-vault proposals require RaftManager configuration",
                "",
                false,
                0,
                std::collections::BTreeMap::new(),
                "",
            )
        })?;

        let vault_group =
            manager.get_vault_group(region, organization, vault_id).map_err(|_| {
                wire_helpers::build_wire_error(
                    ErrorCode::StaleRouting,
                    format!(
                        "Vault {vault_id} (organization {organization}) is not active on this \
                         node in region {region}"
                    ),
                    "",
                    false,
                    0,
                    std::collections::BTreeMap::new(),
                    "",
                )
            })?;

        // Phase 7 / O1: record activity on the vault. Wakes a `Dormant`
        // vault back to `Active` synchronously before the proposal
        // proceeds. See `propose_to_vault_bytes` above for the rationale.
        vault_group.touch_activity();

        // VaultId is i64 (Snowflake-positive); cast preserves the value.
        let vault_hint = Some(vault_id.value() as u64);
        if !vault_group.handle().is_leader() {
            return Err(not_leader_wire_error(
                vault_group.handle().as_ref(),
                Some(manager.peer_addresses()),
                format!(
                    "Not the leader for vault {vault_id} (organization {organization}) in region \
                     {region}"
                ),
                vault_hint,
                organization_slug,
                vault_slug,
            ));
        }

        inferadb_ledger_raft::metrics::record_raft_proposal();

        // Prefer the per-vault batch writer when present so concurrent
        // writes to the same vault amortize the WAL fsync cost. The
        // batch writer's `submit_fn` (in
        // `raft_manager.rs::start_vault_group`) drains the commitment
        // buffer and constructs the `RaftPayload` itself — we simply
        // hand it the typed request and await the batched result.
        if let Some(batch_handle) = vault_group.batch_handle() {
            let receiver = batch_handle.submit(request);
            return match tokio::time::timeout(timeout, receiver).await {
                Ok(Ok(Ok(response))) => Ok(response),
                Ok(Ok(Err(batch_err))) => {
                    Err(error_classify::classify_raft_error_wire(&batch_err.to_string()))
                },
                Ok(Err(_dropped)) => {
                    Err(error_classify::classify_raft_error_wire("Batch writer dropped response"))
                },
                Err(_elapsed) => {
                    inferadb_ledger_raft::metrics::record_raft_proposal_timeout();
                    Err(raft_timeout_wire_error(format!(
                        "Raft proposal timed out after {}ms",
                        timeout.as_millis()
                    )))
                },
            };
        }

        // Fallback: direct propose through the vault's consensus handle.
        // Drain the commitment buffer and wrap the request in a one-shot
        // `RaftPayload` — same shape the batch writer's `submit_fn`
        // produces, but unbatched.
        let commitments = std::mem::take(
            &mut *vault_group.commitment_buffer().lock().unwrap_or_else(|e| e.into_inner()),
        );
        let payload = RaftPayload {
            request,
            proposed_at: chrono::Utc::now(),
            caller,
            state_root_commitments: commitments,
        };

        match vault_group.handle().propose_and_wait(payload, timeout).await {
            Ok(response) => Ok(response),
            Err(HandleError::Consensus { source, .. })
                if source.to_string().contains("Not the leader") =>
            {
                Err(not_leader_wire_error(
                    vault_group.handle().as_ref(),
                    Some(manager.peer_addresses()),
                    format!(
                        "Not the leader for vault {vault_id} (organization {organization}) in \
                         region {region} (lost leadership mid-propose)"
                    ),
                    vault_hint,
                    organization_slug,
                    vault_slug,
                ))
            },
            Err(HandleError::Consensus { source, .. }) => {
                Err(error_classify::classify_raft_error_wire(&source.to_string()))
            },
            Err(HandleError::Timeout { .. }) => {
                inferadb_ledger_raft::metrics::record_raft_proposal_timeout();
                Err(raft_timeout_wire_error(format!(
                    "Vault Raft proposal timed out after {}ms (vault: {vault_id}, org: \
                     {organization}, region: {region})",
                    timeout.as_millis()
                )))
            },
            Err(e) => Err(error_classify::classify_raft_error_wire(&e.to_string())),
        }
    }

    fn regional_state(&self, region: Region) -> Result<Arc<StateLayer<FileBackend>>, WireError> {
        let manager = self.manager.as_ref().ok_or_else(|| {
            wire_helpers::build_wire_error(
                ErrorCode::FailedPrecondition,
                "Regional state access requires RaftManager configuration",
                "",
                false,
                0,
                std::collections::BTreeMap::new(),
                "",
            )
        })?;

        let region_group = manager.get_region_group(region).map_err(|e| {
            wire_helpers::build_wire_error(
                ErrorCode::StaleRouting,
                format!("Region {region} is not active on this node: {e}"),
                "",
                false,
                0,
                std::collections::BTreeMap::new(),
                "",
            )
        })?;

        Ok(region_group.state().clone())
    }

    fn raft_metrics(&self) -> Option<LedgerRaftMetrics> {
        let state = self.handle.shard_state();
        Some(LedgerRaftMetrics {
            current_leader: state.leader.map(|n| n.0),
            current_term: state.term,
            id: self.handle.node_id(),
        })
    }
}

#[cfg(test)]
pub(crate) mod mock {
    //! Mock [`ProposalService`] for unit testing gRPC handlers.
    //!
    //! Captures proposals for assertion and returns pre-enqueued responses.
    //! Proposals are stored as raw `postcard(RaftPayload<R>)` bytes.
    //! Use [`MockProposalService::decode_proposals`] to deserialize them for
    //! assertions in tests.

    use std::{collections::VecDeque, sync::Arc, time::Duration};

    use inferadb_ledger_raft::types::{LedgerResponse, OrganizationRequest, RaftPayload};
    use inferadb_ledger_state::StateLayer;
    use inferadb_ledger_store::FileBackend;
    use inferadb_ledger_types::{OrganizationId, Region, VaultId};
    use inferadb_ledger_wire::{ErrorCode, WireError};
    use parking_lot::Mutex;

    use super::ProposalService;

    /// Test double for [`ProposalService`] that captures proposals and returns
    /// pre-configured responses.
    ///
    /// Thread-safe via `parking_lot::Mutex` on all interior state.
    /// Proposals are stored as raw bytes (`postcard(RaftPayload<R>)`).
    /// Use [`decode_proposals`](Self::decode_proposals) to deserialize.
    pub(crate) struct MockProposalService {
        responses: Mutex<VecDeque<Result<LedgerResponse, WireError>>>,
        regional_responses: Mutex<VecDeque<Result<LedgerResponse, WireError>>>,
        organization_responses: Mutex<VecDeque<Result<LedgerResponse, WireError>>>,
        vault_responses: Mutex<VecDeque<Result<LedgerResponse, WireError>>>,
        typed_vault_responses: Mutex<VecDeque<Result<LedgerResponse, WireError>>>,
        /// Captured global proposals as raw `postcard(RaftPayload<R>)` bytes.
        proposals: Mutex<Vec<Vec<u8>>>,
        /// Captured regional proposals as raw bytes with (region, bytes).
        regional_proposals: Mutex<Vec<(Region, Vec<u8>)>>,
        /// Captured organization proposals as raw bytes with (region, organization_id, bytes).
        organization_proposals: Mutex<Vec<(Region, OrganizationId, Vec<u8>)>>,
        /// Captured vault proposals as raw bytes with (region, organization_id, vault_id, bytes).
        vault_proposals: Mutex<Vec<(Region, OrganizationId, VaultId, Vec<u8>)>>,
        /// Captured typed-vault proposals (used by the typed
        /// `propose_organization_request_to_vault` API).
        typed_vault_proposals:
            Mutex<Vec<(Region, OrganizationId, VaultId, OrganizationRequest, u64)>>,
        regional_state_layer: Mutex<Option<Arc<StateLayer<FileBackend>>>>,
    }

    impl MockProposalService {
        /// Creates a new mock with empty queues.
        pub(crate) fn new() -> Self {
            Self {
                responses: Mutex::new(VecDeque::new()),
                regional_responses: Mutex::new(VecDeque::new()),
                organization_responses: Mutex::new(VecDeque::new()),
                vault_responses: Mutex::new(VecDeque::new()),
                typed_vault_responses: Mutex::new(VecDeque::new()),
                proposals: Mutex::new(Vec::new()),
                regional_proposals: Mutex::new(Vec::new()),
                organization_proposals: Mutex::new(Vec::new()),
                vault_proposals: Mutex::new(Vec::new()),
                typed_vault_proposals: Mutex::new(Vec::new()),
                regional_state_layer: Mutex::new(None),
            }
        }

        /// Enqueues a [`WireError`]-shaped response for the next
        /// `propose_bytes()` call.
        pub(crate) fn enqueue_wire_error(&self, response: Result<LedgerResponse, WireError>) {
            self.responses.lock().push_back(response);
        }

        /// Convenience wrapper for [`Self::enqueue_wire_error`] when the
        /// caller only ever stages successful responses.
        pub(crate) fn enqueue(&self, response: Result<LedgerResponse, WireError>) {
            self.enqueue_wire_error(response);
        }

        /// Enqueues a response for the next `propose_to_region_bytes()` call.
        #[allow(dead_code)]
        pub(crate) fn enqueue_regional(&self, response: Result<LedgerResponse, WireError>) {
            self.regional_responses.lock().push_back(response);
        }

        /// Enqueues a response for the next `propose_to_organization_bytes()` call.
        #[allow(dead_code)]
        pub(crate) fn enqueue_organization(&self, response: Result<LedgerResponse, WireError>) {
            self.organization_responses.lock().push_back(response);
        }

        /// Enqueues a response for the next `propose_to_vault_bytes()` call.
        #[allow(dead_code)]
        pub(crate) fn enqueue_vault(&self, response: Result<LedgerResponse, WireError>) {
            self.vault_responses.lock().push_back(response);
        }

        /// Enqueues a response for the next
        /// `propose_organization_request_to_vault()` call.
        #[allow(dead_code)]
        pub(crate) fn enqueue_typed_vault(&self, response: Result<LedgerResponse, WireError>) {
            self.typed_vault_responses.lock().push_back(response);
        }

        /// Sets the state layer returned by `regional_state()`.
        #[allow(dead_code)]
        pub(crate) fn set_regional_state(&self, state: Arc<StateLayer<FileBackend>>) {
            *self.regional_state_layer.lock() = Some(state);
        }

        /// Returns the raw proposal bytes captured from `propose_bytes()` calls.
        #[allow(dead_code)]
        pub(crate) fn raw_proposals(&self) -> Vec<Vec<u8>> {
            self.proposals.lock().clone()
        }

        /// Returns the raw regional proposal bytes captured from
        /// `propose_to_region_bytes()` calls.
        #[allow(dead_code)]
        pub(crate) fn raw_regional_proposals(&self) -> Vec<(Region, Vec<u8>)> {
            self.regional_proposals.lock().clone()
        }

        /// Returns the raw organization proposal bytes captured from
        /// `propose_to_organization_bytes()` calls.
        #[allow(dead_code)]
        pub(crate) fn raw_organization_proposals(&self) -> Vec<(Region, OrganizationId, Vec<u8>)> {
            self.organization_proposals.lock().clone()
        }

        /// Returns the raw vault proposal bytes captured from
        /// `propose_to_vault_bytes()` calls.
        #[allow(dead_code)]
        pub(crate) fn raw_vault_proposals(
            &self,
        ) -> Vec<(Region, OrganizationId, VaultId, Vec<u8>)> {
            self.vault_proposals.lock().clone()
        }

        /// Returns the typed vault proposals captured from
        /// `propose_organization_request_to_vault()` calls.
        #[allow(dead_code)]
        pub(crate) fn typed_vault_proposals(
            &self,
        ) -> Vec<(Region, OrganizationId, VaultId, OrganizationRequest, u64)> {
            self.typed_vault_proposals.lock().clone()
        }

        /// Decodes captured `propose_bytes` calls into typed `RaftPayload<R>`.
        ///
        /// Panics if any captured bytes cannot be deserialized as `RaftPayload<R>`.
        /// Use this in tests to assert on the typed request after a service call:
        ///
        /// ```no_run
        /// # use inferadb_ledger_raft::types::{SystemRequest, RaftPayload};
        /// # let mock = crate::proposal::mock::MockProposalService::new();
        /// let decoded = mock.decode_proposals::<SystemRequest>();
        /// assert!(matches!(decoded[0].request, SystemRequest::UpdateUser { .. }));
        /// ```
        #[allow(dead_code, clippy::panic)]
        pub(crate) fn decode_proposals<R: serde::de::DeserializeOwned + std::fmt::Debug>(
            &self,
        ) -> Vec<RaftPayload<R>> {
            self.proposals
                .lock()
                .iter()
                .map(|bytes| {
                    postcard::from_bytes::<RaftPayload<R>>(bytes).unwrap_or_else(|e| {
                        panic!("failed to decode proposal bytes as RaftPayload<R>: {e}")
                    })
                })
                .collect()
        }

        /// Decodes captured `propose_to_region_bytes` calls into typed `RaftPayload<R>`.
        ///
        /// Panics if any captured bytes cannot be deserialized as `RaftPayload<R>`.
        #[allow(dead_code, clippy::panic)]
        pub(crate) fn decode_regional_proposals<
            R: serde::de::DeserializeOwned + std::fmt::Debug,
        >(
            &self,
        ) -> Vec<(Region, RaftPayload<R>)> {
            self.regional_proposals
                .lock()
                .iter()
                .map(|(region, bytes)| {
                    let payload =
                        postcard::from_bytes::<RaftPayload<R>>(bytes).unwrap_or_else(|e| {
                            panic!(
                                "failed to decode regional proposal bytes as RaftPayload<R>: {e}"
                            )
                        });
                    (*region, payload)
                })
                .collect()
        }

        /// Returns the number of captured global proposals.
        pub(crate) fn proposals_len(&self) -> usize {
            self.proposals.lock().len()
        }

        /// Returns `true` if no global proposals have been captured.
        #[allow(dead_code)]
        pub(crate) fn proposals_is_empty(&self) -> bool {
            self.proposals.lock().is_empty()
        }
    }

    /// Builds a generic `Internal` `WireError` for the mock's "no response
    /// enqueued" path. Pulled out as a free fn so each impl arm reads the
    /// same way.
    fn mock_internal_err(message: &str) -> WireError {
        WireError::new(ErrorCode::Internal, message.to_string())
    }

    #[tonic::async_trait]
    impl ProposalService for MockProposalService {
        async fn propose_bytes(
            &self,
            bytes: Vec<u8>,
            _timeout: Duration,
        ) -> Result<LedgerResponse, WireError> {
            self.proposals.lock().push(bytes);
            self.responses
                .lock()
                .pop_front()
                .unwrap_or_else(|| Err(mock_internal_err("no mock response enqueued")))
        }

        async fn propose_to_region_bytes(
            &self,
            region: Region,
            bytes: Vec<u8>,
            _timeout: Duration,
        ) -> Result<LedgerResponse, WireError> {
            self.regional_proposals.lock().push((region, bytes));
            self.regional_responses
                .lock()
                .pop_front()
                .unwrap_or_else(|| Err(mock_internal_err("no mock regional response enqueued")))
        }

        async fn propose_to_organization_bytes(
            &self,
            region: Region,
            organization: OrganizationId,
            bytes: Vec<u8>,
            _timeout: Duration,
        ) -> Result<LedgerResponse, WireError> {
            self.organization_proposals.lock().push((region, organization, bytes));
            self.organization_responses
                .lock()
                .pop_front()
                .unwrap_or_else(|| Err(mock_internal_err("no mock organization response enqueued")))
        }

        async fn propose_to_vault_bytes(
            &self,
            region: Region,
            organization: OrganizationId,
            vault_id: VaultId,
            _organization_slug: Option<u64>,
            _vault_slug: Option<u64>,
            bytes: Vec<u8>,
            _timeout: Duration,
        ) -> Result<LedgerResponse, WireError> {
            self.vault_proposals.lock().push((region, organization, vault_id, bytes));
            self.vault_responses
                .lock()
                .pop_front()
                .unwrap_or_else(|| Err(mock_internal_err("no mock vault response enqueued")))
        }

        async fn propose_organization_request_to_vault(
            &self,
            region: Region,
            organization: OrganizationId,
            vault_id: VaultId,
            _organization_slug: Option<u64>,
            _vault_slug: Option<u64>,
            request: OrganizationRequest,
            caller: u64,
            _timeout: Duration,
        ) -> Result<LedgerResponse, WireError> {
            self.typed_vault_proposals.lock().push((
                region,
                organization,
                vault_id,
                request,
                caller,
            ));
            self.typed_vault_responses
                .lock()
                .pop_front()
                .unwrap_or_else(|| Err(mock_internal_err("no mock typed-vault response enqueued")))
        }

        fn regional_state(
            &self,
            region: Region,
        ) -> Result<Arc<StateLayer<FileBackend>>, WireError> {
            self.regional_state_layer.lock().clone().ok_or_else(|| {
                WireError::new(
                    ErrorCode::FailedPrecondition,
                    format!("MockProposalService: no regional state configured for {region}"),
                )
            })
        }

        fn raft_metrics(&self) -> Option<super::LedgerRaftMetrics> {
            None
        }
    }

    #[cfg(test)]
    #[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
    mod tests {
        use inferadb_ledger_raft::types::SystemRequest;

        use super::*;

        #[tokio::test]
        async fn mock_propose_returns_enqueued_response() {
            let mock = MockProposalService::new();
            mock.enqueue(Ok(LedgerResponse::Empty));

            let payload = inferadb_ledger_raft::types::RaftPayload::new(
                SystemRequest::VerifyUserEmail {
                    email_id: inferadb_ledger_types::UserEmailId::new(1),
                },
                42,
            );
            let bytes = postcard::to_allocvec(&payload).unwrap();

            let result = mock.propose_bytes(bytes, Duration::from_secs(5)).await;

            assert!(result.is_ok());
            assert_eq!(mock.proposals_len(), 1);

            // Decode and verify caller is preserved.
            let decoded = mock.decode_proposals::<SystemRequest>();
            assert_eq!(decoded[0].caller, 42);
        }

        #[tokio::test]
        async fn mock_propose_returns_error_when_empty() {
            let mock = MockProposalService::new();

            let payload =
                inferadb_ledger_raft::types::RaftPayload::system(SystemRequest::VerifyUserEmail {
                    email_id: inferadb_ledger_types::UserEmailId::new(1),
                });
            let bytes = postcard::to_allocvec(&payload).unwrap();

            let result = mock.propose_bytes(bytes, Duration::from_secs(5)).await;

            assert!(result.is_err());
            assert_eq!(result.unwrap_err().code, inferadb_ledger_wire::ErrorCode::Internal);
        }

        #[tokio::test]
        async fn mock_regional_state_returns_error_when_unconfigured() {
            let mock = MockProposalService::new();
            let result = mock.regional_state(Region::GLOBAL);
            let err = result.err().expect("expected error");
            assert_eq!(err.code, ErrorCode::FailedPrecondition);
        }

        #[test]
        fn mock_raft_metrics_returns_none() {
            let mock = MockProposalService::new();
            assert!(mock.raft_metrics().is_none());
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use inferadb_ledger_raft::raft_manager::{RaftManagerConfig, RegionConfig};
    use inferadb_ledger_test_utils::TestDir;

    use super::*;

    async fn create_raft_proposal_service() -> (Arc<RaftProposalService>, Arc<RaftManager>, TestDir)
    {
        let temp = TestDir::new();
        let node_id = 1u64;
        let config = RaftManagerConfig::new(temp.path().to_path_buf(), node_id, Region::GLOBAL);
        let manager = Arc::new(RaftManager::new(
            config,
            Arc::new(inferadb_ledger_raft::node_registry::NodeConnectionRegistry::new()),
        ));
        let region_config =
            RegionConfig::system(node_id, "127.0.0.1:0".to_string()).without_background_jobs();
        let system = manager.start_system_region(region_config).await.expect("start system region");

        let svc =
            Arc::new(RaftProposalService::new(system.handle().clone(), Some(manager.clone())));
        (svc, manager, temp)
    }

    #[tokio::test]
    async fn regional_state_returns_state_layer_for_valid_region() {
        let (svc, _manager, _temp) = create_raft_proposal_service().await;

        let state = svc.regional_state(Region::GLOBAL).expect("GLOBAL should be available");
        assert!(Arc::strong_count(&state) >= 1);
    }

    #[tokio::test]
    async fn regional_state_returns_unavailable_for_unknown_region() {
        let (svc, _manager, _temp) = create_raft_proposal_service().await;

        match svc.regional_state(Region::US_EAST_VA) {
            // Wire-side analogue of gRPC `Unavailable` (see
            // `wire_helpers::wire_code_to_tonic`).
            Err(err) => assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::StaleRouting),
            Ok(_) => panic!("Expected StaleRouting error"),
        }
    }

    #[tokio::test]
    async fn regional_state_without_manager_returns_failed_precondition() {
        let temp = TestDir::new();
        let node_id = 1u64;
        let config = RaftManagerConfig::new(temp.path().to_path_buf(), node_id, Region::GLOBAL);
        let manager = Arc::new(RaftManager::new(
            config,
            Arc::new(inferadb_ledger_raft::node_registry::NodeConnectionRegistry::new()),
        ));
        let region_config =
            RegionConfig::system(node_id, "127.0.0.1:0".to_string()).without_background_jobs();
        let system = manager.start_system_region(region_config).await.expect("start system region");

        let svc = RaftProposalService::new(system.handle().clone(), None);

        match svc.regional_state(Region::GLOBAL) {
            Err(err) => {
                assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::FailedPrecondition)
            },
            Ok(_) => panic!("Expected FailedPrecondition error"),
        }
    }

    fn make_system_bytes() -> Vec<u8> {
        let payload = inferadb_ledger_raft::types::RaftPayload::new(
            inferadb_ledger_raft::types::SystemRequest::VerifyUserEmail {
                email_id: inferadb_ledger_types::UserEmailId::new(1),
            },
            0,
        );
        postcard::to_allocvec(&payload).expect("serialize payload")
    }

    #[tokio::test]
    async fn propose_to_region_without_manager_returns_failed_precondition() {
        let temp = TestDir::new();
        let node_id = 1u64;
        let config = RaftManagerConfig::new(temp.path().to_path_buf(), node_id, Region::GLOBAL);
        let manager = Arc::new(RaftManager::new(
            config,
            Arc::new(inferadb_ledger_raft::node_registry::NodeConnectionRegistry::new()),
        ));
        let region_config =
            RegionConfig::system(node_id, "127.0.0.1:0".to_string()).without_background_jobs();
        let system = manager.start_system_region(region_config).await.expect("start system region");

        let svc = RaftProposalService::new(system.handle().clone(), None);

        let err = svc
            .propose_to_region_bytes(
                Region::US_EAST_VA,
                make_system_bytes(),
                Duration::from_secs(5),
            )
            .await
            .unwrap_err();

        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::FailedPrecondition);
    }

    #[tokio::test]
    async fn propose_to_region_unknown_region_returns_failed_precondition() {
        let (svc, manager, _temp) = create_raft_proposal_service().await;

        // Verify the region doesn't exist yet.
        assert!(!manager.has_region(Region::US_EAST_VA), "region should not exist before propose");

        // Unknown regions now return FAILED_PRECONDITION immediately — region
        // creation is an explicit admin operation, not a lazy side-effect.
        let err = svc
            .propose_to_region_bytes(
                Region::US_EAST_VA,
                make_system_bytes(),
                Duration::from_millis(200),
            )
            .await
            .unwrap_err();

        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::FailedPrecondition);
    }

    #[tokio::test]
    async fn raft_metrics_returns_some() {
        let (svc, _manager, _temp) = create_raft_proposal_service().await;
        assert!(svc.raft_metrics().is_some());
    }

    #[tokio::test]
    async fn propose_organization_request_to_vault_unknown_vault_returns_unavailable() {
        let (svc, _manager, _temp) = create_raft_proposal_service().await;

        // No vault group registered for an arbitrary (region, org, vault) tuple
        // — should return UNAVAILABLE so the SDK can reconnect.
        let request = inferadb_ledger_raft::types::OrganizationRequest::IngestExternalEvents {
            source: String::new(),
            events: Vec::new(),
        };
        let err = svc
            .propose_organization_request_to_vault(
                Region::GLOBAL,
                inferadb_ledger_types::OrganizationId::new(99),
                inferadb_ledger_types::VaultId::new(7),
                None,
                None,
                request,
                0,
                Duration::from_millis(200),
            )
            .await
            .unwrap_err();

        // Unknown vault on a configured manager surfaces as `StaleRouting`
        // — the wire-level analogue of gRPC `Unavailable` (see
        // `wire_helpers::wire_code_to_tonic`).
        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::StaleRouting);
    }

    #[tokio::test]
    async fn propose_organization_request_to_vault_without_manager_returns_failed_precondition() {
        let temp = TestDir::new();
        let node_id = 1u64;
        let config = RaftManagerConfig::new(temp.path().to_path_buf(), node_id, Region::GLOBAL);
        let manager = Arc::new(RaftManager::new(
            config,
            Arc::new(inferadb_ledger_raft::node_registry::NodeConnectionRegistry::new()),
        ));
        let region_config =
            RegionConfig::system(node_id, "127.0.0.1:0".to_string()).without_background_jobs();
        let system = manager.start_system_region(region_config).await.expect("start system region");

        let svc = RaftProposalService::new(system.handle().clone(), None);

        let request = inferadb_ledger_raft::types::OrganizationRequest::IngestExternalEvents {
            source: String::new(),
            events: Vec::new(),
        };
        let err = svc
            .propose_organization_request_to_vault(
                Region::US_EAST_VA,
                inferadb_ledger_types::OrganizationId::new(1),
                inferadb_ledger_types::VaultId::new(1),
                None,
                None,
                request,
                0,
                Duration::from_millis(200),
            )
            .await
            .unwrap_err();

        assert_eq!(err.code, inferadb_ledger_wire::ErrorCode::FailedPrecondition);
    }
}
