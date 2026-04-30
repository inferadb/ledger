//! Admin service implementation.
//!
//! Handles cluster membership, snapshots, integrity checks, vault recovery,
//! runtime configuration, backup/restore, and cryptographic key operations.
//! Organization lifecycle is handled by [`super::OrganizationService`].
//! Vault CRUD is handled by [`super::VaultService`].
//!
//! The wire-trait handlers live in [`super::admin_wire`] and operate on
//! wire types directly (no proto round-trip). This module owns the shared
//! [`AdminService`] struct, builder, and a small handful of helpers used
//! by the wire-trait handlers (vault summary projection, vault block hash
//! computation, manifest re-shaping, cluster-id persistence).

use std::{sync::Arc, time::Duration};

use inferadb_ledger_raft::{
    ConsensusHandle, HandleError, VaultGroup,
    log_storage::AppliedStateAccessor,
    logging::{OperationType, RequestContext, Sampler},
    types::{LedgerResponse, RaftPayload, SystemRequest},
};
use inferadb_ledger_state::{BlockArchive, StateLayer};
use inferadb_ledger_store::FileBackend;
#[cfg(test)]
use inferadb_ledger_types::{
    OrganizationId as DomainOrganizationId, VaultId as DomainVaultId, ZERO_HASH,
};
use inferadb_ledger_types::{VaultEntry, config::ValidationConfig};
use inferadb_ledger_wire::services::{admin as w, shared as ws};
use sha2::{Digest, Sha256};
use tonic::Status;

/// gRPC handler for cluster administration operations.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct AdminService {
    /// Consensus handle for proposing admin operations and leadership checks.
    pub(super) handle: Arc<ConsensusHandle>,
    /// State layer for entity and relationship reads during admin operations.
    pub(super) state: Arc<StateLayer<FileBackend>>,
    /// Accessor for applied state (vault heights, health).
    pub(super) applied_state: AppliedStateAccessor,
    /// Block archive for integrity verification.
    #[builder(default)]
    pub(super) block_archive: Option<Arc<BlockArchive<FileBackend>>>,
    /// The address other nodes should use to reach this node.
    ///
    /// Set from `--advertise` (or `--listen` as fallback). Used in
    /// `GetNodeInfo` responses and data region initial_members.
    #[builder(into)]
    pub(super) advertise_addr: String,
    /// Sampler for log tail sampling.
    #[builder(default)]
    pub(super) sampler: Option<Sampler>,
    /// Node ID for logging system context.
    #[builder(default)]
    pub(super) node_id: Option<u64>,
    /// Input validation configuration for request field limits.
    #[builder(default = Arc::new(ValidationConfig::default()))]
    pub(super) validation_config: Arc<ValidationConfig>,
    /// Maximum time to wait for a Raft proposal to commit.
    ///
    /// If a gRPC deadline is shorter, the deadline takes precedence.
    #[builder(default = Duration::from_secs(30))]
    pub(super) proposal_timeout: Duration,
    /// Runtime configuration handle for hot-reloadable settings.
    #[builder(default)]
    pub(super) runtime_config: Option<inferadb_ledger_raft::runtime_config::RuntimeConfigHandle>,
    /// Rate limiter for propagating config changes.
    #[builder(default)]
    pub(super) rate_limiter: Option<Arc<inferadb_ledger_raft::rate_limit::RateLimiter>>,
    /// Hot key detector for propagating config changes.
    #[builder(default)]
    pub(super) hot_key_detector:
        Option<Arc<inferadb_ledger_raft::hot_key_detector::HotKeyDetector>>,
    /// Backup manager for backup and restore operations.
    ///
    /// Must be constructed via [`BackupManager::with_data_dir`] for the
    /// archive-based RPC path; the legacy snapshot path is gone.
    #[builder(default)]
    pub(super) backup_manager: Option<Arc<inferadb_ledger_raft::backup::BackupManager>>,
    /// Region key manager — supplies the local node's RMK fingerprint for
    /// stamping outgoing archives and validating incoming archives at
    /// restore-stage time.
    #[builder(default)]
    pub(super) key_manager: Option<Arc<dyn inferadb_ledger_store::crypto::RegionKeyManager>>,
    /// Backup root directory — every archive lives at
    /// `{backups_dir}/backup-{id}.tar.zst`. Mirrors the path the
    /// [`backup_manager`] writes through; held here so [`restore_backup`]
    /// can resolve a `backup_id` to an archive path without round-tripping
    /// through the manager.
    #[builder(default)]
    pub(super) backups_dir: Option<std::path::PathBuf>,
    /// Handler-phase event handle for recording denial events.
    #[builder(default)]
    pub(super) event_handle:
        Option<inferadb_ledger_raft::event_writer::EventHandle<inferadb_ledger_store::FileBackend>>,
    /// Health state for drain-phase write rejection.
    #[builder(default)]
    pub(super) health_state: Option<inferadb_ledger_raft::graceful_shutdown::HealthState>,
    /// Lock to prevent concurrent leader transfer attempts.
    #[builder(default = Arc::new(std::sync::atomic::AtomicBool::new(false)))]
    pub(super) transfer_lock: Arc<std::sync::atomic::AtomicBool>,
    /// Shared DEK re-wrapping progress (read by `GetRewrapStatus`).
    #[builder(default)]
    pub(super) rewrap_progress: Option<Arc<inferadb_ledger_raft::dek_rewrap::RewrapProgress>>,
    /// Raft manager for lazy region provisioning.
    #[builder(default)]
    pub(super) raft_manager: Option<Arc<inferadb_ledger_raft::raft_manager::RaftManager>>,
    /// Shared peer address map for resolving peer network addresses.
    ///
    /// Used by admin RPCs that need to contact specific peers.
    #[builder(default)]
    pub(super) peer_addresses: Option<inferadb_ledger_raft::PeerAddressMap>,
    /// GLOBAL region consensus transport for registering new peer channels
    /// during dynamic cluster membership changes (JoinCluster/LeaveCluster).
    #[builder(default)]
    pub(super) consensus_transport:
        Option<inferadb_ledger_raft::raft_manager::ConsensusTransportImpl>,
    /// Initialization signal sender for fresh (uninitialized) nodes.
    ///
    /// When present, `init_cluster()` generates a cluster ID, persists it to
    /// `init_data_dir`, and sends `Some(cluster_id)` through this channel to
    /// unblock bootstrap. When absent (restart path), `init_cluster()` returns
    /// `already_initialized = true`.
    #[builder(default)]
    pub(super) init_sender: Option<Arc<tokio::sync::watch::Sender<Option<u64>>>>,
    /// Data directory path for cluster ID persistence (used by `init_cluster`).
    #[builder(default)]
    pub(super) init_data_dir: Option<std::path::PathBuf>,
    /// Static cluster ID for restarted (already-initialized) nodes.
    ///
    /// Set during bootstrap for the restart path. For fresh nodes, this is `None`
    /// and `get_node_info` reads the cluster ID from the `init_sender` channel.
    #[builder(default)]
    pub(super) cluster_id: Option<u64>,
    /// Per-node liveness timestamps. Updated on every successful RPC or Raft message.
    /// Shared with the Raft service and bootstrap liveness checker.
    #[builder(default)]
    pub(super) peer_liveness:
        Option<Arc<parking_lot::RwLock<std::collections::HashMap<u64, std::time::Instant>>>>,
}

impl AdminService {
    /// Attaches input validation configuration for request field limits.
    #[must_use]
    pub fn with_validation_config(mut self, config: Arc<ValidationConfig>) -> Self {
        self.validation_config = config;
        self
    }

    /// Sets the maximum time to wait for Raft proposals.
    #[must_use]
    pub fn with_proposal_timeout(mut self, timeout: Duration) -> Self {
        self.proposal_timeout = timeout;
        self
    }

    /// Attaches the runtime configuration handle for hot-reloadable settings.
    #[must_use]
    pub fn with_runtime_config(
        mut self,
        handle: inferadb_ledger_raft::runtime_config::RuntimeConfigHandle,
        rate_limiter: Option<Arc<inferadb_ledger_raft::rate_limit::RateLimiter>>,
        hot_key_detector: Option<Arc<inferadb_ledger_raft::hot_key_detector::HotKeyDetector>>,
    ) -> Self {
        self.runtime_config = Some(handle);
        self.rate_limiter = rate_limiter;
        self.hot_key_detector = hot_key_detector;
        self
    }

    /// Attaches the backup manager for the multi-DB archive
    /// backup/restore RPCs.
    ///
    /// The legacy snapshot-based path is gone; the backup manager must
    /// be constructed via
    /// [`BackupManager::with_data_dir`](inferadb_ledger_raft::backup::BackupManager::with_data_dir)
    /// so the archive path can enumerate per-org and per-vault DB
    /// files. Pair this with [`Self::with_key_manager`] +
    /// [`Self::with_backups_dir`] at server-bootstrap time.
    #[must_use]
    pub fn with_backup(
        mut self,
        backup_manager: Arc<inferadb_ledger_raft::backup::BackupManager>,
    ) -> Self {
        self.backup_manager = Some(backup_manager);
        self
    }

    /// Attaches the region key manager — required for the multi-DB
    /// archive backup path so the manifest can stamp the local node's
    /// RMK fingerprint on outgoing archives and pre-flight check
    /// incoming archives at restore-stage time.
    #[must_use]
    pub fn with_key_manager(
        mut self,
        key_manager: Arc<dyn inferadb_ledger_store::crypto::RegionKeyManager>,
    ) -> Self {
        self.key_manager = Some(key_manager);
        self
    }

    /// Attaches the backups root directory — used by [`restore_backup`] to
    /// resolve a `backup_id` to an archive path on disk.
    #[must_use]
    pub fn with_backups_dir(mut self, backups_dir: std::path::PathBuf) -> Self {
        self.backups_dir = Some(backups_dir);
        self
    }

    /// Attaches the handler-phase event handle for recording denial events.
    #[must_use]
    pub fn with_event_handle(
        mut self,
        handle: inferadb_ledger_raft::event_writer::EventHandle<inferadb_ledger_store::FileBackend>,
    ) -> Self {
        self.event_handle = Some(handle);
        self
    }

    /// Attaches health state for drain-phase write rejection.
    #[must_use]
    pub fn with_health_state(
        mut self,
        health_state: inferadb_ledger_raft::graceful_shutdown::HealthState,
    ) -> Self {
        self.health_state = Some(health_state);
        self
    }

    /// Attaches the Raft manager for lazy region provisioning.
    #[must_use]
    pub fn with_raft_manager(
        mut self,
        manager: Arc<inferadb_ledger_raft::raft_manager::RaftManager>,
    ) -> Self {
        self.raft_manager = Some(manager);
        self
    }

    /// Attaches the GLOBAL consensus transport for peer channel management.
    ///
    /// When set, `JoinCluster` and `LeaveCluster` will register/unregister
    /// gRPC channels for the affected nodes, enabling Raft replication to
    /// dynamically-added cluster members.
    #[must_use]
    pub fn with_consensus_transport(
        mut self,
        transport: inferadb_ledger_raft::raft_manager::ConsensusTransportImpl,
    ) -> Self {
        self.consensus_transport = Some(transport);
        self
    }

    /// Attaches the initialization signal sender and data directory.
    ///
    /// Used on fresh (uninitialized) nodes so the `InitCluster` RPC can generate
    /// a cluster ID, persist it, and signal bootstrap to proceed.
    #[must_use]
    pub fn with_init_sender(
        mut self,
        sender: Arc<tokio::sync::watch::Sender<Option<u64>>>,
        data_dir: Option<std::path::PathBuf>,
    ) -> Self {
        self.init_sender = Some(sender);
        self.init_data_dir = data_dir;
        self
    }

    /// Sets the static cluster ID for already-initialized nodes.
    ///
    /// On the restart path, the cluster ID is loaded from disk during bootstrap
    /// and passed here so `get_node_info` and `init_cluster` can return it
    /// without needing the init sender channel.
    #[must_use]
    pub fn with_cluster_id(mut self, cluster_id: u64) -> Self {
        self.cluster_id = Some(cluster_id);
        self
    }

    /// Attaches a shared peer liveness map for quorum-based dead node detection.
    #[must_use]
    pub fn with_peer_liveness(
        mut self,
        liveness: Arc<parking_lot::RwLock<std::collections::HashMap<u64, std::time::Instant>>>,
    ) -> Self {
        self.peer_liveness = Some(liveness);
        self
    }

    /// Wire-shaped request-context constructor used by every wire-trait
    /// handler. Mirrors the canonical-log-line setup the tonic admin impl
    /// used (operation type, admin action, sampler, node id) but reads the
    /// trace context directly from the dispatcher's wire `RequestContext`.
    pub(super) fn make_request_context_unified_wire(
        &self,
        method: &'static str,
        wire_ctx: &inferadb_ledger_wire::RequestContext,
    ) -> RequestContext {
        let event_handle: Option<Arc<dyn inferadb_ledger_raft::event_writer::EventEmitter>> =
            self.event_handle.as_ref().map(|h| Arc::new(h.clone()) as _);
        let mut ctx =
            RequestContext::from_wire_context("AdminService", method, wire_ctx, event_handle);
        ctx.set_operation_type(OperationType::Admin);
        ctx.set_admin_action(method);
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }
        ctx
    }

    /// Proposes a `SystemRequest` through Raft with deadline handling and
    /// returns a wire `WireError` on failure.
    ///
    /// Handles timeout computation, Raft proposal submission, and error
    /// classification (leadership errors → `Unavailable`, timeout →
    /// `DeadlineExceeded`, others → `Internal`).
    pub(super) async fn propose_raft_request_wire(
        &self,
        request: SystemRequest,
        wire_ctx: &inferadb_ledger_wire::RequestContext,
        ctx: &mut RequestContext,
    ) -> Result<LedgerResponse, inferadb_ledger_wire::WireError> {
        let timeout = match wire_ctx.deadline {
            Some(instant) => instant
                .checked_duration_since(std::time::Instant::now())
                .unwrap_or(Duration::ZERO)
                .min(self.proposal_timeout),
            None => self.proposal_timeout,
        };
        let timeout = if timeout.is_zero() { self.proposal_timeout } else { timeout };
        let payload = RaftPayload::system(request);

        match self.handle.propose_and_wait(payload, timeout).await {
            Ok(response) => Ok(response),
            Err(HandleError::Consensus { source, .. }) => {
                ctx.set_error("RaftError", &source.to_string());
                Err(crate::proposal::consensus_error_to_wire_error(source))
            },
            Err(HandleError::Timeout { .. }) => {
                inferadb_ledger_raft::metrics::record_raft_proposal_timeout();
                ctx.set_error("Timeout", "Raft proposal timed out");
                Err(inferadb_ledger_wire::WireError::new(
                    inferadb_ledger_wire::ErrorCode::FailedPrecondition,
                    format!("Raft proposal timed out after {}ms", timeout.as_millis()),
                ))
            },
            Err(e) => {
                ctx.set_error("RaftError", &e.to_string());
                Err(inferadb_ledger_wire::WireError::new(
                    inferadb_ledger_wire::ErrorCode::Internal,
                    e.to_string(),
                ))
            },
        }
    }
}

/// Projects a vault `unix_secs` timestamp to UNIX nanoseconds.
///
/// Returns `None` when `secs == 0` — the sentinel for "never observed
/// activity" / "no membership change pending" used by
/// [`InnerVaultGroup::last_activity_unix_secs`] and
/// [`InnerVaultGroup::pending_membership_started_unix_secs`].
pub(super) fn unix_secs_to_ns_opt(secs: u64) -> Option<u64> {
    if secs == 0 { None } else { Some(secs.saturating_mul(1_000_000_000)) }
}

impl AdminService {
    /// Builds the operator-facing wire [`AdminVaultInfo`] summary from a
    /// vault group.
    ///
    /// Pulls the vault's external slug from the applied-state slug index;
    /// when the slug is missing (extremely unlikely outside of a deletion
    /// race) the field falls back to slug `0`. Lifecycle state /
    /// last-activity come from the per-vault [`InnerVaultGroup`] state;
    /// voter / learner counts and the leader come from the consensus
    /// handle's `ShardState` snapshot.
    pub(super) fn vault_info_from_group(&self, group: &VaultGroup) -> w::AdminVaultInfo {
        let region = group.region();
        let organization_id = group.organization_id();
        let vault_id = group.vault_id();

        let slug = self
            .applied_state
            .resolve_vault_id_to_slug(organization_id, vault_id)
            .map(|s| s.value())
            .unwrap_or(0);

        let shard_state = group.handle().shard_state();
        let lifecycle = group.lifecycle_state();
        let last_activity_secs = group.last_activity_unix_secs();
        let last_applied_index =
            group.vault_applied_state().load().last_applied.map_or(0, |id| id.index);

        // Reference `region` so static analysis sees the binding even
        // when no future field is added — keeps the helper grep-friendly
        // for future operators that want to surface the region in the
        // summary view (it is already visible on `ShowVaultResponse`).
        let _ = region;

        w::AdminVaultInfo {
            slug: Some(ws::VaultSlug::new(slug)),
            status: lifecycle.as_metric_label().to_owned(),
            leader_node_id: shard_state.leader.map_or(0, |n| n.0),
            voter_count: shard_state.voters.len() as u64,
            learner_count: shard_state.learners.len() as u64,
            last_applied_index,
            last_activity: unix_secs_to_ns_opt(last_activity_secs),
        }
    }
}

/// Computes the hash of a vault block entry for chain verification.
///
/// The vault block hash commits to all content: height, previous hash,
/// transactions (via tx_merkle_root), and state root.
pub(super) fn compute_vault_block_hash(entry: &VaultEntry) -> [u8; 32] {
    let mut hasher = Sha256::new();

    // Hash the vault block header fields
    hasher.update(entry.organization.value().to_le_bytes());
    hasher.update(entry.vault.value().to_le_bytes());
    hasher.update(entry.vault_height.to_le_bytes());
    hasher.update(entry.previous_vault_hash);
    hasher.update(entry.tx_merkle_root);
    hasher.update(entry.state_root);

    hasher.finalize().into()
}

/// Converts a domain
/// [`inferadb_ledger_raft::backup::archive::BackupManifest`] into its wire
/// representation for inclusion in
/// [`CreateBackupResponse`](w::CreateBackupResponse) /
/// [`RestoreBackupResponse`](w::RestoreBackupResponse) /
/// [`BackupInfo`](w::BackupInfo).
///
/// The wire mirror is intentionally a flat copy — the conversion only
/// re-shapes types (`Region` slug → string, `OrganizationSlug` → wire
/// newtype, `Vec<DbEntry>` → `Vec<BackupDbEntry>`). No validation logic
/// lives here; a manifest that round-trips through this helper carries
/// the same byte values as the on-disk JSON manifest member.
pub(super) fn backup_manifest_to_wire(
    manifest: &inferadb_ledger_raft::backup::archive::BackupManifest,
) -> w::BackupManifest {
    let dbs = manifest
        .dbs
        .iter()
        .map(|entry| w::BackupDbEntry {
            path: entry.path.clone(),
            size_bytes: entry.size_bytes,
            checksum: entry.checksum.clone(),
        })
        .collect();

    w::BackupManifest {
        schema_version: manifest.schema_version,
        format: manifest.format.clone(),
        region: manifest.region.as_str().to_string(),
        organization: Some(ws::OrganizationSlug::new(manifest.organization_slug.value())),
        organization_id: manifest.organization_id.value(),
        timestamp_micros: manifest.timestamp_micros,
        rmk_fingerprint: manifest.rmk_fingerprint.clone(),
        node_id_at_creation: manifest.node_id_at_creation,
        dbs,
        vault_count: manifest.vault_count,
        created_by_app_version: manifest.created_by_app_version.clone(),
    }
}

/// Persists a cluster ID to `{data_dir}/cluster_id`.
///
/// Thin wrapper matching the format used by the server crate's `cluster_id`
/// module. Duplicated here because the services crate cannot depend on the
/// server crate.
pub(super) fn write_cluster_id_to_disk(
    data_dir: &std::path::Path,
    cluster_id: u64,
) -> Result<(), Status> {
    let path = data_dir.join("cluster_id");
    if let Some(parent) = path.parent()
        && !parent.exists()
    {
        std::fs::create_dir_all(parent)
            .map_err(|e| Status::internal(format!("failed to create cluster_id directory: {e}")))?;
    }
    std::fs::write(&path, cluster_id.to_string())
        .map_err(|e| Status::internal(format!("failed to persist cluster_id: {e}")))?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use super::*;

    // =========================================================================
    // compute_vault_block_hash Tests
    // =========================================================================

    #[test]
    fn vault_block_hash_deterministic() {
        // Same input should always produce the same hash
        let entry = VaultEntry {
            organization: DomainOrganizationId::new(1),
            vault: DomainVaultId::new(2),
            vault_height: 10,
            previous_vault_hash: [0u8; 32],
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [0u8; 32],
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let hash1 = compute_vault_block_hash(&entry);
        let hash2 = compute_vault_block_hash(&entry);

        assert_eq!(hash1, hash2, "Hash must be deterministic");
    }

    #[test]
    fn vault_block_hash_different_for_different_inputs() {
        let entry1 = VaultEntry {
            organization: DomainOrganizationId::new(1),
            vault: DomainVaultId::new(2),
            vault_height: 10,
            previous_vault_hash: [0u8; 32],
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [0u8; 32],
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let entry2 = VaultEntry {
            organization: DomainOrganizationId::new(1),
            vault: DomainVaultId::new(2),
            vault_height: 11, // Different height
            previous_vault_hash: [0u8; 32],
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [0u8; 32],
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let hash1 = compute_vault_block_hash(&entry1);
        let hash2 = compute_vault_block_hash(&entry2);

        assert_ne!(hash1, hash2, "Different inputs should produce different hashes");
    }

    #[test]
    fn vault_block_hash_different_state_root() {
        let entry1 = VaultEntry {
            organization: DomainOrganizationId::new(1),
            vault: DomainVaultId::new(2),
            vault_height: 10,
            previous_vault_hash: [0u8; 32],
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [0u8; 32],
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let entry2 = VaultEntry {
            organization: DomainOrganizationId::new(1),
            vault: DomainVaultId::new(2),
            vault_height: 10,
            previous_vault_hash: [0u8; 32],
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [1u8; 32], // Different state root,

            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let hash1 = compute_vault_block_hash(&entry1);
        let hash2 = compute_vault_block_hash(&entry2);

        assert_ne!(hash1, hash2, "Different state_root should produce different hash");
    }

    #[test]
    fn vault_block_hash_chain_continuity() {
        // Simulate a chain of blocks
        let entry1 = VaultEntry {
            organization: DomainOrganizationId::new(1),
            vault: DomainVaultId::new(1),
            vault_height: 1,
            previous_vault_hash: ZERO_HASH, // Genesis block
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [1u8; 32],
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let hash1 = compute_vault_block_hash(&entry1);

        let entry2 = VaultEntry {
            organization: DomainOrganizationId::new(1),
            vault: DomainVaultId::new(1),
            vault_height: 2,
            previous_vault_hash: hash1, // Chain to previous block
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [2u8; 32],
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let hash2 = compute_vault_block_hash(&entry2);

        // Verify the hash commits to the chain
        assert_ne!(hash1, hash2);

        // If we create entry2 with wrong previous_vault_hash, it should differ
        let entry2_wrong = VaultEntry {
            organization: DomainOrganizationId::new(1),
            vault: DomainVaultId::new(1),
            vault_height: 2,
            previous_vault_hash: ZERO_HASH, // Wrong previous hash
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [2u8; 32],
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let hash2_wrong = compute_vault_block_hash(&entry2_wrong);
        assert_ne!(
            hash2, hash2_wrong,
            "Different previous_vault_hash should produce different hash"
        );
    }

    #[test]
    fn vault_block_hash_includes_all_fields() {
        let base_entry = VaultEntry {
            organization: DomainOrganizationId::new(1),
            vault: DomainVaultId::new(2),
            vault_height: 3,
            previous_vault_hash: [4u8; 32],
            transactions: vec![],
            tx_merkle_root: [5u8; 32],
            state_root: [6u8; 32],
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let base_hash = compute_vault_block_hash(&base_entry);

        // Changing organization_id should change hash
        let mut modified = base_entry.clone();
        modified.organization = DomainOrganizationId::new(99);
        assert_ne!(compute_vault_block_hash(&modified), base_hash, "organization_id affects hash");

        // Changing vault_id should change hash
        let mut modified = base_entry.clone();
        modified.vault = DomainVaultId::new(99);
        assert_ne!(compute_vault_block_hash(&modified), base_hash, "vault_id affects hash");

        // Changing tx_merkle_root should change hash
        let mut modified = base_entry.clone();
        modified.tx_merkle_root = [99u8; 32];
        assert_ne!(compute_vault_block_hash(&modified), base_hash, "tx_merkle_root affects hash");
    }
}
