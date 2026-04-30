//! Read service implementation.
//!
//! Handles all read operations including verified reads, block queries,
//! and relationship/entity listing.
//!
//! ## Consistency Levels
//!
//! Reads support two consistency levels:
//! - **EVENTUAL** (default): Read from any replica. Fastest, may be slightly stale.
//! - **LINEARIZABLE**: Strong consistency via leader lease or ReadIndex protocol.
//!
//! ## Linearizable Read Path
//!
//! On the **leader**: Serve directly from local state. The leader lease provides
//! confidence the node is still the real leader (~50ns validity check), but reads
//! are served regardless since the leader's state is always up to date.
//!
//! On a **follower**: Use the ReadIndex protocol — ask the leader for its committed
//! index, then wait for the local applied index to catch up before serving. This
//! avoids forwarding the full request to the leader while still providing
//! linearizable guarantees.

use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use inferadb_ledger_raft::{
    log_storage::AppliedStateAccessor, logging::Sampler, pagination::PageTokenCodec,
    raft_manager::RaftManager, types::LedgerNodeId,
};
use inferadb_ledger_state::{BlockArchive, SnapshotManager, StateLayer};
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{OrganizationId, OrganizationSlug, VaultId, VaultSlug};
use inferadb_ledger_wire::services::shared as ws;
use tokio_stream::Stream;
use tonic::Status;
use tracing::{debug, warn};

use super::{
    region_resolver::{RegionContext, RegionResolver},
    slug_resolver::SlugResolver,
    wire_helpers::wire_error_to_tonic_status,
};

/// Validates that a read key does not target system-reserved prefixes.
///
/// System keys (prefixed with `_`) are internal infrastructure and must not be
/// readable through the public Read API. This mirrors the write-path validation
/// in [`inferadb_ledger_types::validation::validate_key`].
pub(super) fn validate_read_key(key: &str) -> Result<(), Status> {
    if key.starts_with('_') {
        return Err(Status::invalid_argument(
            "key must not start with '_' (reserved for system keys)",
        ));
    }
    Ok(())
}

/// Parses a relationship cursor string (`"resource#relation@subject"`) into a
/// [`Relationship`](inferadb_ledger_types::Relationship) for cursor-based pagination.
///
/// Returns `None` if the string does not contain both `#` and `@` separators.
pub(super) fn parse_relationship_cursor(
    cursor: &str,
) -> Option<inferadb_ledger_types::Relationship> {
    let hash_pos = cursor.find('#')?;
    let at_pos = cursor[hash_pos..].find('@')? + hash_pos;
    let resource = &cursor[..hash_pos];
    let relation = &cursor[hash_pos + 1..at_pos];
    let subject = &cursor[at_pos + 1..];
    Some(inferadb_ledger_types::Relationship::new(resource, relation, subject))
}

/// Validates a `(resource, relation, subject)` triple against a
/// [`ValidationConfig`](inferadb_ledger_types::config::ValidationConfig).
///
/// Returns `Status::invalid_argument` (carrying structured `ErrorDetails` via
/// `build_error_details`) on the first field that fails the character
/// whitelist, length bound, or non-empty check. Lives as a free function so
/// the tests module can exercise it without assembling a full `ReadService`.
fn validate_relationship_triple(
    resource: &str,
    relation: &str,
    subject: &str,
    config: &inferadb_ledger_types::config::ValidationConfig,
) -> Result<(), Status> {
    use inferadb_ledger_types::validation;
    for (value, field) in [(resource, "resource"), (relation, "relation"), (subject, "subject")] {
        if let Err(err) = validation::validate_relationship_string(value, field, config) {
            let mut context = std::collections::HashMap::new();
            context.insert("field".to_owned(), field.to_owned());
            let mut wire_err = super::error_details::build_error_details(
                inferadb_ledger_types::DiagnosticCode::AppInvalidArgument.as_u16(),
                false,
                None,
                context,
                Some(inferadb_ledger_types::DiagnosticCode::AppInvalidArgument.suggested_action()),
            );
            wire_err.code = inferadb_ledger_wire::ErrorCode::InvalidArgument;
            wire_err.message = err.to_string();
            return Err(super::wire_helpers::wire_error_to_tonic_status(wire_err));
        }
    }
    Ok(())
}

/// gRPC handler for read operations.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct ReadService {
    /// Region resolver for routing requests to the correct region's state.
    pub(super) resolver: Arc<dyn RegionResolver>,
    /// Multi-raft manager for creating forward clients to remote regions.
    #[builder(default)]
    pub(super) manager: Option<Arc<RaftManager>>,
    /// Snapshot manager for historical reads optimization.
    #[builder(default)]
    pub(super) snapshot_manager: Option<Arc<SnapshotManager>>,
    /// This node's ID for leadership checks.
    #[builder(default)]
    pub(super) node_id: Option<LedgerNodeId>,
    /// Page token codec for secure pagination (HMAC-protected).
    #[builder(default = PageTokenCodec::with_random_key())]
    pub(super) page_token_codec: PageTokenCodec,
    /// Sampler for log tail sampling.
    #[builder(default)]
    pub(super) sampler: Option<Sampler>,
    /// Node-level connection registry for opening a channel to the current
    /// leader during follower ReadIndex requests (linearizable reads).
    #[builder(default)]
    #[allow(dead_code)]
    pub(super) registry: Option<Arc<inferadb_ledger_raft::node_registry::NodeConnectionRegistry>>,
    /// Shared counter for active `WatchBlocks` streams across all connections.
    ///
    /// Incremented when a stream starts, decremented on drop via `StreamGuard`.
    #[builder(default = Arc::new(AtomicUsize::new(0)))]
    pub(super) active_streams: Arc<AtomicUsize>,
    /// Maximum concurrent `WatchBlocks` streams allowed.
    #[builder(default = 1000)]
    pub(super) max_streams: usize,
    /// Shared peer address map for resolving peer network addresses.
    ///
    /// Used by read forwarding to resolve the leader's address.
    #[builder(default)]
    pub(super) peer_addresses: Option<inferadb_ledger_raft::PeerAddressMap>,
    /// Input validation configuration for request field limits.
    ///
    /// Currently consumed by the `check_relationship` handler to validate the
    /// user-supplied `resource`, `relation`, and `subject` strings.
    #[builder(default = Arc::new(inferadb_ledger_types::config::ValidationConfig::default()))]
    pub(super) validation_config: Arc<inferadb_ledger_types::config::ValidationConfig>,
}

/// RAII guard that decrements the active stream counter on drop.
///
/// Wraps a `WatchBlocks` response stream so the counter is always
/// decremented when the client disconnects or the stream completes,
/// regardless of how the stream ends. The inner stream is boxed to
/// satisfy `Unpin` without requiring `pin_project`.
pub(super) struct StreamGuard {
    pub(super) inner: Pin<Box<dyn Stream<Item = Result<ws::BlockAnnouncement, Status>> + Send>>,
    pub(super) counter: Arc<AtomicUsize>,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Stream for StreamGuard {
    type Item = Result<ws::BlockAnnouncement, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl ReadService {
    /// Resolves organization and vault IDs from a request using the region resolver.
    ///
    /// Returns `(organization_id, vault_id, region_context)`.
    ///
    /// When a vault group is live for the resolved `(region, organization_id,
    /// vault_id)` triple, the returned [`RegionContext::block_archive`] is
    /// swapped from the parent organization's archive to the vault's own
    /// per-vault archive (`{data_dir}/{region}/{org_id}/state/vault-{vault_id}/blocks.db`).
    /// Vault apply appends blocks to the per-vault archive, so read paths must
    /// look up against the same handle. When no vault group is registered
    /// (manager unavailable or vault not yet started on this node) the parent
    /// org's archive is preserved — callers see `BlockNotFound` rather than a
    /// stale-routing failure.
    pub(super) fn resolve_org_vault(
        &self,
        organization: Option<OrganizationSlug>,
        vault: Option<VaultSlug>,
    ) -> Result<(OrganizationId, VaultId, RegionContext), Status> {
        let system = self.resolver.system_region()?;
        let organization_id = SlugResolver::new(system.applied_state.clone())
            .extract_and_resolve(organization)
            .map_err(wire_error_to_tonic_status)?;
        let mut region = self.resolver.resolve(organization_id)?;
        // Vault slug indexes are in GLOBAL applied state, not the data region's.
        let vault_id = SlugResolver::new(system.applied_state)
            .extract_and_resolve_vault(vault)
            .map_err(wire_error_to_tonic_status)?;
        if let Some(manager) = &self.manager
            && let Ok(vault_group) =
                manager.get_vault_group(region.region, organization_id, vault_id)
        {
            // Block archive is only required for the read path — vault
            // entity reads must hit the per-vault archive, not the org's.
            // Kept explicit at the call site so the helper stays minimal
            // and the write-side `attach_vault_group` callers don't pull
            // in archive lookup they don't need.
            region.block_archive = Arc::clone(vault_group.block_archive());
            // Pair `vault_applied_state` with `vault_applied_index_rx` —
            // see `RegionContext::attach_vault_group` for the invariant.
            region.attach_vault_group(&vault_group);
        }
        Ok((organization_id, vault_id, region))
    }

    /// Same as `resolve_org_vault` but ensures GLOBAL state is fresh first.
    pub(super) async fn resolve_org_vault_consistent(
        &self,
        organization: Option<OrganizationSlug>,
        vault: Option<VaultSlug>,
    ) -> Result<(OrganizationId, VaultId, RegionContext), Status> {
        super::helpers::ensure_global_consistency(self.manager.as_deref()).await;
        self.resolve_org_vault(organization, vault)
    }

    /// Attaches input validation configuration for request field limits.
    ///
    /// Used by [`check_relationship`](Self::check_relationship) to enforce the
    /// relationship-string character whitelist and length bound on the
    /// user-supplied `resource`, `relation`, and `subject`.
    #[must_use]
    pub fn with_validation_config(
        mut self,
        config: Arc<inferadb_ledger_types::config::ValidationConfig>,
    ) -> Self {
        self.validation_config = config;
        self
    }

    /// Validates `resource`, `relation`, and `subject` against the attached
    /// [`ValidationConfig`](inferadb_ledger_types::config::ValidationConfig).
    ///
    /// Returns `Status::invalid_argument` on the first field that fails the
    /// character whitelist, length bound, or non-empty check. The returned
    /// status carries structured [`ErrorDetails`] via `build_error_details`
    /// with `ErrorCode::InvalidArgument`.
    pub(super) fn validate_relationship_triple(
        &self,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<(), Status> {
        validate_relationship_triple(resource, relation, subject, &self.validation_config)
    }

    /// Checks if this node is the current Raft leader for the given region context.
    pub(super) fn is_leader_for(ctx: &RegionContext) -> bool {
        ctx.handle.is_leader()
    }

    /// Returns a `NotLeader` `Status` for a follower-served read, populated with
    /// the within-region leader's identity and endpoint when known.
    ///
    /// Region-scoped: passes `None` for the vault hint so the SDK falls back
    /// to the org-level cache entry.
    pub(super) fn not_leader_within_region(&self, ctx: &RegionContext, message: &str) -> Status {
        super::metadata::not_leader_status_from_handle(
            ctx.handle.as_ref(),
            self.peer_addresses.as_ref(),
            message,
            None,
            None,
            None,
        )
    }

    /// Determines how to serve a read based on consistency level.
    ///
    /// - Eventual/Unspecified: serve from local state (any node).
    /// - Linearizable on leader: serve directly (lease check is informational).
    /// - Linearizable on follower: ReadIndex protocol — ask leader for committed index, wait for
    ///   local apply, then serve from local state.
    ///
    /// `grpc_deadline` is the remaining time from the `grpc-timeout` header, used to bound
    /// the `CommittedIndex` RPC on the follower path.
    ///
    /// `organization` and `vault` are the original request slugs. When both
    /// are `Some`, the follower path scopes the `CommittedIndex` RPC and the
    /// apply wait to the per-vault Raft group; when both are `None`, the path
    /// falls back to the org-scoped region group. Mixing one with the other
    /// is a bug — the leader rejects partial vault scope with
    /// `InvalidArgument`.
    pub(super) async fn resolve_read_consistency(
        &self,
        ctx: &RegionContext,
        consistency: i32,
        grpc_deadline: Option<Duration>,
        organization: Option<OrganizationSlug>,
        vault: Option<VaultSlug>,
    ) -> Result<(), Status> {
        let _ = (grpc_deadline, organization, vault);
        // Wire `ReadConsistency`: 0 = Unspecified, 1 = Eventual, 2 = Linearizable.
        match consistency {
            0 | 1 => Ok(()),
            2 => {
                if Self::is_leader_for(ctx) {
                    // Leader path — lease validity is informational; serve either way.
                    // The lease renews on each apply, so a temporarily-expired lease
                    // just means we haven't applied recently (conservative, not wrong).
                    Ok(())
                } else {
                    // Follower path — the legacy tonic Raft `CommittedIndex`
                    // probe has been retired (F.1.f.2.B). Linearizable reads
                    // on followers must redirect to the leader; the SDK
                    // reconnects via the `LeaderHint` carried by the
                    // returned error.
                    Err(self.not_leader_within_region(
                        ctx,
                        "Linearizable reads require the leader; ReadIndex follower path retired",
                    ))
                }
            },
            _ => Ok(()),
        }
    }

    /// Fetches block header from archive for a given vault height.
    ///
    /// Returns None if the block is not found or archive is not available.
    pub(super) fn get_block_header(
        archive: &BlockArchive<FileBackend>,
        applied_state: &AppliedStateAccessor,
        organization: OrganizationId,
        vault: VaultId,
        vault_height: u64,
    ) -> Option<ws::BlockHeader> {
        // Height 0 means no blocks yet
        if vault_height == 0 {
            return None;
        }

        // Find the region height containing this vault block
        let region_height =
            archive.find_region_height(organization, vault, vault_height).ok().flatten()?;

        // Read the region block
        let region_block = archive.read_block(region_height).ok()?;

        // Find the vault entry
        let entry = region_block.vault_entries.iter().find(|e| {
            e.organization == organization && e.vault == vault && e.vault_height == vault_height
        })?;

        // Build wire block header
        let block_hash = inferadb_ledger_types::vault_entry_hash(entry);
        let timestamp_ns = u64::try_from(region_block.timestamp.timestamp().max(0))
            .unwrap_or(0)
            .saturating_mul(1_000_000_000)
            .saturating_add(u64::from(region_block.timestamp.timestamp_subsec_nanos()));
        Some(ws::BlockHeader {
            height: entry.vault_height,
            organization: Some(ws::OrganizationSlug::new(
                applied_state
                    .resolve_id_to_slug(entry.organization)
                    .map_or(entry.organization.value() as u64, |s| s.value()),
            )),
            vault: Some(ws::VaultSlug::new(
                applied_state
                    .resolve_vault_id_to_slug(entry.organization, entry.vault)
                    .map_or(0, |s| s.value()),
            )),
            previous_hash: Some(ws::Hash {
                value: bytes::Bytes::copy_from_slice(&entry.previous_vault_hash),
            }),
            tx_merkle_root: Some(ws::Hash {
                value: bytes::Bytes::copy_from_slice(&entry.tx_merkle_root),
            }),
            state_root: Some(ws::Hash { value: bytes::Bytes::copy_from_slice(&entry.state_root) }),
            timestamp: timestamp_ns,
            leader_id: Some(ws::NodeId::new(region_block.leader_id.to_string())),
            term: region_block.term,
            committed_index: region_block.committed_index,
            block_hash: Some(ws::Hash { value: bytes::Bytes::copy_from_slice(&block_hash) }),
        })
    }

    /// Returns block_hash and state_root for a vault at a given height.
    ///
    /// Returns (block_hash, state_root) or (None, None) if not found.
    pub(super) fn get_tip_hashes(
        archive: &BlockArchive<FileBackend>,
        organization: OrganizationId,
        vault: VaultId,
        vault_height: u64,
    ) -> (Option<ws::Hash>, Option<ws::Hash>) {
        // Find the region height containing this vault block
        let region_height =
            match archive.find_region_height(organization, vault, vault_height).ok().flatten() {
                Some(h) => h,
                None => return (None, None),
            };

        // Read the region block
        let region_block = match archive.read_block(region_height) {
            Ok(block) => block,
            Err(_) => return (None, None),
        };

        // Find the vault entry
        let entry = match region_block.vault_entries.iter().find(|e| {
            e.organization == organization && e.vault == vault && e.vault_height == vault_height
        }) {
            Some(e) => e,
            None => return (None, None),
        };

        // Compute block hash from vault entry
        let block_hash = inferadb_ledger_types::vault_entry_hash(entry);

        (
            Some(ws::Hash { value: bytes::Bytes::copy_from_slice(&block_hash) }),
            Some(ws::Hash { value: bytes::Bytes::copy_from_slice(&entry.state_root) }),
        )
    }

    /// Finds and loads the nearest snapshot for historical read optimization.
    ///
    /// Returns (start_height, snapshot_loaded):
    /// - If a suitable snapshot is found and loaded, returns (snapshot_vault_height + 1, true)
    /// - If no snapshot available or loading fails, returns (1, false)
    ///
    /// The snapshot state is loaded into temp_state for the specified vault.
    pub(super) fn load_nearest_snapshot_for_historical_read(
        &self,
        vault: VaultId,
        target_height: u64,
        temp_state: &StateLayer<FileBackend>,
    ) -> (u64, bool) {
        let snapshot_manager = match &self.snapshot_manager {
            Some(sm) => sm,
            None => return (1, false),
        };

        // List available snapshots
        let snapshots = match snapshot_manager.list_snapshots() {
            Ok(s) => s,
            Err(_) => return (1, false),
        };

        // Find the largest snapshot height where the vault's height is <= target
        for &region_height in snapshots.iter().rev() {
            let snapshot = match snapshot_manager.load(region_height) {
                Ok(s) => s,
                Err(_) => continue,
            };

            // Find the vault's height in this snapshot
            if let Some(vault_meta) = snapshot.header.vault_states.iter().find(|v| v.vault == vault)
                && vault_meta.vault_height <= target_height
            {
                // This snapshot is usable - load its entities into temp_state
                if let Some(entities) = snapshot.state.vault_entities.get(&vault) {
                    // Convert entities to SetEntity operations for replay
                    let operations: Vec<inferadb_ledger_types::Operation> = entities
                        .iter()
                        .map(|entity| {
                            // Entity.key is Vec<u8>, convert to String for Operation
                            let key = String::from_utf8_lossy(&entity.key).into_owned();
                            inferadb_ledger_types::Operation::SetEntity {
                                key,
                                value: entity.value.clone(),
                                condition: None, // No condition for snapshot restore
                                expires_at: if entity.expires_at == 0 {
                                    None
                                } else {
                                    Some(entity.expires_at)
                                },
                            }
                        })
                        .collect();

                    // Apply all entities at the snapshot height
                    if !operations.is_empty()
                        && let Err(e) =
                            temp_state.apply_operations(vault, &operations, vault_meta.vault_height)
                    {
                        debug!("Failed to restore entities from snapshot: {:?}", e);
                        return (1, false);
                    }
                }

                debug!(
                    region_height,
                    vault_height = vault_meta.vault_height,
                    "Loaded snapshot for historical read"
                );
                return (vault_meta.vault_height + 1, true);
            }
        }

        // No suitable snapshot found
        (1, false)
    }

    /// Builds a ChainProof linking blocks from trusted_height+1 to response_height.
    ///
    /// The ChainProof allows clients to verify that the response_height block
    /// is part of the canonical chain descending from their trusted_height.
    ///
    /// Returns None if:
    /// - Block archive is not available
    /// - trusted_height >= response_height (nothing to prove)
    /// - Any block in the range is not found
    pub(super) fn build_chain_proof(
        archive: &BlockArchive<FileBackend>,
        applied_state: &AppliedStateAccessor,
        organization: OrganizationId,
        vault: VaultId,
        trusted_height: u64,
        response_height: u64,
    ) -> Option<ws::ChainProof> {
        // Nothing to prove if trusted is at or past response
        if trusted_height >= response_height {
            return Some(ws::ChainProof { headers: vec![] });
        }

        // Collect headers from trusted_height+1 to response_height
        let mut headers = Vec::with_capacity((response_height - trusted_height) as usize);

        for height in (trusted_height + 1)..=response_height {
            let header =
                Self::get_block_header(archive, applied_state, organization, vault, height)?;
            headers.push(header);
        }

        Some(ws::ChainProof { headers })
    }

    /// Fetches historical block announcements from the block archive.
    ///
    /// Used by watch_blocks to replay committed blocks before streaming new ones.
    pub(super) fn fetch_historical_announcements(
        archive: &BlockArchive<FileBackend>,
        applied_state: &AppliedStateAccessor,
        organization: OrganizationId,
        vault: VaultId,
        start_height: u64,
        end_height: u64,
    ) -> Vec<ws::BlockAnnouncement> {
        let mut announcements = Vec::with_capacity((end_height - start_height + 1) as usize);

        for height in start_height..=end_height {
            // Find the region height containing this vault block
            let region_height = match archive.find_region_height(organization, vault, height) {
                Ok(Some(h)) => h,
                Ok(None) => {
                    debug!(height, "Vault block not found in archive");
                    continue;
                },
                Err(e) => {
                    warn!(height, error = %e, "Error finding region height");
                    continue;
                },
            };

            // Read the region block
            let region_block = match archive.read_block(region_height) {
                Ok(block) => block,
                Err(e) => {
                    warn!(region_height, error = %e, "Error reading region block");
                    continue;
                },
            };

            // Find the vault entry in the region block
            if let Some(entry) = region_block.vault_entries.iter().find(|e| {
                e.organization == organization && e.vault == vault && e.vault_height == height
            }) {
                // Compute vault block hash using the same function as get_tip_hashes
                let block_hash = inferadb_ledger_types::vault_entry_hash(entry);

                // γ Phase 3a: prefer the slug stamped on the entry itself.
                // Fall back to `applied_state` map lookups (historical blocks
                // written before Phase 3a did not stamp the entry; they carry
                // the zero sentinel post-serde-default).
                let organization_slug = if entry.organization_slug.value() == 0 {
                    applied_state
                        .resolve_id_to_slug(entry.organization)
                        .map_or(entry.organization.value() as u64, |s| s.value())
                } else {
                    entry.organization_slug.value()
                };
                let vault_slug = if entry.vault_slug.value() == 0 {
                    applied_state
                        .resolve_vault_id_to_slug(entry.organization, entry.vault)
                        .map_or(0, |s| s.value())
                } else {
                    entry.vault_slug.value()
                };

                let timestamp_ns = u64::try_from(region_block.timestamp.timestamp().max(0))
                    .unwrap_or(0)
                    .saturating_mul(1_000_000_000)
                    .saturating_add(u64::from(region_block.timestamp.timestamp_subsec_nanos()));

                announcements.push(ws::BlockAnnouncement {
                    organization: Some(ws::OrganizationSlug::new(organization_slug)),
                    vault: Some(ws::VaultSlug::new(vault_slug)),
                    height: entry.vault_height,
                    block_hash: Some(ws::Hash {
                        value: bytes::Bytes::copy_from_slice(&block_hash),
                    }),
                    state_root: Some(ws::Hash {
                        value: bytes::Bytes::copy_from_slice(&entry.state_root),
                    }),
                    timestamp: timestamp_ns,
                });
            }
        }

        announcements
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::disallowed_methods, clippy::expect_used)]
mod tests {
    use super::*;

    // ── validate_read_key ───────────────────────────────────────────────────

    #[test]
    fn validate_read_key_accepts_normal_key() {
        assert!(validate_read_key("user:1").is_ok());
    }

    #[test]
    fn validate_read_key_accepts_empty_key() {
        // Empty keys are allowed by the read validator (other validation catches this)
        assert!(validate_read_key("").is_ok());
    }

    #[test]
    fn validate_read_key_rejects_system_prefix() {
        let result = validate_read_key("_meta:seq");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("reserved for system keys"));
    }

    #[test]
    fn validate_read_key_rejects_underscore_only() {
        let result = validate_read_key("_");
        assert!(result.is_err());
    }

    #[test]
    fn validate_read_key_rejects_various_system_prefixes() {
        for prefix in ["_dir:", "_idx:", "_meta:", "_shred:", "_tmp:", "_audit:"] {
            let result = validate_read_key(prefix);
            assert!(result.is_err(), "Should reject key with prefix {prefix}");
        }
    }

    #[test]
    fn validate_read_key_accepts_key_with_underscore_not_at_start() {
        assert!(validate_read_key("user_name:1").is_ok());
        assert!(validate_read_key("a_b").is_ok());
    }

    // ── parse_relationship_cursor ───────────────────────────────────────────

    #[test]
    fn parse_relationship_cursor_valid() {
        let rel = parse_relationship_cursor("doc:1#viewer@user:2").unwrap();
        assert_eq!(rel.resource, "doc:1");
        assert_eq!(rel.relation, "viewer");
        assert_eq!(rel.subject, "user:2");
    }

    #[test]
    fn parse_relationship_cursor_missing_hash_returns_none() {
        assert!(parse_relationship_cursor("doc1vieweruser2").is_none());
    }

    #[test]
    fn parse_relationship_cursor_missing_at_returns_none() {
        assert!(parse_relationship_cursor("doc:1#viewer").is_none());
    }

    #[test]
    fn parse_relationship_cursor_empty_returns_none() {
        assert!(parse_relationship_cursor("").is_none());
    }

    // ── follower_read_index timeout ─────────────────────────────────────────
    //
    // The `CommittedIndex` RPC is wrapped with `tokio::time::timeout`. These
    // tests verify the effective timeout calculation: the deadline applied to
    // the RPC is `min(DEFAULT_READ_INDEX_TIMEOUT=10s, grpc_deadline)`.

    #[test]
    fn read_index_timeout_uses_default_when_no_deadline() {
        let default = Duration::from_secs(10);
        let result = inferadb_ledger_raft::deadline::effective_timeout(default, None);
        assert_eq!(result, default);
    }

    #[test]
    fn read_index_timeout_uses_shorter_grpc_deadline() {
        let default = Duration::from_secs(10);
        let deadline = Duration::from_secs(3);
        let result = inferadb_ledger_raft::deadline::effective_timeout(default, Some(deadline));
        assert_eq!(result, deadline);
    }

    #[test]
    fn read_index_timeout_uses_default_when_grpc_deadline_longer() {
        let default = Duration::from_secs(10);
        let deadline = Duration::from_secs(30);
        let result = inferadb_ledger_raft::deadline::effective_timeout(default, Some(deadline));
        assert_eq!(result, default);
    }

    #[test]
    fn read_index_timeout_deadline_exceeded_message_contains_duration() {
        // Verify the error message format produced on timeout matches expectation.
        let timeout = Duration::from_millis(500);
        let status = Status::deadline_exceeded(format!(
            "ReadIndex timed out waiting for leader response after {}ms",
            timeout.as_millis()
        ));
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
        assert!(status.message().contains("500ms"), "message: {}", status.message());
        assert!(status.message().contains("ReadIndex timed out"));
    }

    // ── check_relationship validation ───────────────────────────────────────
    //
    // These tests exercise the validation helper used by the
    // `check_relationship` handler. Full end-to-end handler coverage lives in
    // the server integration binary (`crates/server/tests/integration.rs`)
    // alongside sibling RPC tests; this module follows the sibling-handler
    // test style (pure-function coverage here, handler coverage in the server
    // integration binary).

    #[test]
    fn check_relationship_accepts_valid_triple() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let result = validate_relationship_triple("doc:1", "viewer", "user:42", &config);
        assert!(result.is_ok(), "expected valid triple to pass, got {result:?}");
    }

    #[test]
    fn check_relationship_rejects_oversize_subject_with_invalid_argument() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        // Default max is 1024 bytes; 2 + 1024 = 1026 bytes trips the bound.
        let subject = format!("u:{}", "x".repeat(1024));
        assert!(subject.len() > config.max_relationship_string_bytes);

        let result = validate_relationship_triple("doc:1", "viewer", &subject, &config);
        let err = result.expect_err("oversize subject should fail validation");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("exceeds maximum"), "message: {}", err.message());
        // ErrorDetails is attached via build_error_details.
        assert!(!err.details().is_empty(), "ErrorDetails should be present");
    }

    #[test]
    fn check_relationship_rejects_empty_resource() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let result = validate_relationship_triple("", "viewer", "user:42", &config);
        let err = result.expect_err("empty resource should fail validation");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("resource"), "message: {}", err.message());
    }

    #[test]
    fn check_relationship_rejects_invalid_char_in_relation() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        // Spaces are not in the [a-zA-Z0-9:/_.-#] whitelist.
        let result = validate_relationship_triple("doc:1", "has viewer", "user:42", &config);
        let err = result.expect_err("invalid character should fail validation");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("invalid character"), "message: {}", err.message());
        assert!(err.message().contains("relation"), "message: {}", err.message());
    }
}
