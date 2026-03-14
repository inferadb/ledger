//! Raft log storage combining durable log entries with state machine access.

use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use inferadb_ledger_proto::proto::BlockAnnouncement;
use inferadb_ledger_state::{
    BlockArchive, StateLayer,
    system::{AppProfile, SYSTEM_VAULT_ID, SystemKeys, Team},
};
use inferadb_ledger_store::{
    Database, DatabaseConfig, FileBackend, Key, StorageBackend, Value, WriteTransaction, tables,
};
use inferadb_ledger_types::{
    AppId, AppSlug, ClientAssertionId, ClientId, EmailVerifyTokenId, NodeId, OrganizationId,
    OrganizationSlug, RefreshTokenId, Region, SigningKeyId, TeamId, TeamSlug, UserEmailId, UserId,
    UserSlug, VaultId, VaultSlug, decode, encode,
};
use openraft::{Entry, LogId, StorageError, Vote};
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tracing::warn;

use super::{
    KEY_APPLIED_STATE, KEY_LAST_PURGED, KEY_VOTE, RegionChainState,
    accessor::AppliedStateAccessor,
    raft_impl::{to_serde_error, to_storage_error},
    types::{
        AppliedState, AppliedStateCore, ClientSequenceEntry, OrganizationMeta,
        PendingExternalWrites, SequenceCounters, VaultHealthStatus, VaultMeta,
    },
};

/// Create a `StorageError` from a corruption reason string.
fn corrupted_error(reason: impl Into<String>) -> StorageError<LedgerNodeId> {
    to_storage_error(&inferadb_ledger_store::Error::Corrupted { reason: reason.into() })
}
use crate::{
    event_writer::EventWriter,
    types::{LedgerNodeId, LedgerTypeConfig, StateRootCommitment},
};

/// Combined Raft storage.
///
/// This implementation stores:
/// - Log entries in the RaftLog table indexed by log index
/// - Vote state (term + voted_for) in RaftState metadata
/// - Committed log ID for recovery
/// - Applied state (state machine) in RaftState metadata
///
/// Additionally, it integrates with:
/// - StateLayer for entity/relationship storage and state root computation
/// - BlockArchive for permanent block storage
///
/// The generic parameter `B` controls the storage backend for StateLayer and
/// BlockArchive. The raft log itself always uses FileBackend for durability.
pub struct RaftLogStore<B: StorageBackend = FileBackend> {
    /// Database handle for raft log.
    pub(super) db: Arc<Database<FileBackend>>,
    /// Cached vote state.
    pub(super) vote_cache: RwLock<Option<Vote<LedgerNodeId>>>,
    /// Cached last purged log ID.
    pub(super) last_purged_cache: RwLock<Option<LogId<LedgerNodeId>>>,
    /// Applied state (state machine) - shared with accessor.
    pub(super) applied_state: Arc<RwLock<AppliedState>>,
    /// State layer for entity/relationship storage (shared with read service).
    pub(super) state_layer: Option<Arc<StateLayer<B>>>,
    /// Block archive for permanent block storage.
    pub(super) block_archive: Option<Arc<BlockArchive<B>>>,
    /// Region for this Raft group.
    pub(super) region: Region,
    /// Node ID for block metadata.
    pub(super) node_id: NodeId,
    /// Region chain state (height and previous hash).
    ///
    /// Consolidated into single lock to avoid lock ordering issues.
    /// See: apply_to_state_machine, restore_from_db
    pub(super) region_chain: RwLock<RegionChainState>,
    /// Block announcement broadcast channel for real-time block notifications.
    ///
    /// When set, announcements are broadcast after each successful block commit.
    /// Receivers subscribe via `WatchBlocks` gRPC streaming endpoint.
    pub(super) block_announcements: Option<broadcast::Sender<BlockAnnouncement>>,
    /// Event writer for persisting apply-phase audit events to `events.db`.
    ///
    /// When set, the state machine apply path emits deterministic events
    /// for each committed operation.
    pub(super) event_writer: Option<EventWriter<B>>,
    /// Client sequence eviction configuration.
    ///
    /// Controls how often expired client sequence entries are purged
    /// from both the in-memory HashMap and the `ClientSequences` B+ tree table.
    pub(super) client_sequence_eviction:
        inferadb_ledger_types::config::ClientSequenceEvictionConfig,
    /// Buffer of state root commitments from recent applies.
    ///
    /// Populated by `apply_to_state_machine` after each block with vault entries.
    /// Drained by the leader when constructing the next `RaftPayload`, piggybacking
    /// commitments onto entry N+1 for follower verification.
    pub(super) state_root_commitments: Arc<Mutex<Vec<StateRootCommitment>>>,
    /// Channel for sending detected state root divergences to the handler task.
    ///
    /// When a mismatch is detected during apply, the divergence event is sent
    /// through this channel. The handler proposes `UpdateVaultHealth { healthy: false }`
    /// to halt the vault cluster-wide.
    pub(super) divergence_sender:
        Option<tokio::sync::mpsc::UnboundedSender<crate::types::StateRootDivergence>>,
}

#[allow(clippy::result_large_err)]
impl<B: StorageBackend> RaftLogStore<B> {
    /// Page size for Raft log storage.
    ///
    /// Uses 16KB pages (vs default 4KB) to support larger batch sizes.
    /// A batch of 100 operations typically serializes to ~8-12KB with postcard.
    /// Max supported: 64KB. Minimum: 512 bytes (must be power of 2).
    pub const RAFT_PAGE_SIZE: usize = 16 * 1024; // 16KB

    /// Opens or creates a Raft log storage database.
    ///
    /// New databases are created with 16KB pages to support larger batch sizes.
    /// Existing databases retain their original page size for backwards compatibility.
    ///
    /// # Errors
    ///
    /// Returns `StorageError` if the database file cannot be opened or created,
    /// or if the cached vote/purge metadata cannot be loaded.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError<LedgerNodeId>> {
        // Try to open existing database, otherwise create new one with larger pages
        let db = if path.as_ref().exists() {
            // Existing database - use whatever page size it was created with
            Database::open(path.as_ref()).map_err(|e| to_storage_error(&e))?
        } else {
            // New database - use larger pages for bigger batch sizes
            let config = DatabaseConfig { page_size: Self::RAFT_PAGE_SIZE, ..Default::default() };
            Database::create_with_config(path.as_ref(), config).map_err(|e| to_storage_error(&e))?
        };

        let store = Self {
            db: Arc::new(db),
            vote_cache: RwLock::new(None),
            last_purged_cache: RwLock::new(None),
            applied_state: Arc::new(RwLock::new(AppliedState {
                sequences: SequenceCounters::new(),
                ..Default::default()
            })),
            state_layer: None,
            block_archive: None,
            region: Region::GLOBAL,
            node_id: NodeId::new(""),
            region_chain: RwLock::new(RegionChainState {
                height: 0,
                previous_hash: inferadb_ledger_types::ZERO_HASH,
            }),
            block_announcements: None,
            event_writer: None,
            client_sequence_eviction:
                inferadb_ledger_types::config::ClientSequenceEvictionConfig::default(),
            state_root_commitments: Arc::new(Mutex::new(Vec::new())),
            divergence_sender: None,
        };

        // Load cached values
        store.load_caches()?;

        Ok(store)
    }

    /// Configures the state layer for transaction application.
    ///
    /// Also rebuilds in-memory secondary indices (team name index, user→org
    /// index) from persisted profiles in the state layer.
    pub fn with_state_layer(mut self, state_layer: Arc<StateLayer<B>>) -> Self {
        self.rebuild_secondary_indices(&state_layer);
        self.state_layer = Some(state_layer);
        self
    }

    /// Configures the block archive for permanent block storage.
    pub fn with_block_archive(mut self, block_archive: Arc<BlockArchive<B>>) -> Self {
        self.block_archive = Some(block_archive);
        self
    }

    /// Configures region metadata.
    pub fn with_region_config(mut self, region: Region, node_id: NodeId) -> Self {
        self.region = region;
        self.node_id = node_id;
        self
    }

    /// Configures the block announcements broadcast channel.
    ///
    /// When set, the log store will broadcast `BlockAnnouncement` messages
    /// after each successful block commit in `apply_to_state_machine`.
    pub fn with_block_announcements(
        mut self,
        sender: broadcast::Sender<BlockAnnouncement>,
    ) -> Self {
        self.block_announcements = Some(sender);
        self
    }

    /// Configures the event writer for apply-phase audit events.
    ///
    /// When set, the apply path emits deterministic events for each committed
    /// operation to the dedicated `events.db`.
    pub fn with_event_writer(mut self, event_writer: EventWriter<B>) -> Self {
        self.event_writer = Some(event_writer);
        self
    }

    /// Configures client sequence eviction parameters.
    ///
    /// Controls TTL-based eviction of expired client sequence entries
    /// from both in-memory state and the `ClientSequences` B+ tree table.
    pub fn with_client_sequence_eviction(
        mut self,
        config: inferadb_ledger_types::config::ClientSequenceEvictionConfig,
    ) -> Self {
        self.client_sequence_eviction = config;
        self
    }

    /// Rebuilds in-memory secondary indices from persisted profiles.
    ///
    /// Rebuilds:
    /// - `team_name_index`: (org_id, name) → team_id from `Team` records
    /// - `app_name_index`: (org_id, name) → app_id from `AppProfile` records
    /// - `user_org_index`: user_id → {org_ids} from `Organization` skeleton records
    ///
    /// Uses prefix scans on the state layer instead of in-memory slug indices.
    /// This correctly handles both GLOBAL and REGIONAL Raft groups:
    /// - GLOBAL: no profile keys exist → no name index entries (correct)
    /// - REGIONAL: profile keys exist → name indices populated (correct)
    fn rebuild_secondary_indices(&self, state_layer: &StateLayer<B>) {
        // Rebuild team name index via prefix scan on team:*
        let name_entries =
            scan_prefix_decode::<B, Team, _>(state_layer, SystemKeys::TEAM_PREFIX, |profile| {
                ((profile.organization, profile.name), profile.team)
            });

        // Rebuild app name index via prefix scan on app_profile:*
        let app_name_entries = scan_prefix_decode::<B, AppProfile, _>(
            state_layer,
            SystemKeys::APP_PROFILE_PREFIX,
            |profile| ((profile.organization, profile.name), profile.app),
        );

        // Rebuild user→org index from Organization skeletons (GLOBAL).
        // Members live in the Organization skeleton, not OrganizationProfile.
        let org_ids: Vec<_> = {
            let state = self.applied_state.read();
            state.organizations.keys().copied().collect()
        };
        let mut user_org_entries: std::collections::HashMap<
            UserId,
            std::collections::HashSet<OrganizationId>,
        > = std::collections::HashMap::new();
        for org_id in org_ids {
            let key = SystemKeys::organization_key(org_id);
            match state_layer.get_entity(SYSTEM_VAULT_ID, key.as_bytes()) {
                Ok(Some(entity)) => {
                    match decode::<inferadb_ledger_state::system::Organization>(&entity.value) {
                        Ok(org) => {
                            for member in &org.members {
                                user_org_entries.entry(member.user_id).or_default().insert(org_id);
                            }
                        },
                        Err(e) => {
                            warn!(
                                organization_id = %org_id,
                                error = %e,
                                "Failed to decode Organization skeleton during index rebuild"
                            );
                        },
                    }
                },
                Ok(None) => {
                    // Skeleton may not exist yet for orgs still provisioning
                },
                Err(e) => {
                    warn!(
                        organization_id = %org_id,
                        error = %e,
                        "Failed to read organization profile during index rebuild"
                    );
                },
            }
        }

        let mut state = self.applied_state.write();
        for (key, team_id) in name_entries {
            state.team_name_index.insert(key, team_id);
        }
        for (key, app_id) in app_name_entries {
            state.app_name_index.insert(key, app_id);
        }
        state.user_org_index = user_org_entries;
    }

    /// Returns a reference to the event writer (if configured).
    pub fn event_writer(&self) -> Option<&EventWriter<B>> {
        self.event_writer.as_ref()
    }

    /// Returns a reference to the block announcements sender (if configured).
    pub fn block_announcements(&self) -> Option<&broadcast::Sender<BlockAnnouncement>> {
        self.block_announcements.as_ref()
    }

    /// Returns the current region height.
    pub fn current_region_height(&self) -> u64 {
        self.region_chain.read().height
    }

    /// Returns a reference to the state layer (if configured).
    pub fn state_layer(&self) -> Option<&Arc<StateLayer<B>>> {
        self.state_layer.as_ref()
    }

    /// Returns a reference to the block archive (if configured).
    pub fn block_archive(&self) -> Option<&Arc<BlockArchive<B>>> {
        self.block_archive.as_ref()
    }

    /// Returns an accessor for reading applied state.
    ///
    /// This accessor can be cloned and passed to services that need to read
    /// vault heights and health status.
    pub fn accessor(&self) -> AppliedStateAccessor {
        AppliedStateAccessor { state: self.applied_state.clone() }
    }

    /// Returns the shared commitment buffer handle.
    ///
    /// Used to pass the same `Arc` to `RegionGroup` so that the proposal path
    /// can drain commitments without holding a reference to the log store.
    pub fn commitment_buffer(&self) -> Arc<Mutex<Vec<StateRootCommitment>>> {
        Arc::clone(&self.state_root_commitments)
    }

    /// Configures the divergence sender channel.
    ///
    /// When set, detected state root mismatches during apply are sent through
    /// this channel to an async handler that proposes `UpdateVaultHealth`.
    pub fn with_divergence_sender(
        mut self,
        sender: tokio::sync::mpsc::UnboundedSender<crate::types::StateRootDivergence>,
    ) -> Self {
        self.divergence_sender = Some(sender);
        self
    }

    /// Drains all buffered state root commitments.
    ///
    /// Called by the leader when constructing the next `RaftPayload` to
    /// piggyback commitments onto the proposal. Returns the full buffer
    /// contents and leaves it empty.
    pub fn drain_state_root_commitments(&self) -> Vec<StateRootCommitment> {
        std::mem::take(&mut *self.state_root_commitments.lock().unwrap_or_else(|e| e.into_inner()))
    }

    /// Checks if this log store has been previously initialized.
    ///
    /// Returns `true` if a vote has been saved, indicating that Raft consensus
    /// has been started at some point. Used for auto-detection of whether to
    /// bootstrap a new cluster or resume an existing one.
    pub fn is_initialized(&self) -> bool {
        self.vote_cache.read().is_some()
    }

    /// Loads metadata values into caches.
    ///
    /// # Errors
    ///
    /// Returns `StorageError` if the underlying database read or
    /// deserialization of cached vote/purge metadata fails.
    pub(super) fn load_caches(&self) -> Result<(), StorageError<LedgerNodeId>> {
        let read_txn = self.db.read().map_err(|e| to_storage_error(&e))?;

        if let Some(vote_data) = read_txn
            .get::<tables::RaftState>(&KEY_VOTE.to_string())
            .map_err(|e| to_storage_error(&e))?
        {
            let vote: Vote<LedgerNodeId> = decode(&vote_data).map_err(|e| to_serde_error(&e))?;
            *self.vote_cache.write() = Some(vote);
        }

        if let Some(purged_data) = read_txn
            .get::<tables::RaftState>(&KEY_LAST_PURGED.to_string())
            .map_err(|e| to_storage_error(&e))?
        {
            let purged: LogId<LedgerNodeId> =
                decode(&purged_data).map_err(|e| to_serde_error(&e))?;
            *self.last_purged_cache.write() = Some(purged);
        }

        // Drop read_txn before loading applied state (which opens its own transactions
        // and may perform old-format migration with a write transaction).
        drop(read_txn);

        let state = self.load_state_from_tables()?;
        *self.region_chain.write() = RegionChainState {
            height: state.region_height,
            previous_hash: state.previous_region_hash,
        };
        *self.applied_state.write() = state;

        Ok(())
    }

    /// Returns the last log entry.
    pub(super) fn get_last_entry(
        &self,
    ) -> Result<Option<Entry<LedgerTypeConfig>>, StorageError<LedgerNodeId>> {
        let read_txn = self.db.read().map_err(|e| to_storage_error(&e))?;

        if let Some((_, entry_data)) =
            read_txn.last::<tables::RaftLog>().map_err(|e| to_storage_error(&e))?
        {
            let entry: Entry<LedgerTypeConfig> =
                decode(&entry_data).map_err(|e| to_serde_error(&e))?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    // ========================================================================
    // Externalized State Persistence (Tasks 4+)
    // ========================================================================

    /// Version sentinel prefix for the new `AppliedStateCore` format.
    ///
    /// The old format stores a full `AppliedState` postcard blob with no prefix.
    /// The new format prepends `[0x00, 0x01]` (version 1) before the postcard
    /// `AppliedStateCore` bytes. Since postcard never starts with `0x00` for a
    /// struct (the first byte encodes the `Option` discriminant for `last_applied`),
    /// this sentinel is unambiguous.
    // Used by save_state_core/load_state_from_tables (wired in next task).
    const STATE_CORE_VERSION: [u8; 2] = [0x00, 0x01];

    /// Flushes accumulated external writes into their respective B+ tree tables.
    ///
    /// Writes to all 12 external tables: `OrganizationMeta`, `VaultMeta`,
    /// `VaultHeights`, `VaultHashes`, `VaultHealth`, `Sequences`,
    /// `ClientSequences`, `OrganizationSlugIndex`, `VaultSlugIndex`,
    /// `UserSlugIndex`, `TeamSlugIndex`, `AppSlugIndex`.
    ///
    /// Handles both inserts and deletes (slug index deletions on org/vault
    /// removal, client sequence eviction).
    ///
    /// This method does NOT commit the transaction — the caller commits after
    /// writing both the core blob and external tables, ensuring atomicity.
    pub(super) fn flush_external_writes(
        pending: &PendingExternalWrites,
        write_txn: &mut WriteTransaction<'_, FileBackend>,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        // OrganizationMeta inserts/updates
        for (org_id, blob) in &pending.organizations {
            write_txn
                .insert::<tables::OrganizationMeta>(&org_id.value(), blob)
                .map_err(|e| to_storage_error(&e))?;
        }

        // OrganizationMeta deletes
        for org_id in &pending.organizations_deleted {
            write_txn
                .delete::<tables::OrganizationMeta>(&org_id.value())
                .map_err(|e| to_storage_error(&e))?;
        }

        // VaultMeta inserts/updates (keyed by vault_id alone)
        for (vault_id, blob) in &pending.vaults {
            write_txn
                .insert::<tables::VaultMeta>(&vault_id.value(), blob)
                .map_err(|e| to_storage_error(&e))?;
        }

        // VaultMeta deletes
        for vault_id in &pending.vaults_deleted {
            write_txn
                .delete::<tables::VaultMeta>(&vault_id.value())
                .map_err(|e| to_storage_error(&e))?;
        }

        // VaultHeights inserts/updates (composite key)
        for ((org_id, vault_id), height) in &pending.vault_heights {
            let key = PendingExternalWrites::vault_composite_key(*org_id, *vault_id);
            let value = encode(height).map_err(|e| to_serde_error(&e))?;
            write_txn
                .insert::<tables::VaultHeights>(&key, &value)
                .map_err(|e| to_storage_error(&e))?;
        }

        // VaultHashes inserts/updates (composite key)
        for ((org_id, vault_id), hash) in &pending.vault_hashes {
            let key = PendingExternalWrites::vault_composite_key(*org_id, *vault_id);
            write_txn
                .insert::<tables::VaultHashes>(&key, &hash.to_vec())
                .map_err(|e| to_storage_error(&e))?;
        }

        // VaultHealth inserts/updates (composite key)
        for ((org_id, vault_id), status) in &pending.vault_health {
            let key = PendingExternalWrites::vault_composite_key(*org_id, *vault_id);
            let value = encode(status).map_err(|e| to_serde_error(&e))?;
            write_txn
                .insert::<tables::VaultHealth>(&key, &value)
                .map_err(|e| to_storage_error(&e))?;
        }

        // Sequences inserts/updates
        for (name, value) in &pending.sequences {
            write_txn.insert::<tables::Sequences>(name, value).map_err(|e| to_storage_error(&e))?;
        }

        // ClientSequences inserts/updates
        for (key, value) in &pending.client_sequences {
            write_txn
                .insert::<tables::ClientSequences>(key, value)
                .map_err(|e| to_storage_error(&e))?;
        }

        // ClientSequences deletes (eviction)
        for key in &pending.client_sequences_deleted {
            write_txn.delete::<tables::ClientSequences>(key).map_err(|e| to_storage_error(&e))?;
        }

        // OrganizationSlugIndex inserts/updates
        for (slug, org_id) in &pending.slug_index {
            let value = encode(org_id).map_err(|e| to_serde_error(&e))?;
            write_txn
                .insert::<tables::OrganizationSlugIndex>(&slug.value(), &value)
                .map_err(|e| to_storage_error(&e))?;
        }

        // OrganizationSlugIndex deletes
        for slug in &pending.slug_index_deleted {
            write_txn
                .delete::<tables::OrganizationSlugIndex>(&slug.value())
                .map_err(|e| to_storage_error(&e))?;
        }

        // VaultSlugIndex inserts/updates
        for (slug, vault_id) in &pending.vault_slug_index {
            let value = encode(vault_id).map_err(|e| to_serde_error(&e))?;
            write_txn
                .insert::<tables::VaultSlugIndex>(&slug.value(), &value)
                .map_err(|e| to_storage_error(&e))?;
        }

        // VaultSlugIndex deletes
        for slug in &pending.vault_slug_index_deleted {
            write_txn
                .delete::<tables::VaultSlugIndex>(&slug.value())
                .map_err(|e| to_storage_error(&e))?;
        }

        // UserSlugIndex inserts/updates
        for (slug, user_id) in &pending.user_slug_index {
            let value = encode(user_id).map_err(|e| to_serde_error(&e))?;
            write_txn
                .insert::<tables::UserSlugIndex>(&slug.value(), &value)
                .map_err(|e| to_storage_error(&e))?;
        }

        // UserSlugIndex deletes
        for slug in &pending.user_slug_index_deleted {
            write_txn
                .delete::<tables::UserSlugIndex>(&slug.value())
                .map_err(|e| to_storage_error(&e))?;
        }

        // TeamSlugIndex inserts/updates
        for (slug, (org_id, team_id)) in &pending.team_slug_index {
            let value = encode(&(*org_id, *team_id)).map_err(|e| to_serde_error(&e))?;
            write_txn
                .insert::<tables::TeamSlugIndex>(&slug.value(), &value)
                .map_err(|e| to_storage_error(&e))?;
        }

        // TeamSlugIndex deletes
        for slug in &pending.team_slug_index_deleted {
            write_txn
                .delete::<tables::TeamSlugIndex>(&slug.value())
                .map_err(|e| to_storage_error(&e))?;
        }

        // AppSlugIndex inserts/updates
        for (slug, (org_id, app_id)) in &pending.app_slug_index {
            let value = encode(&(*org_id, *app_id)).map_err(|e| to_serde_error(&e))?;
            write_txn
                .insert::<tables::AppSlugIndex>(&slug.value(), &value)
                .map_err(|e| to_storage_error(&e))?;
        }

        // AppSlugIndex deletes
        for slug in &pending.app_slug_index_deleted {
            write_txn
                .delete::<tables::AppSlugIndex>(&slug.value())
                .map_err(|e| to_storage_error(&e))?;
        }

        Ok(())
    }

    /// Persist `AppliedStateCore` and flush all pending external writes
    /// in a single atomic `WriteTransaction`.
    ///
    /// Replaces `save_applied_state()` — the core blob is now <512 bytes
    /// regardless of cluster scale, while HashMap data is distributed across
    /// 9 dedicated B+ tree tables.
    ///
    /// The version sentinel prefix `[0x00, 0x01]` is prepended to the
    /// serialized `AppliedStateCore` bytes to distinguish from old-format
    /// full `AppliedState` blobs during startup migration.
    pub(super) fn save_state_core(
        &self,
        state: &AppliedState,
        pending: &PendingExternalWrites,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let core = AppliedStateCore::from(state);
        let core_bytes = encode(&core).map_err(|e| to_serde_error(&e))?;

        // Prepend version sentinel
        let mut state_data = Vec::with_capacity(Self::STATE_CORE_VERSION.len() + core_bytes.len());
        state_data.extend_from_slice(&Self::STATE_CORE_VERSION);
        state_data.extend_from_slice(&core_bytes);

        let mut write_txn = self.db.write().map_err(|e| to_storage_error(&e))?;

        // Write core blob to RaftState
        write_txn
            .insert::<tables::RaftState>(&KEY_APPLIED_STATE.to_string(), &state_data)
            .map_err(|e| to_storage_error(&e))?;

        // Flush all external table writes in the same transaction
        Self::flush_external_writes(pending, &mut write_txn)?;

        // Single atomic commit
        write_txn.commit().map_err(|e| to_storage_error(&e))?;

        Ok(())
    }

    /// Reconstruct the full `AppliedState` from external tables on startup.
    ///
    /// Reads `AppliedStateCore` from the `RaftState` table (with version
    /// sentinel detection) and populates each HashMap field by iterating
    /// its corresponding table. Also reconstructs derived fields
    /// (`id_to_slug`, `vault_id_to_slug`, `organization_storage_bytes`).
    ///
    /// Handles two cases:
    /// - **Valid format** (version sentinel present): normal load from tables.
    /// - **Fresh database** (no `KEY_APPLIED_STATE` entry): returns default state.
    pub(super) fn load_state_from_tables(
        &self,
    ) -> Result<AppliedState, StorageError<LedgerNodeId>> {
        let read_txn = self.db.read().map_err(|e| to_storage_error(&e))?;

        let state_data = read_txn
            .get::<tables::RaftState>(&KEY_APPLIED_STATE.to_string())
            .map_err(|e| to_storage_error(&e))?;

        let Some(state_data) = state_data else {
            // Fresh database — return default state
            return Ok(AppliedState { sequences: SequenceCounters::new(), ..Default::default() });
        };

        // Drop read transaction before potentially opening a write transaction for migration
        drop(read_txn);

        // Check for version sentinel
        if state_data.len() >= 2 && state_data[0..2] == Self::STATE_CORE_VERSION {
            // New format — deserialize AppliedStateCore, populate from tables
            let core: AppliedStateCore =
                decode(&state_data[2..]).map_err(|e| to_serde_error(&e))?;
            self.reconstruct_from_tables(core)
        } else {
            // Unrecognized format — corrupt data
            Err(corrupted_error(
                "Unrecognized AppliedState format (missing version sentinel)".to_string(),
            ))
        }
    }

    /// Reconstruct the full `AppliedState` from `AppliedStateCore` and external tables.
    fn reconstruct_from_tables(
        &self,
        core: AppliedStateCore,
    ) -> Result<AppliedState, StorageError<LedgerNodeId>> {
        let read_txn = self.db.read().map_err(|e| to_storage_error(&e))?;

        let mut state = AppliedState {
            last_applied: core.last_applied,
            membership: core.membership,
            region_height: core.region_height,
            previous_region_hash: core.previous_region_hash,
            last_applied_timestamp_ns: core.last_applied_timestamp_ns,
            sequences: SequenceCounters::new(),
            ..Default::default()
        };

        // Sequences table (5 individual keys)
        Self::load_sequences(&read_txn, &mut state)?;

        // OrganizationMeta table scan
        Self::load_organizations(&read_txn, &mut state)?;

        // VaultMeta table scan
        Self::load_vaults(&read_txn, &mut state)?;

        // VaultHeights table scan
        Self::load_vault_heights(&read_txn, &mut state)?;

        // VaultHashes table scan
        Self::load_vault_hashes(&read_txn, &mut state)?;

        // VaultHealth table scan
        Self::load_vault_health(&read_txn, &mut state)?;

        // ClientSequences table scan
        Self::load_client_sequences(&read_txn, &mut state)?;

        // OrganizationSlugIndex table scan
        Self::load_slug_index(&read_txn, &mut state)?;

        // VaultSlugIndex table scan
        Self::load_vault_slug_index(&read_txn, &mut state)?;

        // UserSlugIndex table scan
        Self::load_user_slug_index(&read_txn, &mut state)?;

        // TeamSlugIndex table scan
        Self::load_team_slug_index(&read_txn, &mut state)?;

        // AppSlugIndex table scan
        Self::load_app_slug_index(&read_txn, &mut state)?;

        Ok(state)
    }

    /// Load sequence counters from the Sequences table.
    fn load_sequences(
        read_txn: &inferadb_ledger_store::ReadTransaction<'_, FileBackend>,
        state: &mut AppliedState,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let mut iter = read_txn.iter::<tables::Sequences>().map_err(|e| to_storage_error(&e))?;

        while let Some((key_bytes, value_bytes)) =
            iter.next_entry().map_err(|e| to_storage_error(&e))?
        {
            let key = String::from_utf8(key_bytes)
                .map_err(|e| corrupted_error(format!("invalid UTF-8 sequence key: {e}")))?;
            // Use the store's Value::decode to match the encoding (big-endian u64)
            let value: u64 = <u64 as Value>::decode(&value_bytes).ok_or_else(|| {
                corrupted_error(format!("invalid u64 sequence value for key '{key}'"))
            })?;

            match key.as_str() {
                "organization" => {
                    state.sequences.organization = OrganizationId::new(value as i64);
                },
                "vault" => state.sequences.vault = VaultId::new(value as i64),
                "user" => state.sequences.user = UserId::new(value as i64),
                "user_email" => state.sequences.user_email = UserEmailId::new(value as i64),
                "email_verify" => {
                    state.sequences.email_verify = EmailVerifyTokenId::new(value as i64);
                },
                "team" => state.sequences.team = TeamId::new(value as i64),
                "app" => state.sequences.app = AppId::new(value as i64),
                "client_assertion" => {
                    state.sequences.client_assertion = ClientAssertionId::new(value as i64);
                },
                "signing_key" => {
                    state.sequences.signing_key = SigningKeyId::new(value as i64);
                },
                "refresh_token" => {
                    state.sequences.refresh_token = RefreshTokenId::new(value as i64);
                },
                unknown => {
                    warn!(key = unknown, "Unknown sequence key in Sequences table, skipping");
                },
            }
        }

        Ok(())
    }

    /// Load organizations from the OrganizationMeta table, populating derived fields.
    fn load_organizations(
        read_txn: &inferadb_ledger_store::ReadTransaction<'_, FileBackend>,
        state: &mut AppliedState,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let mut iter =
            read_txn.iter::<tables::OrganizationMeta>().map_err(|e| to_storage_error(&e))?;

        while let Some((key_bytes, value_bytes)) =
            iter.next_entry().map_err(|e| to_storage_error(&e))?
        {
            // Use store's Key::decode to reverse the sign-bit-flipped big-endian encoding
            let org_id_raw = <i64 as Key>::decode(&key_bytes)
                .ok_or_else(|| corrupted_error("invalid i64 key in OrganizationMeta table"))?;
            let org_id = OrganizationId::new(org_id_raw);

            let meta: OrganizationMeta = decode(&value_bytes).map_err(|e| to_serde_error(&e))?;

            // Derived: id_to_slug reverse mapping
            state.id_to_slug.insert(org_id, meta.slug);

            // Derived: organization_storage_bytes
            if meta.storage_bytes > 0 {
                state.organization_storage_bytes.insert(org_id, meta.storage_bytes);
            }

            state.organizations.insert(org_id, meta);
        }

        Ok(())
    }

    /// Load vaults from the VaultMeta table, populating derived fields.
    fn load_vaults(
        read_txn: &inferadb_ledger_store::ReadTransaction<'_, FileBackend>,
        state: &mut AppliedState,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let mut iter = read_txn.iter::<tables::VaultMeta>().map_err(|e| to_storage_error(&e))?;

        while let Some((key_bytes, value_bytes)) =
            iter.next_entry().map_err(|e| to_storage_error(&e))?
        {
            let vault_id_raw = <i64 as Key>::decode(&key_bytes)
                .ok_or_else(|| corrupted_error("invalid i64 key in VaultMeta table"))?;
            let vault_id = VaultId::new(vault_id_raw);

            let meta: VaultMeta = decode(&value_bytes).map_err(|e| to_serde_error(&e))?;

            // Derived: vault_id_to_slug reverse mapping
            state.vault_id_to_slug.insert(vault_id, meta.slug);

            // Key uses organization from the deserialized blob and vault_id from the table key
            state.vaults.insert((meta.organization, vault_id), meta);
        }

        Ok(())
    }

    /// Load vault heights from the VaultHeights table.
    fn load_vault_heights(
        read_txn: &inferadb_ledger_store::ReadTransaction<'_, FileBackend>,
        state: &mut AppliedState,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let mut iter = read_txn.iter::<tables::VaultHeights>().map_err(|e| to_storage_error(&e))?;

        while let Some((key_bytes, value_bytes)) =
            iter.next_entry().map_err(|e| to_storage_error(&e))?
        {
            let (org_id, vault_id) = Self::decode_vault_composite_key(&key_bytes)?;
            let height: u64 = decode(&value_bytes).map_err(|e| to_serde_error(&e))?;
            state.vault_heights.insert((org_id, vault_id), height);
        }

        Ok(())
    }

    /// Load vault hashes from the VaultHashes table.
    fn load_vault_hashes(
        read_txn: &inferadb_ledger_store::ReadTransaction<'_, FileBackend>,
        state: &mut AppliedState,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let mut iter = read_txn.iter::<tables::VaultHashes>().map_err(|e| to_storage_error(&e))?;

        while let Some((key_bytes, value_bytes)) =
            iter.next_entry().map_err(|e| to_storage_error(&e))?
        {
            let (org_id, vault_id) = Self::decode_vault_composite_key(&key_bytes)?;

            let hash: [u8; 32] = value_bytes
                .try_into()
                .map_err(|_| corrupted_error("invalid hash length in VaultHashes table"))?;
            state.previous_vault_hashes.insert((org_id, vault_id), hash);
        }

        Ok(())
    }

    /// Load vault health from the VaultHealth table.
    fn load_vault_health(
        read_txn: &inferadb_ledger_store::ReadTransaction<'_, FileBackend>,
        state: &mut AppliedState,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let mut iter = read_txn.iter::<tables::VaultHealth>().map_err(|e| to_storage_error(&e))?;

        while let Some((key_bytes, value_bytes)) =
            iter.next_entry().map_err(|e| to_storage_error(&e))?
        {
            let (org_id, vault_id) = Self::decode_vault_composite_key(&key_bytes)?;
            let status: VaultHealthStatus = decode(&value_bytes).map_err(|e| to_serde_error(&e))?;
            state.vault_health.insert((org_id, vault_id), status);
        }

        Ok(())
    }

    /// Load client sequences from the ClientSequences table.
    fn load_client_sequences(
        read_txn: &inferadb_ledger_store::ReadTransaction<'_, FileBackend>,
        state: &mut AppliedState,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let mut iter =
            read_txn.iter::<tables::ClientSequences>().map_err(|e| to_storage_error(&e))?;

        while let Some((key_bytes, value_bytes)) =
            iter.next_entry().map_err(|e| to_storage_error(&e))?
        {
            if key_bytes.len() < 16 {
                return Err(corrupted_error(
                    "ClientSequences key too short (expected >= 16 bytes)",
                ));
            }
            let org_id = OrganizationId::new(i64::from_be_bytes(
                key_bytes[..8]
                    .try_into()
                    .map_err(|_| corrupted_error("invalid org_id in ClientSequences key"))?,
            ));
            let vault_id = VaultId::new(i64::from_be_bytes(
                key_bytes[8..16]
                    .try_into()
                    .map_err(|_| corrupted_error("invalid vault_id in ClientSequences key"))?,
            ));
            let client_id = String::from_utf8(key_bytes[16..].to_vec()).map_err(|e| {
                corrupted_error(format!("invalid UTF-8 client_id in ClientSequences key: {e}"))
            })?;

            let entry: ClientSequenceEntry =
                decode(&value_bytes).map_err(|e| to_serde_error(&e))?;
            state.client_sequences.insert((org_id, vault_id, ClientId::new(client_id)), entry);
        }

        Ok(())
    }

    /// Load organization slug index from the OrganizationSlugIndex table.
    fn load_slug_index(
        read_txn: &inferadb_ledger_store::ReadTransaction<'_, FileBackend>,
        state: &mut AppliedState,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let mut iter =
            read_txn.iter::<tables::OrganizationSlugIndex>().map_err(|e| to_storage_error(&e))?;

        while let Some((key_bytes, value_bytes)) =
            iter.next_entry().map_err(|e| to_storage_error(&e))?
        {
            let slug_raw = <u64 as Key>::decode(&key_bytes)
                .ok_or_else(|| corrupted_error("invalid u64 key in OrganizationSlugIndex table"))?;
            let slug = OrganizationSlug::new(slug_raw);
            let org_id: OrganizationId = decode(&value_bytes).map_err(|e| to_serde_error(&e))?;
            state.slug_index.insert(slug, org_id);
        }

        Ok(())
    }

    /// Load vault slug index from the VaultSlugIndex table.
    fn load_vault_slug_index(
        read_txn: &inferadb_ledger_store::ReadTransaction<'_, FileBackend>,
        state: &mut AppliedState,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let mut iter =
            read_txn.iter::<tables::VaultSlugIndex>().map_err(|e| to_storage_error(&e))?;

        while let Some((key_bytes, value_bytes)) =
            iter.next_entry().map_err(|e| to_storage_error(&e))?
        {
            let slug_raw = <u64 as Key>::decode(&key_bytes)
                .ok_or_else(|| corrupted_error("invalid u64 key in VaultSlugIndex table"))?;
            let slug = VaultSlug::new(slug_raw);
            let vault_id: VaultId = decode(&value_bytes).map_err(|e| to_serde_error(&e))?;
            state.vault_slug_index.insert(slug, vault_id);
        }

        Ok(())
    }

    /// Load user slug index from the UserSlugIndex table.
    fn load_user_slug_index(
        read_txn: &inferadb_ledger_store::ReadTransaction<'_, FileBackend>,
        state: &mut AppliedState,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let mut iter =
            read_txn.iter::<tables::UserSlugIndex>().map_err(|e| to_storage_error(&e))?;

        while let Some((key_bytes, value_bytes)) =
            iter.next_entry().map_err(|e| to_storage_error(&e))?
        {
            let slug_raw = <u64 as Key>::decode(&key_bytes)
                .ok_or_else(|| corrupted_error("invalid u64 key in UserSlugIndex table"))?;
            let slug = UserSlug::new(slug_raw);
            let user_id: UserId = decode(&value_bytes).map_err(|e| to_serde_error(&e))?;
            state.user_slug_index.insert(slug, user_id);
            state.user_id_to_slug.insert(user_id, slug);
        }

        Ok(())
    }

    /// Load team slug index from the TeamSlugIndex table.
    fn load_team_slug_index(
        read_txn: &inferadb_ledger_store::ReadTransaction<'_, FileBackend>,
        state: &mut AppliedState,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let mut iter =
            read_txn.iter::<tables::TeamSlugIndex>().map_err(|e| to_storage_error(&e))?;

        while let Some((key_bytes, value_bytes)) =
            iter.next_entry().map_err(|e| to_storage_error(&e))?
        {
            let slug_raw = <u64 as Key>::decode(&key_bytes)
                .ok_or_else(|| corrupted_error("invalid u64 key in TeamSlugIndex table"))?;
            let slug = TeamSlug::new(slug_raw);
            let (org_id, team_id): (OrganizationId, TeamId) =
                decode(&value_bytes).map_err(|e| to_serde_error(&e))?;
            state.team_slug_index.insert(slug, (org_id, team_id));
            state.team_id_to_slug.insert(team_id, slug);
        }

        Ok(())
    }

    /// Load app slug index from the AppSlugIndex table.
    fn load_app_slug_index(
        read_txn: &inferadb_ledger_store::ReadTransaction<'_, FileBackend>,
        state: &mut AppliedState,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let mut iter = read_txn.iter::<tables::AppSlugIndex>().map_err(|e| to_storage_error(&e))?;

        while let Some((key_bytes, value_bytes)) =
            iter.next_entry().map_err(|e| to_storage_error(&e))?
        {
            let slug_raw = <u64 as Key>::decode(&key_bytes)
                .ok_or_else(|| corrupted_error("invalid u64 key in AppSlugIndex table"))?;
            let slug = AppSlug::new(slug_raw);
            let (org_id, app_id): (OrganizationId, AppId) =
                decode(&value_bytes).map_err(|e| to_serde_error(&e))?;
            state.app_slug_index.insert(slug, (org_id, app_id));
            state.app_id_to_slug.insert(app_id, slug);
        }

        Ok(())
    }

    /// Decode a 16-byte composite key into (OrganizationId, VaultId).
    fn decode_vault_composite_key(
        key_bytes: &[u8],
    ) -> Result<(OrganizationId, VaultId), StorageError<LedgerNodeId>> {
        if key_bytes.len() != 16 {
            return Err(corrupted_error(format!(
                "vault composite key must be 16 bytes, got {}",
                key_bytes.len()
            )));
        }
        let org_id = OrganizationId::new(i64::from_be_bytes(
            key_bytes[..8]
                .try_into()
                .map_err(|_| corrupted_error("invalid org_id in composite key"))?,
        ));
        let vault_id = VaultId::new(i64::from_be_bytes(
            key_bytes[8..16]
                .try_into()
                .map_err(|_| corrupted_error("invalid vault_id in composite key"))?,
        ));
        Ok((org_id, vault_id))
    }
}

/// Prefix-scans the state layer, deserializes each entity, and maps to results.
///
/// Pages through `list_entities` with the given prefix. Decode failures are
/// logged and skipped.
fn scan_prefix_decode<B: StorageBackend, T: serde::de::DeserializeOwned, R>(
    state_layer: &StateLayer<B>,
    prefix: &str,
    map_fn: impl Fn(T) -> R,
) -> Vec<R> {
    const PAGE_SIZE: usize = 1000;
    let mut results = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        match state_layer.list_entities(SYSTEM_VAULT_ID, Some(prefix), cursor.as_deref(), PAGE_SIZE)
        {
            Ok(page) => {
                let is_last = page.len() < PAGE_SIZE;
                for entity in &page {
                    let key_str = String::from_utf8_lossy(&entity.key).into_owned();
                    cursor = Some(key_str.clone());
                    match decode::<T>(&entity.value) {
                        Ok(value) => results.push(map_fn(value)),
                        Err(e) => {
                            warn!(
                                key = %key_str,
                                error = %e,
                                "Failed to decode profile during index rebuild"
                            );
                        },
                    }
                }
                if is_last {
                    break;
                }
            },
            Err(e) => {
                warn!(prefix = %prefix, error = %e, "Failed to scan profiles for index rebuild");
                break;
            },
        }
    }
    results
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use inferadb_ledger_state::system::{OrganizationStatus, OrganizationTier};
    use inferadb_ledger_store::{FileBackend, tables};
    use inferadb_ledger_types::{
        AppId, ClientAssertionId, ClientId, OrganizationId, OrganizationSlug, RefreshTokenId,
        Region, SigningKeyId, TeamId, UserEmailId, VaultId, VaultSlug, decode, encode,
    };
    use openraft::{CommittedLeaderId, LogId};
    use tempfile::tempdir;

    use super::*;
    use crate::types::{BlockRetentionPolicy, LedgerNodeId};

    /// Helper to create log IDs for tests.
    fn make_log_id(term: u64, index: u64) -> LogId<LedgerNodeId> {
        LogId::new(CommittedLeaderId::new(term, 0), index)
    }

    /// Build an `AppliedState` with realistic data across multiple organizations
    /// and vaults for round-trip testing.
    fn build_populated_state() -> AppliedState {
        let mut state = AppliedState {
            last_applied: Some(make_log_id(3, 42)),
            membership: openraft::StoredMembership::default(),
            region_height: 100,
            previous_region_hash: [0xAB; 32],
            sequences: SequenceCounters {
                organization: OrganizationId::new(10),
                vault: VaultId::new(20),
                user: UserId::new(30),
                user_email: UserEmailId::new(40),
                email_verify: EmailVerifyTokenId::new(50),
                team: TeamId::new(0),
                app: AppId::new(0),
                client_assertion: ClientAssertionId::new(0),
                signing_key: SigningKeyId::new(0),
                refresh_token: RefreshTokenId::new(0),
            },
            ..Default::default()
        };

        // 2 organizations
        for i in 1..=2i64 {
            let org_id = OrganizationId::new(i);
            let slug = OrganizationSlug::new(1000 + i as u64);
            state.organizations.insert(
                org_id,
                OrganizationMeta {
                    organization: org_id,
                    slug,
                    region: Region::GLOBAL,
                    status: OrganizationStatus::Active,
                    tier: OrganizationTier::Free,
                    pending_region: None,
                    storage_bytes: i as u64 * 1024,
                },
            );
            state.slug_index.insert(slug, org_id);
            state.id_to_slug.insert(org_id, slug);
            state.organization_storage_bytes.insert(org_id, i as u64 * 1024);
        }

        // 3 vaults per organization (6 total)
        for org_i in 1..=2i64 {
            let org_id = OrganizationId::new(org_i);
            for vault_i in 1..=3i64 {
                let vault_id = VaultId::new(org_i * 10 + vault_i);
                let slug = VaultSlug::new(2000 + vault_id.value() as u64);
                let meta = VaultMeta {
                    organization: org_id,
                    vault: vault_id,
                    slug,
                    name: Some(format!("vault-{org_i}-{vault_i}")),
                    deleted: false,
                    last_write_timestamp: 1000 + vault_id.value() as u64,
                    retention_policy: BlockRetentionPolicy::default(),
                };
                state.vaults.insert((org_id, vault_id), meta);
                state.vault_slug_index.insert(slug, vault_id);
                state.vault_id_to_slug.insert(vault_id, slug);

                state.vault_heights.insert((org_id, vault_id), vault_id.value() as u64 * 10);
                state
                    .previous_vault_hashes
                    .insert((org_id, vault_id), [vault_id.value() as u8; 32]);
            }
        }

        // Vault health for one vault
        state.vault_health.insert(
            (OrganizationId::new(1), VaultId::new(11)),
            VaultHealthStatus::Diverged { expected: [1u8; 32], computed: [2u8; 32], at_height: 5 },
        );

        // Client sequences
        for org_i in 1..=2i64 {
            for vault_i in 1..=2i64 {
                let org_id = OrganizationId::new(org_i);
                let vault_id = VaultId::new(org_i * 10 + vault_i);
                state.client_sequences.insert(
                    (org_id, vault_id, ClientId::new(format!("client-{org_i}-{vault_i}"))),
                    ClientSequenceEntry {
                        sequence: 100 + org_i as u64 * 10 + vault_i as u64,
                        ..ClientSequenceEntry::default()
                    },
                );
            }
        }

        state
    }

    /// Build `PendingExternalWrites` from an `AppliedState`.
    fn build_pending_from_state(state: &AppliedState) -> PendingExternalWrites {
        let mut pending = PendingExternalWrites::new();

        for (org_id, meta) in &state.organizations {
            let blob = encode(meta).unwrap();
            pending.organizations.push((*org_id, blob));
        }

        for ((_org_id, _vault_id), meta) in &state.vaults {
            let blob = encode(meta).unwrap();
            pending.vaults.push((meta.vault, blob));
        }

        for ((org_id, vault_id), height) in &state.vault_heights {
            pending.vault_heights.push(((*org_id, *vault_id), *height));
        }

        for ((org_id, vault_id), hash) in &state.previous_vault_hashes {
            pending.vault_hashes.push(((*org_id, *vault_id), *hash));
        }

        for ((org_id, vault_id), status) in &state.vault_health {
            pending.vault_health.push(((*org_id, *vault_id), status.clone()));
        }

        pending
            .sequences
            .push(("organization".to_string(), state.sequences.organization.value() as u64));
        pending.sequences.push(("vault".to_string(), state.sequences.vault.value() as u64));
        pending.sequences.push(("user".to_string(), state.sequences.user.value() as u64));
        pending
            .sequences
            .push(("user_email".to_string(), state.sequences.user_email.value() as u64));
        pending
            .sequences
            .push(("email_verify".to_string(), state.sequences.email_verify.value() as u64));
        pending.sequences.push(("team".to_string(), state.sequences.team.value() as u64));
        pending.sequences.push(("app".to_string(), state.sequences.app.value() as u64));
        pending.sequences.push((
            "client_assertion".to_string(),
            state.sequences.client_assertion.value() as u64,
        ));
        pending
            .sequences
            .push(("signing_key".to_string(), state.sequences.signing_key.value() as u64));
        pending
            .sequences
            .push(("refresh_token".to_string(), state.sequences.refresh_token.value() as u64));

        for ((org_id, vault_id, client_id), sequence) in &state.client_sequences {
            let key = PendingExternalWrites::client_sequence_key(
                *org_id,
                *vault_id,
                client_id.as_bytes(),
            );
            let value = encode(sequence).unwrap();
            pending.client_sequences.push((key, value));
        }

        for (slug, org_id) in &state.slug_index {
            pending.slug_index.push((*slug, *org_id));
        }

        for (slug, vault_id) in &state.vault_slug_index {
            pending.vault_slug_index.push((*slug, *vault_id));
        }

        for (slug, ids) in &state.team_slug_index {
            pending.team_slug_index.push((*slug, *ids));
        }

        for (slug, ids) in &state.app_slug_index {
            pending.app_slug_index.push((*slug, *ids));
        }

        pending
    }

    /// Compare two `AppliedState` instances field by field.
    fn assert_states_equal(left: &AppliedState, right: &AppliedState) {
        assert_eq!(left.last_applied, right.last_applied, "last_applied mismatch");
        assert_eq!(left.region_height, right.region_height, "region_height mismatch");
        assert_eq!(
            left.previous_region_hash, right.previous_region_hash,
            "previous_region_hash mismatch"
        );
        assert_eq!(left.sequences, right.sequences, "sequences mismatch");
        assert_eq!(
            left.organizations.len(),
            right.organizations.len(),
            "organizations count mismatch"
        );
        for (id, meta) in &left.organizations {
            let right_meta = right
                .organizations
                .get(id)
                .unwrap_or_else(|| panic!("missing organization {id:?}"));
            assert_eq!(meta.organization, right_meta.organization, "org {id:?} organization");
            assert_eq!(meta.slug, right_meta.slug, "org {id:?} slug");
            assert_eq!(meta.storage_bytes, right_meta.storage_bytes, "org {id:?} storage_bytes");
        }
        assert_eq!(left.vaults.len(), right.vaults.len(), "vaults count mismatch");
        for (key, meta) in &left.vaults {
            let right_meta =
                right.vaults.get(key).unwrap_or_else(|| panic!("missing vault {key:?}"));
            assert_eq!(meta.organization, right_meta.organization, "vault {key:?} organization");
            assert_eq!(meta.vault, right_meta.vault, "vault {key:?} vault");
            assert_eq!(meta.slug, right_meta.slug, "vault {key:?} slug");
            assert_eq!(meta.name, right_meta.name, "vault {key:?} name");
        }
        assert_eq!(left.vault_heights, right.vault_heights, "vault_heights mismatch");
        assert_eq!(
            left.previous_vault_hashes, right.previous_vault_hashes,
            "previous_vault_hashes mismatch"
        );
        assert_eq!(
            left.vault_health.len(),
            right.vault_health.len(),
            "vault_health count mismatch"
        );
        assert_eq!(left.client_sequences, right.client_sequences, "client_sequences mismatch");
        assert_eq!(left.slug_index, right.slug_index, "slug_index mismatch");
        assert_eq!(left.id_to_slug, right.id_to_slug, "id_to_slug (derived) mismatch");
        assert_eq!(left.vault_slug_index, right.vault_slug_index, "vault_slug_index mismatch");
        assert_eq!(
            left.vault_id_to_slug, right.vault_id_to_slug,
            "vault_id_to_slug (derived) mismatch"
        );
        assert_eq!(
            left.organization_storage_bytes, right.organization_storage_bytes,
            "organization_storage_bytes (derived) mismatch"
        );
        assert_eq!(left.team_slug_index, right.team_slug_index, "team_slug_index mismatch");
        assert_eq!(
            left.team_id_to_slug, right.team_id_to_slug,
            "team_id_to_slug (derived) mismatch"
        );
        assert_eq!(left.app_slug_index, right.app_slug_index, "app_slug_index mismatch");
        assert_eq!(left.app_id_to_slug, right.app_id_to_slug, "app_id_to_slug (derived) mismatch");
        assert_eq!(left.app_name_index, right.app_name_index, "app_name_index mismatch");
    }

    // ========================================================================
    // Round-trip: save_state_core → load_state_from_tables
    // ========================================================================

    #[test]
    fn test_save_load_round_trip_all_fields() {
        let dir = tempdir().unwrap();
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft.db")).unwrap();

        let original = build_populated_state();
        let pending = build_pending_from_state(&original);

        store.save_state_core(&original, &pending).unwrap();
        let loaded = store.load_state_from_tables().unwrap();

        assert_states_equal(&original, &loaded);
    }

    #[test]
    fn test_save_load_multiple_orgs_vaults_client_sequences() {
        let dir = tempdir().unwrap();
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft.db")).unwrap();

        let state = build_populated_state();
        let pending = build_pending_from_state(&state);

        store.save_state_core(&state, &pending).unwrap();
        let loaded = store.load_state_from_tables().unwrap();

        // Verify specific counts
        assert_eq!(loaded.organizations.len(), 2);
        assert_eq!(loaded.vaults.len(), 6);
        assert_eq!(loaded.client_sequences.len(), 4);
        assert_eq!(loaded.vault_heights.len(), 6);
        assert_eq!(loaded.previous_vault_hashes.len(), 6);
        assert_eq!(loaded.slug_index.len(), 2);
        assert_eq!(loaded.vault_slug_index.len(), 6);
        assert_eq!(loaded.id_to_slug.len(), 2);
        assert_eq!(loaded.vault_id_to_slug.len(), 6);
        assert_eq!(loaded.organization_storage_bytes.len(), 2);
    }

    // ========================================================================
    // Aborted WriteTransaction
    // ========================================================================

    #[test]
    fn test_aborted_write_transaction_leaves_state_unchanged() {
        let dir = tempdir().unwrap();
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft.db")).unwrap();

        // Write initial state
        let original = build_populated_state();
        let pending = build_pending_from_state(&original);
        store.save_state_core(&original, &pending).unwrap();

        // Open write txn, insert data, then DROP without commit
        {
            let mut write_txn = store.db.write().unwrap();
            write_txn
                .insert::<tables::OrganizationMeta>(
                    &999i64,
                    &encode(&OrganizationMeta {
                        organization: OrganizationId::new(999),
                        slug: OrganizationSlug::new(9999),
                        region: Region::GLOBAL,
                        status: OrganizationStatus::Active,
                        tier: OrganizationTier::Free,
                        pending_region: None,
                        storage_bytes: 0,
                    })
                    .unwrap(),
                )
                .unwrap();
            // Drop without commit — COW semantics discard changes
        }

        let loaded = store.load_state_from_tables().unwrap();
        assert_states_equal(&original, &loaded);
    }

    // ========================================================================
    // Corrupt data detection
    // ========================================================================

    #[test]
    fn test_corrupt_organization_meta_detected() {
        let dir = tempdir().unwrap();
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft.db")).unwrap();

        // Write valid state first
        let original = build_populated_state();
        let pending = build_pending_from_state(&original);
        store.save_state_core(&original, &pending).unwrap();

        // Corrupt one OrganizationMeta entry with invalid postcard bytes
        {
            let mut write_txn = store.db.write().unwrap();
            write_txn.insert::<tables::OrganizationMeta>(&1i64, &vec![0xFF, 0xFE, 0xFD]).unwrap();
            write_txn.commit().unwrap();
        }

        // load_state_from_tables should fail with a deserialization error
        let result = store.load_state_from_tables();
        assert!(result.is_err(), "should fail on corrupt OrganizationMeta");
    }

    // ========================================================================
    // Missing Sequences keys
    // ========================================================================

    #[test]
    fn test_missing_sequences_keys_default_to_zero() {
        let dir = tempdir().unwrap();
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft.db")).unwrap();

        // Write state then delete some sequence keys
        let original = build_populated_state();
        let pending = build_pending_from_state(&original);
        store.save_state_core(&original, &pending).unwrap();

        // Delete 2 of 5 sequence keys
        {
            let mut write_txn = store.db.write().unwrap();
            write_txn.delete::<tables::Sequences>(&"user_email".to_string()).unwrap();
            write_txn.delete::<tables::Sequences>(&"email_verify".to_string()).unwrap();
            write_txn.commit().unwrap();
        }

        // Load should succeed — missing keys get default values from SequenceCounters::new()
        let loaded = store.load_state_from_tables().unwrap();
        assert_eq!(loaded.sequences.organization, original.sequences.organization);
        assert_eq!(loaded.sequences.vault, original.sequences.vault);
        assert_eq!(loaded.sequences.user, original.sequences.user);
        // Missing keys: SequenceCounters::new() initializes these to 1 (the default initial value)
        // But since we initialized from Core which sets SequenceCounters::new() then only overwrote
        // from the Sequences table, the missing ones keep the SequenceCounters::new() defaults.
        assert_eq!(
            loaded.sequences.user_email,
            UserEmailId::new(1),
            "missing key defaults to SequenceCounters::new() initial value"
        );
        assert_eq!(
            loaded.sequences.email_verify,
            EmailVerifyTokenId::new(1),
            "missing key defaults to SequenceCounters::new() initial value"
        );
    }

    // ========================================================================
    // Vault keys use organization from VaultMeta blob
    // ========================================================================

    #[test]
    fn test_vault_keys_use_organization_from_blob() {
        let dir = tempdir().unwrap();
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft.db")).unwrap();

        let state = build_populated_state();
        let pending = build_pending_from_state(&state);
        store.save_state_core(&state, &pending).unwrap();

        let loaded = store.load_state_from_tables().unwrap();

        // Verify all 6 vaults use the correct organization from the deserialized blob
        for org_i in 1..=2i64 {
            let org_id = OrganizationId::new(org_i);
            for vault_i in 1..=3i64 {
                let vault_id = VaultId::new(org_i * 10 + vault_i);
                let key = (org_id, vault_id);
                let meta =
                    loaded.vaults.get(&key).unwrap_or_else(|| panic!("missing vault {key:?}"));
                assert_eq!(
                    meta.organization, org_id,
                    "vault {key:?} organization should come from blob"
                );
            }
        }
    }

    // ========================================================================
    // Fresh database returns default state
    // ========================================================================

    #[test]
    fn test_fresh_database_returns_default_state() {
        let dir = tempdir().unwrap();
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft.db")).unwrap();

        let loaded = store.load_state_from_tables().unwrap();

        assert_eq!(loaded.last_applied, None);
        assert_eq!(loaded.region_height, 0);
        assert_eq!(loaded.previous_region_hash, [0u8; 32]);
        assert!(loaded.organizations.is_empty());
        assert!(loaded.vaults.is_empty());
        assert!(loaded.vault_heights.is_empty());
        assert!(loaded.previous_vault_hashes.is_empty());
        assert!(loaded.vault_health.is_empty());
        assert!(loaded.client_sequences.is_empty());
        assert!(loaded.slug_index.is_empty());
        assert!(loaded.vault_slug_index.is_empty());
        assert!(loaded.id_to_slug.is_empty());
        assert!(loaded.vault_id_to_slug.is_empty());
        assert!(loaded.organization_storage_bytes.is_empty());
        // SequenceCounters::new() has initial values of 1
        assert_eq!(loaded.sequences, SequenceCounters::new());
    }

    // ========================================================================
    // Partial write failure (flush_external_writes atomicity)
    // ========================================================================

    #[test]
    fn test_partial_write_failure_atomicity() {
        let dir = tempdir().unwrap();
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft.db")).unwrap();

        // Commit initial state
        let original = build_populated_state();
        let pending = build_pending_from_state(&original);
        store.save_state_core(&original, &pending).unwrap();

        // Start a new write with additional data but drop without commit
        {
            let mut pending2 = PendingExternalWrites::new();
            pending2.organizations.push((
                OrganizationId::new(99),
                encode(&OrganizationMeta {
                    organization: OrganizationId::new(99),
                    slug: OrganizationSlug::new(9900),
                    region: Region::GLOBAL,
                    status: OrganizationStatus::Active,
                    tier: OrganizationTier::Free,
                    pending_region: None,
                    storage_bytes: 0,
                })
                .unwrap(),
            ));

            let mut write_txn = store.db.write().unwrap();
            RaftLogStore::<FileBackend>::flush_external_writes(&pending2, &mut write_txn).unwrap();
            // Drop without commit
        }

        // Original state should be intact
        let loaded = store.load_state_from_tables().unwrap();
        assert_eq!(loaded.organizations.len(), 2, "should still have original 2 orgs");
        assert!(
            !loaded.organizations.contains_key(&OrganizationId::new(99)),
            "uncommitted org should not be visible"
        );
    }

    // ========================================================================
    // Mixed inserts AND deletes in a single flush
    // ========================================================================

    #[test]
    fn test_flush_mixed_inserts_and_deletes() {
        let dir = tempdir().unwrap();
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft.db")).unwrap();

        // Initial state with 2 orgs, 6 vaults, slug indexes, client sequences
        let original = build_populated_state();
        let pending = build_pending_from_state(&original);
        store.save_state_core(&original, &pending).unwrap();

        // Now do mixed inserts + deletes in a single flush
        let mut mixed = PendingExternalWrites::new();

        // Insert new org
        let new_org_id = OrganizationId::new(3);
        let new_slug = OrganizationSlug::new(3000);
        mixed.organizations.push((
            new_org_id,
            encode(&OrganizationMeta {
                organization: new_org_id,
                slug: new_slug,
                region: Region::GLOBAL,
                status: OrganizationStatus::Active,
                tier: OrganizationTier::Free,
                pending_region: None,
                storage_bytes: 0,
            })
            .unwrap(),
        ));
        mixed.slug_index.push((new_slug, new_org_id));

        // Delete org 1
        mixed.organizations_deleted.push(OrganizationId::new(1));
        mixed.slug_index_deleted.push(OrganizationSlug::new(1001));

        // Delete a vault
        mixed.vaults_deleted.push(VaultId::new(11));
        mixed.vault_slug_index_deleted.push(VaultSlug::new(2011));

        // Delete a client sequence
        let cs_key = PendingExternalWrites::client_sequence_key(
            OrganizationId::new(1),
            VaultId::new(11),
            b"client-1-1",
        );
        mixed.client_sequences_deleted.push(cs_key);

        // Apply as a new state core save
        let mut updated = original.clone();
        // Apply the mutations to in-memory state for comparison
        updated.organizations.remove(&OrganizationId::new(1));
        updated.slug_index.remove(&OrganizationSlug::new(1001));
        updated.id_to_slug.remove(&OrganizationId::new(1));
        updated.organization_storage_bytes.remove(&OrganizationId::new(1));
        updated.organizations.insert(
            new_org_id,
            OrganizationMeta {
                organization: new_org_id,
                slug: new_slug,
                region: Region::GLOBAL,
                status: OrganizationStatus::Active,
                tier: OrganizationTier::Free,
                pending_region: None,
                storage_bytes: 0,
            },
        );
        updated.slug_index.insert(new_slug, new_org_id);
        updated.id_to_slug.insert(new_org_id, new_slug);
        updated.vaults.remove(&(OrganizationId::new(1), VaultId::new(11)));
        updated.vault_slug_index.remove(&VaultSlug::new(2011));
        updated.vault_id_to_slug.remove(&VaultId::new(11));
        updated.client_sequences.remove(&(
            OrganizationId::new(1),
            VaultId::new(11),
            ClientId::new("client-1-1"),
        ));

        // Write
        {
            let mut write_txn = store.db.write().unwrap();
            RaftLogStore::<FileBackend>::flush_external_writes(&mixed, &mut write_txn).unwrap();
            write_txn.commit().unwrap();
        }

        let loaded = store.load_state_from_tables().unwrap();

        // New org present
        assert!(loaded.organizations.contains_key(&new_org_id));
        // Old org 1 deleted
        assert!(!loaded.organizations.contains_key(&OrganizationId::new(1)));
        // Org 2 still present
        assert!(loaded.organizations.contains_key(&OrganizationId::new(2)));
        // Deleted vault absent
        assert!(!loaded.vaults.contains_key(&(OrganizationId::new(1), VaultId::new(11))));
        // Deleted client sequence absent
        assert!(!loaded.client_sequences.contains_key(&(
            OrganizationId::new(1),
            VaultId::new(11),
            ClientId::new("client-1-1")
        )));
        // Slug indexes updated
        assert!(loaded.slug_index.contains_key(&new_slug));
        assert!(!loaded.slug_index.contains_key(&OrganizationSlug::new(1001)));
    }

    // ========================================================================
    // Version sentinel correctness
    // ========================================================================

    #[test]
    fn test_version_sentinel_written_correctly() {
        let dir = tempdir().unwrap();
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft.db")).unwrap();

        let state = build_populated_state();
        let pending = build_pending_from_state(&state);
        store.save_state_core(&state, &pending).unwrap();

        // Read raw bytes and verify sentinel
        let read_txn = store.db.read().unwrap();
        let raw =
            read_txn.get::<tables::RaftState>(&KEY_APPLIED_STATE.to_string()).unwrap().unwrap();

        assert_eq!(&raw[..2], &[0x00, 0x01], "version sentinel");

        // Remaining bytes should deserialize to AppliedStateCore
        let core: AppliedStateCore = decode(&raw[2..]).unwrap();
        assert_eq!(core.last_applied, state.last_applied);
        assert_eq!(core.region_height, state.region_height);
        assert_eq!(core.previous_region_hash, state.previous_region_hash);
    }

    // ========================================================================
    // Empty PendingExternalWrites
    // ========================================================================

    #[test]
    fn test_save_with_empty_pending_writes() {
        let dir = tempdir().unwrap();
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft.db")).unwrap();

        let state = AppliedState {
            last_applied: Some(make_log_id(1, 5)),
            sequences: SequenceCounters::new(),
            ..Default::default()
        };
        let pending = PendingExternalWrites::new();

        store.save_state_core(&state, &pending).unwrap();
        let loaded = store.load_state_from_tables().unwrap();

        assert_eq!(loaded.last_applied, Some(make_log_id(1, 5)));
        assert!(loaded.organizations.is_empty());
        assert!(loaded.vaults.is_empty());
        // Sequences should be at initial values since nothing was written to Sequences table
        assert_eq!(loaded.sequences, SequenceCounters::new());
    }

    // ========================================================================
    // Large dataset round-trip: 1000 orgs, 5000 vaults, 100K client sequences
    // ========================================================================

    #[test]
    fn test_load_large_dataset() {
        let dir = tempdir().unwrap();
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft.db")).unwrap();

        let mut state = AppliedState {
            last_applied: Some(make_log_id(5, 1000)),
            region_height: 500,
            previous_region_hash: [0xFF; 32],
            sequences: SequenceCounters {
                organization: OrganizationId::new(1001),
                vault: VaultId::new(5001),
                user: UserId::new(10000),
                user_email: UserEmailId::new(10000),
                email_verify: EmailVerifyTokenId::new(10000),
                team: TeamId::new(0),
                app: AppId::new(0),
                client_assertion: ClientAssertionId::new(0),
                signing_key: SigningKeyId::new(0),
                refresh_token: RefreshTokenId::new(0),
            },
            ..Default::default()
        };

        // 1000 organizations
        for i in 1..=1000i64 {
            let org_id = OrganizationId::new(i);
            let slug = OrganizationSlug::new(i as u64 + 10000);
            state.organizations.insert(
                org_id,
                OrganizationMeta {
                    organization: org_id,
                    slug,
                    region: inferadb_ledger_types::ALL_REGIONS
                        [i as usize % inferadb_ledger_types::ALL_REGIONS.len()],
                    status: OrganizationStatus::Active,
                    tier: OrganizationTier::Free,
                    pending_region: None,
                    storage_bytes: i as u64 * 100,
                },
            );
            state.slug_index.insert(slug, org_id);
            state.id_to_slug.insert(org_id, slug);
            state.organization_storage_bytes.insert(org_id, i as u64 * 100);
        }

        // 5000 vaults (5 per org)
        for org_i in 1..=1000i64 {
            let org_id = OrganizationId::new(org_i);
            for v in 1..=5i64 {
                let vault_id = VaultId::new((org_i - 1) * 5 + v);
                let slug = VaultSlug::new(vault_id.value() as u64 + 20000);
                state.vaults.insert(
                    (org_id, vault_id),
                    VaultMeta {
                        organization: org_id,
                        vault: vault_id,
                        slug,
                        name: Some(format!("vault-{org_i}-{v}")),
                        deleted: false,
                        last_write_timestamp: 1000,
                        retention_policy: BlockRetentionPolicy::default(),
                    },
                );
                state.vault_slug_index.insert(slug, vault_id);
                state.vault_id_to_slug.insert(vault_id, slug);
                state.vault_heights.insert((org_id, vault_id), v as u64 * 10);
                state.previous_vault_hashes.insert((org_id, vault_id), [v as u8; 32]);
            }
        }

        // 100K client sequences (100 per org for first 1000 orgs, spread across vaults)
        for org_i in 1..=1000i64 {
            let org_id = OrganizationId::new(org_i);
            let vault_id = VaultId::new((org_i - 1) * 5 + 1);
            for c in 0..100u64 {
                state.client_sequences.insert(
                    (org_id, vault_id, ClientId::new(format!("c-{c}"))),
                    ClientSequenceEntry { sequence: c, ..ClientSequenceEntry::default() },
                );
            }
        }

        let pending = build_pending_from_state(&state);

        // Save and reload
        store.save_state_core(&state, &pending).unwrap();
        let loaded = store.load_state_from_tables().unwrap();

        // Verify counts
        assert_eq!(loaded.organizations.len(), 1000);
        assert_eq!(loaded.vaults.len(), 5000);
        assert_eq!(loaded.client_sequences.len(), 100_000);
    }

    // ========================================================================
    // Overwrite semantics: save_state_core replaces previous state
    // ========================================================================

    #[test]
    fn test_save_overwrites_previous_state() {
        let dir = tempdir().unwrap();
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft.db")).unwrap();

        // First save
        let state1 = build_populated_state();
        let pending1 = build_pending_from_state(&state1);
        store.save_state_core(&state1, &pending1).unwrap();

        // Second save with different state
        let mut state2 = AppliedState {
            last_applied: Some(make_log_id(10, 100)),
            region_height: 999,
            previous_region_hash: [0xCC; 32],
            sequences: SequenceCounters {
                organization: OrganizationId::new(50),
                vault: VaultId::new(60),
                user: UserId::new(70),
                user_email: UserEmailId::new(80),
                email_verify: EmailVerifyTokenId::new(90),
                team: TeamId::new(0),
                app: AppId::new(0),
                client_assertion: ClientAssertionId::new(0),
                signing_key: SigningKeyId::new(0),
                refresh_token: RefreshTokenId::new(0),
            },
            ..Default::default()
        };
        let org_id = OrganizationId::new(50);
        let slug = OrganizationSlug::new(5000);
        state2.organizations.insert(
            org_id,
            OrganizationMeta {
                organization: org_id,
                slug,
                region: Region::GLOBAL,
                status: OrganizationStatus::Active,
                tier: OrganizationTier::Free,
                pending_region: None,
                storage_bytes: 5000,
            },
        );
        state2.slug_index.insert(slug, org_id);
        state2.id_to_slug.insert(org_id, slug);
        state2.organization_storage_bytes.insert(org_id, 5000);

        let mut pending2 = build_pending_from_state(&state2);
        // Delete old data from state1
        for id in state1.organizations.keys() {
            pending2.organizations_deleted.push(*id);
        }
        for slug in state1.slug_index.keys() {
            pending2.slug_index_deleted.push(*slug);
        }
        for (_o, v) in state1.vaults.keys() {
            pending2.vaults_deleted.push(*v);
        }
        for slug in state1.vault_slug_index.keys() {
            pending2.vault_slug_index_deleted.push(*slug);
        }

        store.save_state_core(&state2, &pending2).unwrap();
        let loaded = store.load_state_from_tables().unwrap();

        assert_eq!(loaded.last_applied, Some(make_log_id(10, 100)));
        assert_eq!(loaded.region_height, 999);
        assert_eq!(loaded.organizations.len(), 1);
        assert!(loaded.organizations.contains_key(&org_id));
    }
}
