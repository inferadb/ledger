//! Apply logic, snapshot creation, and snapshot installation for [`RaftLogStore`].
//!
//! Contains the state machine apply path (adapted for [`CommittedEntry`]) and
//! snapshot builder/installer. The openraft trait implementations have been
//! removed — all consensus integration goes through [`CommittedEntry`] batches.

use std::{sync::Arc, time::Instant};

use chrono::DateTime;
use inferadb_ledger_consensus::{committed::CommittedEntry, types::EntryKind};
use inferadb_ledger_state::{BlockArchive, EventsDatabase, StateLayer};
use inferadb_ledger_store::{Database, FileBackend, TableId, tables};
use inferadb_ledger_types::{
    AppId, AppSlug, ClientAssertionId, EmailVerifyTokenId, OrganizationId, Region, TeamId,
    TeamSlug, UserEmailId, UserId, VaultId, decode, encode, events::EventConfig,
};

use super::{
    RegionChainState,
    store::RaftLogStore,
    types::{
        AppliedState, AppliedStateCore, LogId, PendingExternalWrites, SnapshotMeta, StoreError,
        StoredMembership,
    },
};
use crate::{
    metrics,
    snapshot::{
        SNAPSHOT_TABLE_IDS_ORG_RAFT, SNAPSHOT_TABLE_IDS_VAULT_BLOCKS,
        SNAPSHOT_TABLE_IDS_VAULT_RAFT, SNAPSHOT_TABLE_IDS_VAULT_STATE, SnapshotError,
        SnapshotReader, SnapshotScope, SnapshotWriter, SyncSnapshotReader, TableBlockKind,
        allowed_tables_for_block,
    },
};

/// Version sentinel prepended to the serialized `AppliedStateCore` in the
/// `RaftState` B+ tree table. Matches `RaftLogStore::STATE_CORE_VERSION`.
const STATE_CORE_VERSION_SENTINEL: [u8; 2] = [0x00, 0x01];

// ============================================================================
// Snapshot Builder
// ============================================================================

/// Builds a snapshot from the current state.
///
/// Created by [`RaftLogStore::get_snapshot_builder`]. Stage 1b bifurcates
/// snapshot creation into two scopes: an org-level snapshot captures the
/// org's `raft.db` only; a per-vault snapshot captures the vault's `raft.db`,
/// `state.db`, `blocks.db`, and apply-phase events. The scope is fixed at
/// construction time and persisted in the on-disk header so the install
/// path can refuse cross-scope installs.
pub struct LedgerSnapshotBuilder {
    /// The Raft log database — org-level `raft.db` for `Org` scope, the
    /// per-vault `raft.db` for `Vault` scope.
    db: Arc<Database<FileBackend>>,
    /// State layer for per-vault `state.db` access. Required for `Vault`
    /// scope (used via `state_layer.db_for(vault_id)` to read entity tables).
    /// Unused for `Org` scope.
    state_layer: Option<Arc<StateLayer<FileBackend>>>,
    /// Per-vault `BlockArchive` for `blocks.db` access. Required for `Vault`
    /// scope. Unused for `Org` scope (org-level groups never own a block
    /// archive — block storage lives on per-vault DBs per root rule 17).
    block_archive: Option<Arc<BlockArchive<FileBackend>>>,
    /// Events database for apply-phase event snapshot (separate from `db`).
    /// `Vault` scope uses this; `Org` scope leaves it `None` and writes a
    /// zero-length event section.
    events_db: Option<Arc<EventsDatabase<FileBackend>>>,
    /// Event config for snapshot event limits. `None` outside `Vault` scope.
    event_config: Option<EventConfig>,
    /// Per-scope snapshot encryption key provider.
    ///
    /// Threaded through Stage 1a; resolved by Stage 2's `SnapshotPersister`
    /// against [`SnapshotScope`] coordinates. The Stage 1b builder body
    /// does not encrypt — Stage 2's persister is the first dereferencing
    /// call site.
    #[allow(dead_code)]
    snapshot_key_provider: Arc<dyn crate::snapshot_key_provider::SnapshotKeyProvider>,
    /// Region this snapshot belongs to.
    region: Region,
    /// Organization the snapshot belongs to.
    organization_id: OrganizationId,
    /// Vault the snapshot belongs to (Vault scope only).
    vault_id: Option<VaultId>,
}

impl LedgerSnapshotBuilder {
    /// Constructs an org-scoped snapshot builder from raw `Arc` handles.
    ///
    /// Used by [`crate::snapshot_persister::SnapshotPersister`] when the
    /// `RaftLogStore` instance has been moved into the apply task and only
    /// the underlying handles are reachable from
    /// [`InnerGroup`](crate::raft_manager::InnerGroup). Equivalent to
    /// [`RaftLogStore::get_snapshot_builder`] for an org-scoped store.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn for_org_group(
        db: Arc<Database<FileBackend>>,
        events_db: Option<Arc<EventsDatabase<FileBackend>>>,
        event_config: Option<EventConfig>,
        snapshot_key_provider: Arc<dyn crate::snapshot_key_provider::SnapshotKeyProvider>,
        region: Region,
        organization_id: OrganizationId,
    ) -> Self {
        Self {
            db,
            state_layer: None,
            block_archive: None,
            events_db,
            event_config,
            snapshot_key_provider,
            region,
            organization_id,
            vault_id: None,
        }
    }

    /// Constructs a vault-scoped snapshot builder from raw `Arc` handles.
    ///
    /// Sibling of [`Self::for_org_group`] for the per-vault path. The
    /// `state_layer` and `block_archive` arguments are required —
    /// [`Self::build_snapshot`] returns an error if either is missing for
    /// a vault scope.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn for_vault_group(
        db: Arc<Database<FileBackend>>,
        state_layer: Arc<StateLayer<FileBackend>>,
        block_archive: Arc<BlockArchive<FileBackend>>,
        events_db: Option<Arc<EventsDatabase<FileBackend>>>,
        event_config: Option<EventConfig>,
        snapshot_key_provider: Arc<dyn crate::snapshot_key_provider::SnapshotKeyProvider>,
        region: Region,
        organization_id: OrganizationId,
        vault_id: VaultId,
    ) -> Self {
        Self {
            db,
            state_layer: Some(state_layer),
            block_archive: Some(block_archive),
            events_db,
            event_config,
            snapshot_key_provider,
            region,
            organization_id,
            vault_id: Some(vault_id),
        }
    }
}

impl LedgerSnapshotBuilder {
    /// Returns the [`SnapshotScope`] this builder will produce.
    pub fn scope(&self) -> SnapshotScope {
        match self.vault_id {
            Some(vault_id) => {
                SnapshotScope::Vault { organization_id: self.organization_id, vault_id }
            },
            None => SnapshotScope::Org { organization_id: self.organization_id },
        }
    }

    /// Returns the region this snapshot belongs to.
    pub fn region(&self) -> Region {
        self.region
    }

    /// Builds a snapshot file and returns the file handle plus metadata.
    ///
    /// Dispatches on [`scope`](Self::scope): an org-level builder reads only
    /// from `raft.db`; a vault builder reads from the vault's `raft.db`,
    /// `state.db`, `blocks.db`, and the apply-phase event stream.
    ///
    /// Forces `Database::sync_state` before reading so the snapshot captures
    /// a durable dual-slot-consistent state. Under the lazy-commit regime,
    /// `committed_state` can be ahead of the last synced disk state.
    /// Shipping a snapshot at an unsynced `applied` index to followers risks
    /// follower WAL truncation of entries the local node still needs on
    /// crash recovery.
    ///
    /// This narrows the race window rather than eliminating it: a concurrent
    /// apply on the `ApplyWorker` can land a `commit_in_memory` between this
    /// `sync_state` call and the subsequent `db.read()`, leaving the captured
    /// `last_applied` one or more entries ahead of the synced dual-slot. The
    /// `StateCheckpointer` re-syncs on its 500ms cadence, so the residual
    /// window collapses quickly under steady state.
    pub async fn build_snapshot(&mut self) -> Result<(tokio::fs::File, SnapshotMeta), StoreError> {
        match self.scope() {
            SnapshotScope::Org { .. } => self.build_org_snapshot().await,
            SnapshotScope::Vault { vault_id, .. } => self.build_vault_snapshot(vault_id).await,
        }
    }

    /// Builds an org-level snapshot. Reads org-scoped tables from `raft.db`
    /// plus org-scoped apply-phase events from the org's `events.db`.
    /// Entity / block / per-vault data lives on per-vault DBs (root rule
    /// 17) and is captured by per-vault snapshots.
    async fn build_org_snapshot(&mut self) -> Result<(tokio::fs::File, SnapshotMeta), StoreError> {
        Arc::clone(&self.db).sync_state().await.map_err(|e| to_storage_error(&e))?;

        // Org-scoped emissions (CreateVault, AddOrganizationMember, etc.)
        // land in the org's events.db. Collect them — `events_db` is None
        // for fixtures without an event writer.
        let last_ts = extract_last_applied_timestamp_ns(&self.db).unwrap_or(0);
        let event_entries = collect_snapshot_events(
            self.events_db.as_deref(),
            self.event_config.as_ref(),
            &collect_organization_ids_from_raft_db(&self.db).unwrap_or_default(),
            last_ts,
        );

        let scope = SnapshotScope::Org { organization_id: self.organization_id };
        write_org_snapshot_to_file(&self.db, scope, event_entries)
            .await
            .map_err(|e| StoreError::msg(e.to_string()))
    }

    /// Builds a per-vault snapshot from the four-source-DB layout.
    ///
    /// Returns [`SnapshotError::ScopeMismatch`] (wrapped in `StoreError`) if
    /// the builder was constructed without the per-vault dependencies wired
    /// (`state_layer` / `block_archive`); production callers always wire
    /// them, but the type system permits `None` so we precondition-check
    /// here rather than panicking.
    async fn build_vault_snapshot(
        &mut self,
        vault_id: VaultId,
    ) -> Result<(tokio::fs::File, SnapshotMeta), StoreError> {
        let state_layer = self.state_layer.as_ref().cloned().ok_or_else(|| {
            StoreError::msg(format!(
                "vault snapshot builder for vault {} missing state_layer dependency",
                vault_id.value()
            ))
        })?;
        let block_archive = self.block_archive.as_ref().cloned().ok_or_else(|| {
            StoreError::msg(format!(
                "vault snapshot builder for vault {} missing block_archive dependency",
                vault_id.value()
            ))
        })?;
        let state_db = state_layer
            .db_for(vault_id)
            .map_err(|e| StoreError::msg(format!("failed to open vault state.db: {e}")))?;
        let blocks_db = Arc::clone(block_archive.db());

        // Sync all four source DBs before the read transactions below.
        Arc::clone(&self.db).sync_state().await.map_err(|e| to_storage_error(&e))?;
        Arc::clone(&state_db).sync_state().await.map_err(|e| to_storage_error(&e))?;
        Arc::clone(&blocks_db).sync_state().await.map_err(|e| to_storage_error(&e))?;

        // Per-vault event collection. Vault snapshots scope events to the
        // owning organization (the apply-phase event stream is keyed by
        // organization_id; per-vault filtering happens at consumption time).
        let event_entries = collect_snapshot_events(
            self.events_db.as_deref(),
            self.event_config.as_ref(),
            &[self.organization_id],
            extract_last_applied_timestamp_ns(&self.db).unwrap_or(0),
        );

        let scope = SnapshotScope::Vault { organization_id: self.organization_id, vault_id };
        write_vault_snapshot_to_file(&self.db, &state_db, &blocks_db, event_entries, scope)
            .await
            .map_err(|e| StoreError::msg(e.to_string()))
    }
}

// ============================================================================
// Shared snapshot creation helper
// ============================================================================

/// Reads the on-disk `AppliedStateCore` blob from a Raft database and decodes
/// it. Tolerates the legacy whole-`AppliedState` shape (re-encoding via the
/// `From<&AppliedState>` projection) and the empty-state case (fresh DB).
fn read_applied_state_core(
    db: &Database<FileBackend>,
) -> Result<(AppliedStateCore, Vec<u8>), SnapshotError> {
    let read_txn = db.read().map_err(|e| SnapshotError::InvalidEntry {
        reason: format!("Failed to open read transaction: {e}"),
    })?;

    let state_data = read_txn
        .get::<tables::RaftState>(&super::KEY_APPLIED_STATE.to_string())
        .map_err(|e| SnapshotError::InvalidEntry {
            reason: format!("Failed to read AppliedStateCore from RaftState: {e}"),
        })?;

    match state_data {
        Some(data) if data.len() >= 2 && data[0..2] == STATE_CORE_VERSION_SENTINEL => {
            let core: AppliedStateCore =
                decode(&data[2..]).map_err(|e| SnapshotError::InvalidEntry {
                    reason: format!("Failed to decode AppliedStateCore: {e}"),
                })?;
            let core_bytes = encode(&core).map_err(|e| SnapshotError::InvalidEntry {
                reason: format!("Failed to re-encode AppliedStateCore: {e}"),
            })?;
            Ok((core, core_bytes))
        },
        Some(data) => {
            let state: AppliedState = decode(&data).map_err(|e| SnapshotError::InvalidEntry {
                reason: format!("Failed to decode old-format AppliedState: {e}"),
            })?;
            let core = AppliedStateCore::from(&state);
            let core_bytes = encode(&core).map_err(|e| SnapshotError::InvalidEntry {
                reason: format!("Failed to encode AppliedStateCore: {e}"),
            })?;
            Ok((core, core_bytes))
        },
        None => {
            let core = AppliedStateCore {
                last_applied: None,
                membership: StoredMembership::default(),
                region_height: 0,
                previous_region_hash: inferadb_ledger_types::Hash::default(),
                last_applied_timestamp_ns: 0,
            };
            let core_bytes = encode(&core).map_err(|e| SnapshotError::InvalidEntry {
                reason: format!("Failed to encode default AppliedStateCore: {e}"),
            })?;
            Ok((core, core_bytes))
        },
    }
}

/// Builds a deterministic snapshot id from the applied log index and current
/// wall-clock — same shape across org and vault paths.
fn build_snapshot_id(core: &AppliedStateCore) -> String {
    format!(
        "snapshot-{}-{}",
        core.last_applied.as_ref().map_or(0, |l| l.index),
        chrono::Utc::now().timestamp()
    )
}

/// One table's worth of pre-collected `(key, value)` entries.
type CollectedTableEntries = Vec<(Vec<u8>, Vec<u8>)>;

/// One table-block's worth of pre-collected `(table_id, entries)` pairs,
/// ready to stream into a `SnapshotWriter` block.
type CollectedTableBlock = Vec<(u8, CollectedTableEntries)>;

/// Collects all entries for the given table-id whitelist from a database, in
/// the same order the whitelist enumerates them.
fn collect_table_block(
    db: &Database<FileBackend>,
    table_ids: &[u8],
) -> Result<CollectedTableBlock, SnapshotError> {
    let read_txn = db.read().map_err(|e| SnapshotError::InvalidEntry {
        reason: format!("Failed to open read transaction: {e}"),
    })?;
    let mut out = Vec::with_capacity(table_ids.len());
    for &table_id_u8 in table_ids {
        let table_id = TableId::from_u8(table_id_u8)
            .ok_or(SnapshotError::UnknownTableId { table_id: table_id_u8 })?;
        let entries = iter_table_raw(&read_txn, table_id)?;
        out.push((table_id_u8, entries));
    }
    Ok(out)
}

/// Streams a single typed table block into the writer.
async fn write_table_block(
    writer: &mut SnapshotWriter<tokio::fs::File>,
    kind: TableBlockKind,
    tables: &CollectedTableBlock,
) -> Result<(), SnapshotError> {
    writer.write_table_block_header(kind, tables.len() as u32).await?;
    for (table_id_u8, entries) in tables {
        writer.write_table_header(*table_id_u8, entries.len() as u32).await?;
        for (key, value) in entries {
            writer.write_table_entry(key, value).await?;
        }
    }
    Ok(())
}

/// Creates a complete org-level snapshot file from the org's `raft.db`.
///
/// Org-scoped events (CreateVault, organization membership, etc.) live in
/// the org's `events.db` and are captured here; vault-scoped events live
/// on per-vault `events.db`s and are captured by [`write_vault_snapshot_to_file`].
async fn write_org_snapshot_to_file(
    raft_db: &Database<FileBackend>,
    scope: SnapshotScope,
    event_entries: Vec<Vec<u8>>,
) -> Result<(tokio::fs::File, SnapshotMeta), SnapshotError> {
    let (core, core_bytes) = read_applied_state_core(raft_db)?;
    let last_applied = core.last_applied;
    let membership = core.membership.clone();
    let snapshot_id = build_snapshot_id(&core);
    let raft_tables = collect_table_block(raft_db, SNAPSHOT_TABLE_IDS_ORG_RAFT)?;

    let std_file = tempfile::tempfile()
        .map_err(|e| SnapshotError::Io { source: e, location: snafu::location!() })?;
    let file = tokio::fs::File::from_std(std_file);
    let mut writer = SnapshotWriter::new(file);

    writer.write_header(scope, &core_bytes).await?;

    // Org snapshot: 1 table block (OrgRaft).
    writer.write_table_block_count(1).await?;
    write_table_block(&mut writer, TableBlockKind::OrgRaft, &raft_tables).await?;

    // Event section — org-scoped apply-phase events from the org's events.db.
    writer.write_event_count(event_entries.len() as u64).await?;
    for entry_bytes in &event_entries {
        writer.write_event_entry(entry_bytes).await?;
    }

    let mut file = writer.finish().await?;

    use tokio::io::AsyncSeekExt;
    file.seek(std::io::SeekFrom::Start(0))
        .await
        .map_err(|e| SnapshotError::Io { source: e, location: snafu::location!() })?;

    let meta = SnapshotMeta { last_log_id: last_applied, last_membership: membership, snapshot_id };

    tracing::info!(
        ?last_applied,
        organization_id = scope.organization_id().value(),
        raft_table_count = raft_tables.len(),
        event_count = event_entries.len(),
        "Org snapshot file created"
    );

    Ok((file, meta))
}

/// Creates a complete per-vault snapshot file from the vault's four source DBs.
async fn write_vault_snapshot_to_file(
    raft_db: &Database<FileBackend>,
    state_db: &Database<FileBackend>,
    blocks_db: &Database<FileBackend>,
    event_entries: Vec<Vec<u8>>,
    scope: SnapshotScope,
) -> Result<(tokio::fs::File, SnapshotMeta), SnapshotError> {
    let (core, core_bytes) = read_applied_state_core(raft_db)?;
    let last_applied = core.last_applied;
    let membership = core.membership.clone();
    let snapshot_id = build_snapshot_id(&core);

    let raft_tables = collect_table_block(raft_db, SNAPSHOT_TABLE_IDS_VAULT_RAFT)?;
    let state_tables = collect_table_block(state_db, SNAPSHOT_TABLE_IDS_VAULT_STATE)?;
    let blocks_tables = collect_table_block(blocks_db, SNAPSHOT_TABLE_IDS_VAULT_BLOCKS)?;

    let std_file = tempfile::tempfile()
        .map_err(|e| SnapshotError::Io { source: e, location: snafu::location!() })?;
    let file = tokio::fs::File::from_std(std_file);
    let mut writer = SnapshotWriter::new(file);

    writer.write_header(scope, &core_bytes).await?;

    // Vault snapshot: 3 table blocks (raft, state, blocks) — order is part of
    // the wire contract; install replays in the same order.
    writer.write_table_block_count(3).await?;
    write_table_block(&mut writer, TableBlockKind::VaultRaft, &raft_tables).await?;
    write_table_block(&mut writer, TableBlockKind::VaultState, &state_tables).await?;
    write_table_block(&mut writer, TableBlockKind::VaultBlocks, &blocks_tables).await?;

    writer.write_event_count(event_entries.len() as u64).await?;
    for entry_bytes in &event_entries {
        writer.write_event_entry(entry_bytes).await?;
    }

    let mut file = writer.finish().await?;

    use tokio::io::AsyncSeekExt;
    file.seek(std::io::SeekFrom::Start(0))
        .await
        .map_err(|e| SnapshotError::Io { source: e, location: snafu::location!() })?;

    let meta = SnapshotMeta { last_log_id: last_applied, last_membership: membership, snapshot_id };

    tracing::info!(
        ?last_applied,
        organization_id = scope.organization_id().value(),
        vault_id = scope.vault_id().map(|v| v.value()),
        raft_table_count = raft_tables.len(),
        state_table_count = state_tables.len(),
        blocks_table_count = blocks_tables.len(),
        event_count = event_entries.len(),
        "Vault snapshot file created"
    );

    Ok((file, meta))
}

/// Collects all organization ids materialised in the `OrganizationMeta`
/// table on a raft.db. Used by the org-snapshot path to drive event
/// collection. Returns an empty vec on read failure (snapshot is still
/// taken; events are best-effort).
fn collect_organization_ids_from_raft_db(
    db: &Database<FileBackend>,
) -> Option<Vec<inferadb_ledger_types::OrganizationId>> {
    let read_txn = db.read().ok()?;
    let iter = read_txn.iter::<tables::OrganizationMeta>().ok()?;
    Some(
        iter.filter_map(|(key, _)| {
            <i64 as inferadb_ledger_store::Key>::decode(&key)
                .map(inferadb_ledger_types::OrganizationId::new)
        })
        .collect(),
    )
}

/// Reads the `last_applied_timestamp_ns` from the `AppliedStateCore` if
/// available; returns `None` for fresh databases.
fn extract_last_applied_timestamp_ns(db: &Database<FileBackend>) -> Option<i64> {
    let read_txn = db.read().ok()?;
    let data =
        read_txn.get::<tables::RaftState>(&super::KEY_APPLIED_STATE.to_string()).ok().flatten()?;
    if data.len() >= 2 && data[0..2] == STATE_CORE_VERSION_SENTINEL {
        decode::<AppliedStateCore>(&data[2..]).ok().map(|core| core.last_applied_timestamp_ns)
    } else {
        None
    }
}

/// Iterates all entries in a table by its runtime `TableId`, returning raw
/// pre-encoded `(key, value)` byte pairs.
fn iter_table_raw(
    read_txn: &inferadb_ledger_store::ReadTransaction<'_, FileBackend>,
    table_id: TableId,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, SnapshotError> {
    macro_rules! collect_table {
        ($table:ty) => {{
            let iter = read_txn.iter::<$table>().map_err(|e| SnapshotError::InvalidEntry {
                reason: format!("Failed to iterate table {}: {e}", table_id as u8),
            })?;
            Ok(iter.collect())
        }};
    }

    match table_id {
        TableId::Entities => collect_table!(tables::Entities),
        TableId::Relationships => collect_table!(tables::Relationships),
        TableId::ObjIndex => collect_table!(tables::ObjIndex),
        TableId::SubjIndex => collect_table!(tables::SubjIndex),
        TableId::Blocks => collect_table!(tables::Blocks),
        TableId::VaultBlockIndex => collect_table!(tables::VaultBlockIndex),
        TableId::RaftLog => collect_table!(tables::RaftLog),
        TableId::RaftState => collect_table!(tables::RaftState),
        TableId::VaultMeta => collect_table!(tables::VaultMeta),
        TableId::OrganizationMeta => collect_table!(tables::OrganizationMeta),
        TableId::Sequences => collect_table!(tables::Sequences),
        TableId::ClientSequences => collect_table!(tables::ClientSequences),
        TableId::CompactionMeta => collect_table!(tables::CompactionMeta),
        TableId::OrganizationSlugIndex => collect_table!(tables::OrganizationSlugIndex),
        TableId::VaultSlugIndex => collect_table!(tables::VaultSlugIndex),
        TableId::UserSlugIndex => collect_table!(tables::UserSlugIndex),
        TableId::VaultHeights => collect_table!(tables::VaultHeights),
        TableId::VaultHashes => collect_table!(tables::VaultHashes),
        TableId::VaultHealth => collect_table!(tables::VaultHealth),
        TableId::TeamSlugIndex => collect_table!(tables::TeamSlugIndex),
        TableId::AppSlugIndex => collect_table!(tables::AppSlugIndex),
        TableId::StringDictionary => collect_table!(tables::StringDictionary),
        TableId::StringDictionaryReverse => collect_table!(tables::StringDictionaryReverse),
    }
}

/// Collects apply-phase events from events.db for snapshot inclusion.
///
/// Uses per-organization range scans with a timestamp cutoff derived from
/// the last applied entry's deterministic timestamp.
fn collect_snapshot_events(
    events_db: Option<&EventsDatabase<FileBackend>>,
    event_config: Option<&EventConfig>,
    org_ids: &[inferadb_ledger_types::OrganizationId],
    last_applied_timestamp_ns: i64,
) -> Vec<Vec<u8>> {
    let Some(edb) = events_db else {
        return Vec::new();
    };
    let max_events = event_config.map_or(10_000, |c| c.max_snapshot_events);
    let ttl_days = event_config.map_or(90, |c| c.default_ttl_days);

    let ttl_ns = i64::from(ttl_days) * 86_400 * 1_000_000_000;
    let cutoff_ns = last_applied_timestamp_ns.saturating_sub(ttl_ns).max(0) as u64;

    let read_txn = match edb.read() {
        Ok(txn) => txn,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to open events.db for snapshot");
            return Vec::new();
        },
    };

    let entries = match inferadb_ledger_state::EventStore::scan_apply_phase_ranged(
        &read_txn, org_ids, cutoff_ns, max_events,
    ) {
        Ok(entries) => entries,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to scan events for snapshot");
            return Vec::new();
        },
    };

    if !entries.is_empty() {
        tracing::debug!(
            count = entries.len(),
            max = max_events,
            org_count = org_ids.len(),
            cutoff_ns,
            "Collected apply-phase events for snapshot via range scan"
        );
    }

    entries
}

// ============================================================================
// Apply Logic (CommittedEntry-based)
// ============================================================================

/// Outcome of a call to [`RaftLogStore::replay_crash_gap`].
///
/// The caller (`RaftManager::start_region`) uses this to emit the
/// `ledger_state_recovery_*` metrics and log a single lifecycle line for
/// the recovery sweep. `replay_crash_gap` itself emits no metrics —
/// keeping the metrics contract at the caller avoids a circular
/// dependency between `log_storage` and the `metrics` module's
/// region-labelled helpers.
#[derive(Debug, Clone, Copy)]
pub struct RecoveryStats {
    /// Number of WAL entries replayed through `apply_committed_entries`.
    /// Zero on clean shutdown or empty WAL.
    pub replayed_entries: u64,
    /// Wall-clock time spent inside `replay_crash_gap`, including the
    /// post-replay `Database::sync_state` when any entries were replayed.
    pub duration: std::time::Duration,
    /// Highest log index durably captured in the synced state DB at the
    /// start of recovery (i.e. `applied_durable`).
    pub applied_durable: u64,
    /// Highest committed log index known to the WAL checkpoint.
    pub last_committed: u64,
}

impl RaftLogStore {
    /// Applies a batch of committed entries from the consensus engine.
    ///
    /// Generic over the tier-specific request type `R`: the apply pipeline
    /// decodes each entry as `RaftPayload<R>` and dispatches to the
    /// tier-specific apply method via [`ApplyableRequest::apply_on`].
    /// Callers specify `R` at construction time (one `ApplyWorker<R>` per
    /// Raft group); misrouting between tiers is a compile error.
    ///
    /// For each entry:
    /// - Normal entries: deserialize as `RaftPayload<R>`, apply via the tier-specific
    ///   `apply_*_request_with_events` method.
    /// - Membership entries: update `state.membership`.
    ///
    /// Returns a response for each entry in the batch.
    pub async fn apply_committed_entries<R>(
        &mut self,
        entries: &[CommittedEntry],
        leader_node: Option<u64>,
    ) -> Result<Vec<crate::types::LedgerResponse>, StoreError>
    where
        R: crate::log_storage::operations::ApplyableRequest,
    {
        let _span = tracing::info_span!(
            "apply_committed_entries",
            entry_count = entries.len(),
            region = self.region.as_str(),
        )
        .entered();

        let mut responses = Vec::new();
        let mut vault_entries = Vec::new();
        let current = self.applied_state.load_full();
        let mut state = (*current).clone();

        // Per-phase latency instrumentation. Labels stay low-cardinality
        // via `metrics::ApplyPhase` — see `record_apply_phase`. Pre-
        // stringify the ConsensusState once; cloning the region string is
        // cheap and keeps the record-site inline.
        let phase_region = self.region.as_str();
        let phase_org = self.organization_id().value().to_string();

        // Pre-decode all Normal entry payloads once. Membership entries get None.
        // This avoids redundant deserialization — the same payload was previously
        // decoded up to 3 times (timestamp extraction, commitment verification,
        // and the main apply loop).
        let decode_start = Instant::now();
        let decoded_payloads: Vec<Option<crate::types::RaftPayload<R>>> = entries
            .iter()
            .map(|e| match &e.kind {
                // Empty data = Raft no-op entry (§5.4.2) or barrier — skip decode.
                EntryKind::Normal if e.data.is_empty() => None,
                EntryKind::Normal => inferadb_ledger_types::decode(&e.data).ok(),
                EntryKind::Membership(_) => None,
            })
            .collect();
        crate::metrics::record_apply_phase(
            phase_region,
            &phase_org,
            crate::metrics::ApplyPhase::Decode,
            decode_start.elapsed().as_secs_f64(),
        );

        // Event accumulation — deterministic timestamp from leader's proposal.
        let block_timestamp = decoded_payloads
            .last()
            .and_then(|p| p.as_ref())
            .map_or(DateTime::UNIX_EPOCH, |p| p.proposed_at);
        let mut events = Vec::new();
        let mut op_index = 0u32;
        let mut pending = PendingExternalWrites::default();
        let ttl_days = self.event_writer.as_ref().map_or(0, |ew| ew.config().default_ttl_days);

        let committed_index = entries.last().map_or(0, |e| e.index);
        let term = entries.last().map_or(0, |e| e.term);

        // Read atomicity sentinel from state layer to detect entries that were
        // already committed before a crash. The sentinel is scoped to the
        // Raft group this log store owns — `vault_id == None` for the
        // parent organization's group, `Some(vault)` for a per-vault
        // group. Per-vault log indices are not comparable to the parent
        // org's log indices, so each Raft group records its own sentinel
        // under a distinct meta.db key (see `StateLayer::read_last_applied`).
        let sentinel_scope = self.vault_id;
        let state_layer_sentinel: Option<LogId> = self
            .state_layer
            .as_ref()
            .and_then(|sl| match sl.read_last_applied(sentinel_scope) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to read atomicity sentinel, all entries will be applied");
                    None
                },
            })
            .and_then(|bytes| match decode(&bytes) {
                Ok(v) => Some(v),
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to decode atomicity sentinel, all entries will be applied");
                    None
                },
            });

        let apply_loop_start = Instant::now();
        for (entry, decoded_payload) in entries.iter().zip(decoded_payloads.into_iter()) {
            let log_id = LogId::new(entry.term, 0, entry.index);

            // Verify state root commitments piggybacked from the previous batch.
            if let Some(ref payload) = decoded_payload {
                for commitment in &payload.state_root_commitments {
                    self.verify_state_root_commitment(commitment);
                }
            }

            state.last_applied = Some(log_id);

            let (response, vault_entry) = match &entry.kind {
                EntryKind::Normal if decoded_payload.is_none() => {
                    // No-op entry (Raft §5.4.2) or barrier — nothing to apply.
                    (crate::types::LedgerResponse::Empty, None)
                },
                EntryKind::Normal => {
                    let payload = decoded_payload.ok_or_else(|| {
                        StoreError::msg("failed to decode Normal entry payload".to_string())
                    })?;

                    let log_id_bytes = encode(&log_id).ok();
                    let skip_state_writes =
                        state_layer_sentinel.as_ref().is_some_and(|sentinel| log_id <= *sentinel);
                    if skip_state_writes {
                        tracing::info!(
                            log_id = %log_id,
                            "Skipping state layer writes for already-applied entry"
                        );
                    }
                    R::apply_on(
                        self,
                        &payload.request,
                        &mut state,
                        block_timestamp,
                        &mut op_index,
                        &mut events,
                        ttl_days,
                        &mut pending,
                        log_id_bytes.as_deref(),
                        skip_state_writes,
                        payload.caller,
                        // Defer state_root computation to post-loop
                        // amortization below — cuts per-entity state-root
                        // work to one call per unique vault per batch.
                        true,
                    )
                },
                EntryKind::Membership(consensus_membership) => {
                    let voter_ids = consensus_membership.voters.iter().map(|n| n.0).collect();
                    let learner_ids = consensus_membership
                        .learners
                        .iter()
                        .map(|n| n.0)
                        .collect::<std::collections::BTreeSet<u64>>();
                    state.membership = StoredMembership::new(Some(log_id), voter_ids, learner_ids);
                    (crate::types::LedgerResponse::Empty, None)
                },
            };

            responses.push(response);

            if let Some(entry) = vault_entry {
                vault_entries.push(entry);
            }
        }
        crate::metrics::record_apply_phase(
            phase_region,
            &phase_org,
            crate::metrics::ApplyPhase::ApplyLoop,
            apply_loop_start.elapsed().as_secs_f64(),
        );

        // Amortized + parallel state-root computation (Opt B + Opt
        // Parallel-apply): because `apply_request_with_events` was called
        // with `defer_state_root = true`, every accumulated
        // `VaultEntry.state_root` is currently `EMPTY_HASH` and every Write
        // response carries a stale placeholder `block_hash`.
        //
        // 1. Collect unique vault IDs (sorted for deterministic log output).
        // 2. Compute `state_root` per vault in parallel via rayon — `StorageEngine` supports
        //    concurrent read txns, so there is no contention between vaults. With N unique vaults
        //    and M rayon workers, this shrinks the phase from ~N × per-vault-work to ~N/M.
        // 3. Patch vault_entries' state_root field (serial, cheap).
        // 4. Compute per-entry `block_hash` in parallel — pure CPU, each entry independent.
        // 5. Patch Write response block_hashes (serial walk to preserve the original response
        //    ordering into BatchWrite.responses).
        //
        // Semantic model: within a batched commit, all vault heights for a
        // given vault share the post-batch state_root — they observe the
        // same atomic state. Prior per-entity state_roots described states
        // that existed for microseconds before the next op in the same
        // batch overwrote them; the batch-end root is the only one that
        // ever externally materialises on disk.
        if let Some(state_layer) = &self.state_layer
            && !vault_entries.is_empty()
        {
            use rayon::prelude::*;

            // Step 1: unique vault IDs (BTreeSet → sorted + deduped).
            let unique_vaults: Vec<inferadb_ledger_types::VaultId> = {
                let mut set: std::collections::BTreeSet<_> = std::collections::BTreeSet::new();
                for e in &vault_entries {
                    set.insert(e.vault);
                }
                set.into_iter().collect()
            };

            // Phase 7 / O3: per-vault state-root cache hit accounting.
            // Probe the commitment dirty-bit BEFORE computing — clean
            // commitments short-circuit inside `compute_state_root`, so a
            // pre-check is the only place we can label the call as a hit.
            // Opt-in via `vault_metrics_enabled()`; the helper itself
            // fast-paths to a no-op when off.
            for vault in &unique_vaults {
                if state_layer.state_root_is_cached(*vault) {
                    crate::metrics::record_vault_state_root_cache_hit(
                        phase_region,
                        &phase_org,
                        *vault,
                    );
                }
            }

            // Use the shared bounded apply pool (see
            // `inferadb_ledger_state::apply_pool`) rather than rayon's global
            // pool — the latter competes 1:1 with tokio workers and inflates
            // p99 tail under apply bursts.
            let pool = &*inferadb_ledger_state::apply_pool::APPLY_POOL;

            // Step 2: parallel compute_state_root across unique vaults.
            let state_root_start = Instant::now();
            let patched_roots: std::collections::HashMap<
                inferadb_ledger_types::VaultId,
                inferadb_ledger_types::Hash,
            > = pool.install(|| {
                unique_vaults
                    .par_iter()
                    .filter_map(|vault| match state_layer.compute_state_root(*vault) {
                        Ok(root) => Some((*vault, root)),
                        Err(e) => {
                            tracing::error!(
                                vault = %vault.value(),
                                error = %e,
                                "Deferred state_root computation failed; retaining placeholder hash"
                            );
                            None
                        },
                    })
                    .collect()
            });
            crate::metrics::record_apply_phase(
                phase_region,
                &phase_org,
                crate::metrics::ApplyPhase::StateRoot,
                state_root_start.elapsed().as_secs_f64(),
            );

            // Step 3: serial patch of vault_entries' state_root fields.
            for entry in &mut vault_entries {
                if let Some(root) = patched_roots.get(&entry.vault) {
                    entry.state_root = *root;
                }
            }

            // Step 4: parallel block_hash computation (pure CPU).
            let block_hash_start = Instant::now();
            let block_hashes: Vec<inferadb_ledger_types::Hash> = pool.install(|| {
                vault_entries.par_iter().map(|e| self.compute_vault_block_hash(e)).collect()
            });
            crate::metrics::record_apply_phase(
                phase_region,
                &phase_org,
                crate::metrics::ApplyPhase::BlockHash,
                block_hash_start.elapsed().as_secs_f64(),
            );

            // Step 5: serial patch of Write response block_hashes using the
            // pre-computed values. `vault_entries` mirrors the subset of
            // `responses` that produced a VaultEntry, in the same order; the
            // block_hashes vec is 1:1 with vault_entries. Walk both vectors
            // together.
            let mut bh_iter = block_hashes.iter();
            for response in responses.iter_mut() {
                if let crate::types::LedgerResponse::Write { block_hash, .. } = response {
                    if let Some(bh) = bh_iter.next() {
                        *block_hash = *bh;
                    }
                } else if let crate::types::LedgerResponse::BatchWrite { responses: inner } =
                    response
                {
                    for inner_resp in inner.iter_mut() {
                        if let crate::types::LedgerResponse::Write { block_hash, .. } = inner_resp
                            && let Some(bh) = bh_iter.next()
                        {
                            *block_hash = *bh;
                        }
                    }
                }
            }
        }

        // Create and store RegionBlock if we have vault entries
        if !vault_entries.is_empty() {
            let timestamp = block_timestamp;

            let chain_state = *self.region_chain.read();
            let new_region_height = chain_state.height + 1;

            let region_block = inferadb_ledger_types::RegionBlock {
                region: self.region,
                region_height: new_region_height,
                previous_region_hash: chain_state.previous_hash,
                vault_entries: vault_entries.clone(),
                timestamp,
                leader_id: self.node_id.clone(),
                term,
                committed_index,
            };

            if let Some(archive) = &self.block_archive {
                let archive_start = Instant::now();
                if let Err(e) = archive.append_block(&region_block) {
                    tracing::error!("Failed to store block: {}", e);
                }
                crate::metrics::record_apply_phase(
                    phase_region,
                    &phase_org,
                    crate::metrics::ApplyPhase::BlockArchive,
                    archive_start.elapsed().as_secs_f64(),
                );
            }

            // Broadcast block announcements for real-time subscribers.
            //
            // γ Phase 3a: slugs are read directly from the stamped `VaultEntry`
            // fields rather than looked up in `state.id_to_slug` /
            // `state.vault_id_to_slug`. The per-org `AppliedState` does not
            // own those maps — attempting to populate them inside the Write
            // apply arm broke state-root agreement in three earlier flip
            // attempts (`docs/superpowers/plans/2026-04-22-gamma-per-org-vault-allocation.md`).
            // For entries without a stamped slug (background jobs / saga /
            // system-vault writes leave the zero sentinel), fall back to the
            // internal id so existing tests that match on raw id still pass.
            let broadcast_start = Instant::now();
            if let Some(sender) = &self.block_announcements {
                for entry in &vault_entries {
                    let block_hash = inferadb_ledger_types::vault_entry_hash(entry);
                    let organization_slug = if entry.organization_slug.value() == 0 {
                        entry.organization.value() as u64
                    } else {
                        entry.organization_slug.value()
                    };
                    let vault_slug = if entry.vault_slug.value() == 0 {
                        entry.vault.value() as u64
                    } else {
                        entry.vault_slug.value()
                    };
                    let announcement = inferadb_ledger_proto::proto::BlockAnnouncement {
                        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                            slug: organization_slug,
                        }),
                        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault_slug }),
                        height: entry.vault_height,
                        block_hash: Some(inferadb_ledger_proto::proto::Hash {
                            value: block_hash.to_vec(),
                        }),
                        state_root: Some(inferadb_ledger_proto::proto::Hash {
                            value: entry.state_root.to_vec(),
                        }),
                        timestamp: Some(prost_types::Timestamp {
                            seconds: timestamp.timestamp(),
                            nanos: timestamp.timestamp_subsec_nanos() as i32,
                        }),
                    };
                    let _ = sender.send(announcement);
                    tracing::debug!(
                        organization_id = entry.organization.value(),
                        vault_id = entry.vault.value(),
                        height = entry.vault_height,
                        "Block announcement broadcast"
                    );
                }
            }
            crate::metrics::record_apply_phase(
                phase_region,
                &phase_org,
                crate::metrics::ApplyPhase::Broadcast,
                broadcast_start.elapsed().as_secs_f64(),
            );

            // Update previous vault hashes for each entry
            for entry in &vault_entries {
                let vault_block = region_block.extract_vault_block(
                    entry.organization,
                    entry.vault,
                    entry.vault_height,
                );
                if let Some(vb) = vault_block {
                    let block_hash = inferadb_ledger_types::hash::block_hash(&vb.header);
                    let key = (entry.organization, entry.vault);
                    state.previous_vault_hashes.insert(key, block_hash);
                    pending.vault_hashes.push((key, block_hash));
                }
            }

            // Update region chain tracking
            let region_hash =
                inferadb_ledger_types::sha256(&encode(&region_block).unwrap_or_default());
            *self.region_chain.write() =
                RegionChainState { height: new_region_height, previous_hash: region_hash };

            state.region_height = new_region_height;
            state.previous_region_hash = region_hash;

            // Buffer state root commitments for piggybacked verification.
            let commitments: Vec<crate::types::StateRootCommitment> = vault_entries
                .iter()
                .map(|e| crate::types::StateRootCommitment {
                    organization: e.organization,
                    vault: e.vault,
                    vault_height: e.vault_height,
                    state_root: e.state_root,
                })
                .collect();
            if let Ok(mut buf) = self.state_root_commitments.lock() {
                const MAX_COMMITMENT_BUFFER: usize = 10_000;
                let buf_len = buf.len();
                let available = MAX_COMMITMENT_BUFFER.saturating_sub(buf_len);
                if available < commitments.len() {
                    let to_drop = commitments.len().saturating_sub(available);
                    buf.drain(..to_drop.min(buf_len));
                }
                buf.extend(commitments);
            }
        }

        // Client sequence TTL eviction
        let eviction_interval = self.client_sequence_eviction.eviction_interval;
        let eviction_ttl = self.client_sequence_eviction.ttl_seconds;
        let should_evict =
            eviction_interval > 0 && entries.iter().any(|e| e.index % eviction_interval == 0);

        if should_evict {
            let proposed_at_secs = block_timestamp.timestamp();
            let mut expired_keys: Vec<(
                (
                    inferadb_ledger_types::OrganizationId,
                    inferadb_ledger_types::VaultId,
                    inferadb_ledger_types::ClientId,
                ),
                Vec<u8>,
            )> = state
                .client_sequences
                .iter()
                .filter(|(_, entry)| {
                    proposed_at_secs.saturating_sub(entry.last_seen) > eviction_ttl
                })
                .map(|(key, _)| {
                    let bytes_key =
                        PendingExternalWrites::client_sequence_key(key.0, key.1, key.2.as_bytes());
                    (key.clone(), bytes_key)
                })
                .collect();

            expired_keys.sort_by(|a, b| a.1.cmp(&b.1));

            for (map_key, bytes_key) in expired_keys {
                state.client_sequences.remove(&map_key);
                pending.client_sequences_deleted.push(bytes_key);
            }
        }

        // Record deterministic timestamp for snapshot event collection.
        state.last_applied_timestamp_ns = block_timestamp.timestamp_nanos_opt().unwrap_or(0);

        // Snapshot sequence counters into pending
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
        pending.sequences.push(("invite".to_string(), state.sequences.invite.value() as u64));

        // Persist core state blob + external table writes atomically
        self.save_state_core(&state, &pending)?;

        // Capture the applied index before moving state into Arc.
        let applied_index = state.last_applied.as_ref().map(|id| id.index);

        // Atomically publish the new state for lock-free readers
        self.applied_state.store(Arc::new(state));

        // Broadcast the latest applied index for ReadIndex protocol waiters.
        if let Some(index) = applied_index {
            let _ = self.applied_index_tx.send(index);
        }

        // Renew leader lease only on the leader node. Followers should not
        // maintain a valid lease — stale leases on followers could serve reads
        // that miss the latest committed writes.
        if leader_node == Some(self.ledger_node_id) {
            self.leader_lease.renew();
        }

        // Write accumulated events to events.db (best-effort)
        if !events.is_empty()
            && let Some(ew) = &self.event_writer
        {
            match ew.write_events(&events) {
                Ok(count) if count > 0 => {
                    tracing::debug!(
                        written = count,
                        total = events.len(),
                        "Apply-phase events persisted"
                    );
                },
                Ok(_) => {},
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        event_count = events.len(),
                        "Failed to persist apply-phase events"
                    );
                },
            }
        }

        Ok(responses)
    }

    /// Replays WAL entries from `(applied_durable, last_committed]` through the
    /// normal apply pipeline, then forces a [`Database::sync_state`] so the
    /// recovered state is durable before the node begins serving traffic.
    ///
    /// Closes the crash-recovery gap widened by the `commit_in_memory`
    /// apply path. `apply_committed_entries` commits in-memory and leaves
    /// durability to the periodic
    /// `StateCheckpointer`; on crash, up to ~500ms of committed applies can
    /// sit between the synced dual-slot and the WAL tail. This method
    /// re-drives those entries through the same apply pipeline a live shard
    /// would use, leaning on the idempotency audit captured in
    /// `docs/superpowers/specs/2026-04-19-sprint-1b2-apply-batching-design.md`.
    ///
    /// # Lifecycle
    ///
    /// Called by `RaftManager::start_region` AFTER [`RaftLogStore::open`] +
    /// builder wiring, and BEFORE [`ConsensusEngine::start`] consumes the WAL
    /// and the apply worker is spawned. The apply worker not being live
    /// during replay is load-bearing: it prevents concurrent modification of
    /// the same `applied_state` ArcSwap while we re-drive the pipeline.
    ///
    /// # Idempotency
    ///
    /// Replay is safe against partially-checkpointed batches: every field
    /// reconstructed by `apply_committed_entries` is either CAS-idempotent or
    /// monotonic-per-log-index, and `BlockArchive::append_block` is
    /// idempotent-by-height. See the design doc's "Replay idempotency audit"
    /// table for the field-by-field proof.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] if the WAL scan, any replayed apply, or the
    /// post-replay `sync_state` fails. A failure here should abort region
    /// startup — a region that can't recover must not start serving reads.
    pub async fn replay_crash_gap<W, R>(
        &mut self,
        wal: &W,
        shard_id: inferadb_ledger_consensus::types::ConsensusStateId,
    ) -> Result<RecoveryStats, StoreError>
    where
        W: inferadb_ledger_consensus::WalBackend,
        R: crate::log_storage::operations::ApplyableRequest,
    {
        use inferadb_ledger_consensus::recovery::recover_from_wal;

        let start = std::time::Instant::now();

        // 1. Read applied_durable. `RaftLogStore::open` runs `load_caches`,
        // which populates `applied_state` from the synced state DB, so the
        // ArcSwap already reflects disk state at this point.
        let applied_durable =
            self.applied_state.load().last_applied.as_ref().map_or(0, |id| id.index);

        // 2. Read last_committed via the consensus recovery helper. This
        // consults the WAL checkpoint and only surfaces entries in
        // (applied_durable, committed_index]. Treats a missing checkpoint
        // (fresh cluster, no commits yet) as a no-op.
        let mut applied_map = std::collections::HashMap::new();
        applied_map.insert(shard_id, applied_durable);

        let recovery = recover_from_wal(wal, &applied_map).map_err(|e| to_storage_error(&e))?;
        let last_committed = recovery.committed_index;

        // Fast-path: nothing to replay. Either fresh boot (no checkpoint),
        // clean shutdown (`sync_all_state_dbs` drove the gap to zero),
        // or a restart into an already-caught-up state DB.
        if recovery.replay_count == 0 {
            tracing::info!(
                region = self.region.as_str(),
                applied_durable,
                last_committed,
                "RaftLogStore::replay_crash_gap: no entries to replay",
            );
            return Ok(RecoveryStats {
                replayed_entries: 0,
                duration: start.elapsed(),
                applied_durable,
                last_committed,
            });
        }

        // 3. Slow-path: re-drive the apply pipeline. `recover_from_wal`
        // returns one batch per shard; for this region's shard we feed the
        // entries through in chunks sized to match the normal steady-state
        // apply batch. This keeps per-batch overhead (save_state_core, block
        // announcements, event flushes) amortized the same way live applies
        // experience.
        let mut total_replayed: u64 = 0;
        const REPLAY_CHUNK_SIZE: usize = 256;

        for batch in recovery.entries_to_replay {
            if batch.shard != shard_id {
                // Another shard's entries — not our responsibility. The
                // shard-owning region's `replay_crash_gap` handles them when
                // that region starts.
                continue;
            }
            if batch.entries.is_empty() {
                continue;
            }

            for chunk in batch.entries.chunks(REPLAY_CHUNK_SIZE) {
                // `leader_node = None` is correct for recovery: we're not
                // acting as leader during replay, and `apply_committed_entries`
                // uses `leader_node` only to decide whether to renew the
                // leader lease (which is meaningless for a node that hasn't
                // started serving yet).
                self.apply_committed_entries::<R>(chunk, None).await?;
                total_replayed = total_replayed.saturating_add(chunk.len() as u64);
            }
        }

        // 4. Post-replay sync: the replayed applies went through
        // `commit_in_memory` on every durability DB configured for this
        // region — raft.db (via `save_state_core` → `KEY_APPLIED_STATE`),
        // state.db (via `apply_request_with_events` →
        // entity/relationship tables), blocks.db (via
        // `BlockArchive::append_block`), and events.db (via
        // `EventWriter::write_events`). Force a `sync_state` on every
        // configured DB concurrently
        // so the recovered state is durably captured before the node
        // begins serving traffic — otherwise a second crash during the
        // server-startup window could regress state past the recovered
        // point again. Missing any DB's sync would specifically leak the
        // replayed writes to that DB.
        //
        // Test harnesses that only exercise the raft-log half may have
        // `state_layer` / `block_archive` / `event_writer` all unset; the
        // match below syncs whichever combination is configured.
        // Slice 2c: there is no longer a singleton state DB — each
        // vault owns its own `Database`. Snapshot the live vault set
        // and fan out per-vault `sync_state` alongside raft.db / blocks.db
        // / events.db. Test harnesses exercising the raft-log half alone
        // configure neither a state layer, block archive, nor event writer
        // and therefore skip every per-vault sync.
        let vault_dbs: Vec<(inferadb_ledger_types::VaultId, Arc<Database<FileBackend>>)> =
            self.state_layer.as_ref().map(|sl| sl.live_vault_dbs()).unwrap_or_default();
        let blocks_db_opt = self.block_archive.as_ref().map(|ba| Arc::clone(ba.db()));
        let events_db_opt = self.event_writer.as_ref().map(|ew| Arc::clone(ew.events_db().db()));

        let raft_db = Arc::clone(&self.db);

        // Launch every configured sync concurrently, then surface the
        // first error. Per-vault state DB syncs run in parallel
        // alongside raft.db / blocks.db / events.db so a region with
        // many materialised vaults doesn't serialise post-replay
        // durability.
        let vault_futs =
            futures::future::join_all(vault_dbs.iter().map(|(_, db)| Arc::clone(db).sync_state()));
        let raft_fut = raft_db.sync_state();
        let blocks_fut_opt = blocks_db_opt.as_ref().map(Arc::clone);
        let blocks_fut = async move {
            if let Some(db) = blocks_fut_opt { Some(db.sync_state().await) } else { None }
        };
        let events_fut_opt = events_db_opt.as_ref().map(Arc::clone);
        let events_fut = async move {
            if let Some(db) = events_fut_opt { Some(db.sync_state().await) } else { None }
        };

        let (vault_results, raft_res, blocks_res_opt, events_res_opt) =
            tokio::join!(vault_futs, raft_fut, blocks_fut, events_fut);

        log_replay_sync_outcome(&self.region, "raft", &raft_res);
        for ((vault, _), res) in vault_dbs.iter().zip(vault_results.iter()) {
            log_replay_sync_outcome(&self.region, &format!("state(vault-{})", vault.value()), res);
        }
        if let Some(ref res) = blocks_res_opt {
            log_replay_sync_outcome(&self.region, "blocks", res);
        }
        if let Some(ref res) = events_res_opt {
            log_replay_sync_outcome(&self.region, "events", res);
        }

        raft_res.map_err(|e| to_storage_error(&e))?;
        for res in vault_results {
            res.map_err(|e| to_storage_error(&e))?;
        }
        if let Some(res) = blocks_res_opt {
            res.map_err(|e| to_storage_error(&e))?;
        }
        if let Some(res) = events_res_opt {
            res.map_err(|e| to_storage_error(&e))?;
        }

        Ok(RecoveryStats {
            replayed_entries: total_replayed,
            duration: start.elapsed(),
            applied_durable,
            last_committed,
        })
    }

    /// Returns a snapshot builder.
    pub fn get_snapshot_builder(&self) -> LedgerSnapshotBuilder {
        LedgerSnapshotBuilder {
            db: Arc::clone(&self.db),
            state_layer: self.state_layer.as_ref().map(Arc::clone),
            block_archive: self.block_archive.as_ref().map(Arc::clone),
            events_db: self.event_writer.as_ref().map(|ew| Arc::clone(ew.events_db())),
            event_config: self.event_writer.as_ref().map(|ew| ew.config().clone()),
            snapshot_key_provider: Arc::clone(&self.snapshot_key_provider),
            region: self.region,
            organization_id: self.organization_id,
            vault_id: self.vault_id,
        }
    }

    /// Installs a snapshot received from the leader.
    ///
    /// Streams decompressed data directly into the appropriate B-trees and
    /// restores the in-memory state from the committed tables. Stage 1b
    /// dispatches on the snapshot's [`SnapshotScope`] header marker:
    ///
    /// - **Org snapshot** — writes restored rows into `self.db` (the org's `raft.db`) only.
    /// - **Vault snapshot** — writes vault raft tables into `self.db` (the per-vault `raft.db`),
    ///   vault state tables into the per-vault `state.db` (resolved via `state_layer.db_for(...)`),
    ///   vault block tables into the per-vault `blocks.db` (owned by the vault's `BlockArchive`),
    ///   and apply-phase events into the per-vault events DB.
    ///
    /// Cross-scope installs (org snapshot into vault store, vault into org,
    /// vault A into vault B) are rejected with
    /// [`SnapshotError::ScopeMismatch`] before any data is written.
    ///
    /// No `Database::sync_state` hook here. Installing a snapshot is a
    /// receive path (we're absorbing state shipped by the leader), not a
    /// ship path. The write transactions at the end of the streaming sync
    /// phase are themselves durable commits and capture the restored state
    /// on disk, so there is nothing local to flush first.
    pub async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Box<tokio::fs::File>,
    ) -> Result<(), StoreError> {
        use tokio::io::AsyncSeekExt;

        let mut file = *snapshot;

        // Async phase: verify SHA-256 checksum over compressed bytes.
        let file_size = file.seek(std::io::SeekFrom::End(0)).await.map_err(StoreError::from_io)?;
        file.seek(std::io::SeekFrom::Start(0)).await.map_err(StoreError::from_io)?;

        SnapshotReader::verify_checksum(&mut file, file_size)
            .await
            .map_err(|e| StoreError::msg(e.to_string()))?;

        // Convert to std::fs::File for synchronous streaming.
        let std_file = file.into_std().await;
        let compressed_size = file_size - crate::snapshot::CHECKSUM_SIZE as u64;

        // Resolve per-vault dependencies up-front. We don't know the snapshot
        // scope until we read the header, so we capture references for both
        // possible code paths.
        let raft_db = Arc::clone(&self.db);
        let store_vault_id = self.vault_id;
        let store_org_id = self.organization_id;
        let state_layer = self.state_layer.as_ref().cloned();
        let block_archive = self.block_archive.as_ref().cloned();
        let event_writer_ref = self.event_writer.as_ref();

        // For the vault path, eagerly resolve state.db / blocks.db handles
        // *if* this is a per-vault store. If the snapshot's scope turns out
        // to be Org, these handles are simply unused.
        let vault_state_db = match (store_vault_id, &state_layer) {
            (Some(vid), Some(sl)) => Some(
                sl.db_for(vid)
                    .map_err(|e| StoreError::msg(format!("failed to open vault state.db: {e}")))?,
            ),
            _ => None,
        };
        let vault_blocks_db = block_archive.as_ref().map(|a| Arc::clone(a.db()));

        let (core, scope, raft_table_count, state_table_count, blocks_table_count, event_count) =
            tokio::task::block_in_place(|| -> Result<_, StoreError> {
                use std::io::{BufReader, Read, Seek, SeekFrom};

                let mut file = std_file;
                file.seek(SeekFrom::Start(0)).map_err(StoreError::from_io)?;

                let reader = BufReader::new(file.take(compressed_size));
                let mut decoder =
                    zstd::stream::read::Decoder::new(reader).map_err(StoreError::from_io)?;

                // Read header → scope + AppliedStateCore.
                let (scope, core_bytes) = SyncSnapshotReader::read_header(&mut decoder)
                    .map_err(|e| StoreError::msg(e.to_string()))?;
                let core: AppliedStateCore =
                    decode(&core_bytes).map_err(|e| StoreError::from_error(&e))?;

                // Cross-scope rejection BEFORE writing any data.
                match (scope, store_vault_id) {
                    (SnapshotScope::Org { .. }, Some(vid)) => {
                        return Err(StoreError::msg(
                            SnapshotError::ScopeMismatch {
                                reason: format!(
                                    "org snapshot installed into per-vault store (vault_id={})",
                                    vid.value()
                                ),
                            }
                            .to_string(),
                        ));
                    },
                    (SnapshotScope::Vault { vault_id, .. }, None) => {
                        return Err(StoreError::msg(
                            SnapshotError::ScopeMismatch {
                                reason: format!(
                                    "vault snapshot (vault_id={}) installed into org-level store",
                                    vault_id.value()
                                ),
                            }
                            .to_string(),
                        ));
                    },
                    (SnapshotScope::Vault { vault_id, .. }, Some(store_vid))
                        if vault_id != store_vid =>
                    {
                        return Err(StoreError::msg(
                            SnapshotError::ScopeMismatch {
                                reason: format!(
                                    "vault snapshot for vault_id={} installed into store for \
                                     vault_id={}",
                                    vault_id.value(),
                                    store_vid.value()
                                ),
                            }
                            .to_string(),
                        ));
                    },
                    _ => {},
                }

                // Read the table-block count.
                let block_count = SyncSnapshotReader::read_u32(&mut decoder)
                    .map_err(|e| StoreError::msg(e.to_string()))?;

                // Track stats per block kind for the post-install log line.
                let mut raft_table_count: u32 = 0;
                let mut state_table_count: u32 = 0;
                let mut blocks_table_count: u32 = 0;

                // Open one write transaction per source DB.
                let mut raft_txn = raft_db.write().map_err(|e| to_storage_error(&e))?;
                let mut state_txn = match (&vault_state_db, scope) {
                    (Some(db), SnapshotScope::Vault { .. }) => {
                        Some(db.write().map_err(|e| to_storage_error(&e))?)
                    },
                    _ => None,
                };
                let mut blocks_txn = match (&vault_blocks_db, scope) {
                    (Some(db), SnapshotScope::Vault { .. }) => {
                        Some(db.write().map_err(|e| to_storage_error(&e))?)
                    },
                    _ => None,
                };

                for _ in 0..block_count {
                    let (kind, table_count) =
                        SyncSnapshotReader::read_table_block_header(&mut decoder)
                            .map_err(|e| StoreError::msg(e.to_string()))?;

                    // Defense-in-depth: enforce that block kind matches the
                    // snapshot scope. An org snapshot must contain only
                    // `OrgRaft`; a vault snapshot must contain only the
                    // three vault-tagged kinds.
                    let block_ok = matches!(
                        (scope, kind),
                        (SnapshotScope::Org { .. }, TableBlockKind::OrgRaft)
                            | (
                                SnapshotScope::Vault { .. },
                                TableBlockKind::VaultRaft
                                    | TableBlockKind::VaultState
                                    | TableBlockKind::VaultBlocks
                            )
                    );
                    if !block_ok {
                        return Err(StoreError::msg(
                            SnapshotError::ScopeMismatch {
                                reason: format!(
                                    "table block kind {kind:?} not allowed for snapshot scope \
                                     {scope:?}"
                                ),
                            }
                            .to_string(),
                        ));
                    }

                    let allowed = allowed_tables_for_block(kind);

                    for _ in 0..table_count {
                        let (table_id_u8, entry_count) =
                            SyncSnapshotReader::read_table_header(&mut decoder)
                                .map_err(|e| StoreError::msg(e.to_string()))?;

                        if !allowed.contains(&table_id_u8) {
                            return Err(StoreError::msg(
                                SnapshotError::InvalidEntry {
                                    reason: format!(
                                        "table_id {table_id_u8} not permitted in block {kind:?}"
                                    ),
                                }
                                .to_string(),
                            ));
                        }

                        let table_id = TableId::from_u8(table_id_u8).ok_or_else(|| {
                            StoreError::msg(format!("unknown table ID: {table_id_u8}"))
                        })?;

                        for _ in 0..entry_count {
                            let (key, value) = SyncSnapshotReader::read_kv_entry(&mut decoder)
                                .map_err(|e| StoreError::msg(e.to_string()))?;

                            match kind {
                                TableBlockKind::OrgRaft | TableBlockKind::VaultRaft => {
                                    raft_txn
                                        .insert_raw(table_id, &key, &value)
                                        .map_err(|e| to_storage_error(&e))?;
                                },
                                TableBlockKind::VaultState => {
                                    let txn = state_txn.as_mut().ok_or_else(|| {
                                        StoreError::msg(
                                            "vault snapshot install missing state.db transaction"
                                                .to_string(),
                                        )
                                    })?;
                                    txn.insert_raw(table_id, &key, &value)
                                        .map_err(|e| to_storage_error(&e))?;
                                },
                                TableBlockKind::VaultBlocks => {
                                    let txn = blocks_txn.as_mut().ok_or_else(|| {
                                        StoreError::msg(
                                            "vault snapshot install missing blocks.db transaction"
                                                .to_string(),
                                        )
                                    })?;
                                    txn.insert_raw(table_id, &key, &value)
                                        .map_err(|e| to_storage_error(&e))?;
                                },
                            }
                        }
                    }

                    match kind {
                        TableBlockKind::OrgRaft | TableBlockKind::VaultRaft => {
                            raft_table_count = raft_table_count.saturating_add(table_count);
                        },
                        TableBlockKind::VaultState => {
                            state_table_count = state_table_count.saturating_add(table_count);
                        },
                        TableBlockKind::VaultBlocks => {
                            blocks_table_count = blocks_table_count.saturating_add(table_count);
                        },
                    }
                }

                // Persist the AppliedStateCore blob with version sentinel into raft.db.
                let core_data = encode(&core).map_err(|e| to_serde_error(&e))?;
                let mut state_data =
                    Vec::with_capacity(STATE_CORE_VERSION_SENTINEL.len() + core_data.len());
                state_data.extend_from_slice(&STATE_CORE_VERSION_SENTINEL);
                state_data.extend_from_slice(&core_data);
                raft_txn
                    .insert::<tables::RaftState>(&super::KEY_APPLIED_STATE.to_string(), &state_data)
                    .map_err(|e| to_storage_error(&e))?;

                // Atomic commits — raft first, then per-vault DBs. Each
                // commit is independently durable; the AppliedStateCore in
                // raft.db is the recovery anchor, so committing it last
                // would mean a crash mid-install leaves the state machines
                // ahead of the apply progress.
                raft_txn.commit().map_err(|e| to_storage_error(&e))?;
                if let Some(txn) = state_txn {
                    txn.commit().map_err(|e| to_storage_error(&e))?;
                }
                if let Some(txn) = blocks_txn {
                    txn.commit().map_err(|e| to_storage_error(&e))?;
                }

                // Best-effort event restoration. Same shape as the v1 path:
                // failures here log + continue rather than failing the
                // install, since events are crash-replayable from the apply
                // pipeline once the state machine is back online.
                let event_count = SyncSnapshotReader::read_u64(&mut decoder)
                    .map_err(|e| StoreError::msg(e.to_string()))?;

                if event_count > 0
                    && let Some(ew) = event_writer_ref
                {
                    match ew.events_db().write() {
                        Ok(mut event_txn) => {
                            let mut written = 0usize;
                            for _ in 0..event_count {
                                match SyncSnapshotReader::read_event_entry(&mut decoder) {
                                    Ok(entry_bytes) => {
                                        match decode::<inferadb_ledger_types::events::EventEntry>(
                                            &entry_bytes,
                                        ) {
                                            Ok(entry) => {
                                                if inferadb_ledger_state::EventStore::write(
                                                    &mut event_txn,
                                                    &entry,
                                                )
                                                .is_ok()
                                                {
                                                    written += 1;
                                                }
                                            },
                                            Err(e) => {
                                                tracing::warn!(
                                                    error = %e,
                                                    "Failed to decode event entry from snapshot"
                                                );
                                            },
                                        }
                                    },
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            "Failed to read event entry from snapshot"
                                        );
                                        break;
                                    },
                                }
                            }
                            if let Err(e) = event_txn.commit() {
                                tracing::warn!(
                                    error = %e,
                                    "Failed to commit restored events from snapshot"
                                );
                            } else {
                                tracing::info!(
                                    count = written,
                                    total = event_count,
                                    "Restored apply-phase events from snapshot"
                                );
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "Failed to open write transaction for event snapshot restore"
                            );
                        },
                    }
                }

                Ok((
                    core,
                    scope,
                    raft_table_count,
                    state_table_count,
                    blocks_table_count,
                    event_count,
                ))
            })?;

        // Restore in-memory state from the committed tables.
        let loaded_state = self.load_applied_state_from_db(&core)?;
        self.applied_state.store(Arc::new(loaded_state));

        // Restore region chain tracking.
        *self.region_chain.write() = RegionChainState {
            height: core.region_height,
            previous_hash: core.previous_region_hash,
        };

        tracing::info!(
            snapshot_id = %meta.snapshot_id,
            last_log_id = ?meta.last_log_id,
            scope = ?scope,
            store_organization_id = store_org_id.value(),
            store_vault_id = store_vault_id.map(|v| v.value()),
            raft_table_count,
            state_table_count,
            blocks_table_count,
            event_count,
            "Snapshot installed (streaming)"
        );

        Ok(())
    }

    /// Gets the current snapshot, if any state has been applied.
    ///
    /// Dispatches on the store's residency identity: a per-vault store
    /// (`vault_id().is_some()`) produces a [`SnapshotScope::Vault`]
    /// snapshot; an org-scoped store produces a [`SnapshotScope::Org`]
    /// snapshot. Forces `Database::sync_state` before reading.
    pub async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<(tokio::fs::File, SnapshotMeta)>, StoreError> {
        Arc::clone(&self.db).sync_state().await.map_err(|e| to_storage_error(&e))?;

        // Check whether any state has been applied
        {
            let read_txn = self.db.read().map_err(|e| to_storage_error(&e))?;
            let has_state = read_txn
                .get::<tables::RaftState>(&super::KEY_APPLIED_STATE.to_string())
                .map_err(|e| to_storage_error(&e))?
                .is_some();
            if !has_state {
                return Ok(None);
            }
        }

        let mut builder = self.get_snapshot_builder();
        let (file, meta) = builder.build_snapshot().await?;
        Ok(Some((file, meta)))
    }

    /// Reconstructs the full `AppliedState` from an `AppliedStateCore` blob and
    /// the externalized B-tree tables. Used during snapshot installation to
    /// rebuild the in-memory state after table data has been committed.
    fn load_applied_state_from_db(
        &self,
        core: &AppliedStateCore,
    ) -> Result<AppliedState, StoreError> {
        let read_txn = self.db.read().map_err(|e| to_storage_error(&e))?;

        let mut state = AppliedState {
            last_applied: core.last_applied,
            membership: core.membership.clone(),
            region_height: core.region_height,
            previous_region_hash: core.previous_region_hash,
            last_applied_timestamp_ns: core.last_applied_timestamp_ns,
            ..Default::default()
        };

        // Load organizations
        let org_iter =
            read_txn.iter::<tables::OrganizationMeta>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in org_iter {
            if let Some(org_id_val) = inferadb_ledger_store::Key::decode(&key_bytes) {
                let org_id = inferadb_ledger_types::OrganizationId::new(org_id_val);
                match decode::<super::types::OrganizationMeta>(&value_bytes) {
                    Ok(meta) => {
                        state.id_to_slug.insert(org_id, meta.slug);
                        state.slug_index.insert(meta.slug, org_id);
                        state.organization_storage_bytes.insert(org_id, meta.storage_bytes);
                        state.organizations.insert(org_id, meta);
                    },
                    Err(e) => {
                        tracing::warn!(error = %e, ?org_id, "Skipping corrupt OrganizationMeta during state rebuild")
                    },
                }
            }
        }

        // Load vaults
        let vault_iter = read_txn.iter::<tables::VaultMeta>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in vault_iter {
            if let Some(vault_id_val) = <i64 as inferadb_ledger_store::Key>::decode(&key_bytes) {
                let vault_id = inferadb_ledger_types::VaultId::new(vault_id_val);
                match decode::<super::types::VaultMeta>(&value_bytes) {
                    Ok(meta) => {
                        state.vault_id_to_slug.insert((meta.organization, vault_id), meta.slug);
                        state.vault_slug_index.insert(meta.slug, (meta.organization, vault_id));
                        state.vaults.insert((meta.organization, vault_id), meta);
                    },
                    Err(e) => {
                        tracing::warn!(error = %e, ?vault_id, "Skipping corrupt VaultMeta during state rebuild")
                    },
                }
            }
        }

        // Load sequences
        let seq_iter = read_txn.iter::<tables::Sequences>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in seq_iter {
            if let (Some(name), Some(val)) = (
                <String as inferadb_ledger_store::Key>::decode(&key_bytes),
                <u64 as inferadb_ledger_store::Value>::decode(&value_bytes),
            ) {
                match name.as_str() {
                    "organization" => {
                        state.sequences.organization =
                            inferadb_ledger_types::OrganizationId::new(val as i64);
                    },
                    "vault" => {
                        state.sequences.vault = inferadb_ledger_types::VaultId::new(val as i64);
                    },
                    "user" => {
                        state.sequences.user = UserId::new(val as i64);
                    },
                    "user_email" => state.sequences.user_email = UserEmailId::new(val as i64),
                    "email_verify" => {
                        state.sequences.email_verify = EmailVerifyTokenId::new(val as i64);
                    },
                    "team" => {
                        state.sequences.team = TeamId::new(val as i64);
                    },
                    "app" => {
                        state.sequences.app = AppId::new(val as i64);
                    },
                    "client_assertion" => {
                        state.sequences.client_assertion = ClientAssertionId::new(val as i64);
                    },
                    _ => {},
                }
            }
        }

        // Load vault heights
        let height_iter =
            read_txn.iter::<tables::VaultHeights>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in height_iter {
            if key_bytes.len() == 16 {
                let org_id = inferadb_ledger_types::OrganizationId::new(i64::from_be_bytes(
                    key_bytes[..8].try_into().unwrap_or([0; 8]),
                ));
                let vault_id = inferadb_ledger_types::VaultId::new(i64::from_be_bytes(
                    key_bytes[8..16].try_into().unwrap_or([0; 8]),
                ));
                match decode::<u64>(&value_bytes) {
                    Ok(height) => {
                        state.vault_heights.insert((org_id, vault_id), height);
                    },
                    Err(e) => {
                        tracing::warn!(error = %e, "Skipping corrupt vault height during state rebuild")
                    },
                }
            }
        }

        // Load vault hashes
        let hash_iter = read_txn.iter::<tables::VaultHashes>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in hash_iter {
            if key_bytes.len() == 16 && value_bytes.len() == 32 {
                let org_id = inferadb_ledger_types::OrganizationId::new(i64::from_be_bytes(
                    key_bytes[..8].try_into().unwrap_or([0; 8]),
                ));
                let vault_id = inferadb_ledger_types::VaultId::new(i64::from_be_bytes(
                    key_bytes[8..16].try_into().unwrap_or([0; 8]),
                ));
                let hash: inferadb_ledger_types::Hash =
                    value_bytes.as_slice().try_into().unwrap_or([0; 32]);
                state.previous_vault_hashes.insert((org_id, vault_id), hash);
            }
        }

        // Load vault health
        let health_iter =
            read_txn.iter::<tables::VaultHealth>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in health_iter {
            if key_bytes.len() == 16 {
                let org_id = inferadb_ledger_types::OrganizationId::new(i64::from_be_bytes(
                    key_bytes[..8].try_into().unwrap_or([0; 8]),
                ));
                let vault_id = inferadb_ledger_types::VaultId::new(i64::from_be_bytes(
                    key_bytes[8..16].try_into().unwrap_or([0; 8]),
                ));
                match decode::<super::types::VaultHealthStatus>(&value_bytes) {
                    Ok(status) => {
                        state.vault_health.insert((org_id, vault_id), status);
                    },
                    Err(e) => {
                        tracing::warn!(error = %e, "Skipping corrupt VaultHealthStatus during state rebuild")
                    },
                }
            }
        }

        // Load client sequences
        let cs_iter =
            read_txn.iter::<tables::ClientSequences>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in cs_iter {
            if key_bytes.len() >= 16 {
                let org_id = inferadb_ledger_types::OrganizationId::new(i64::from_be_bytes(
                    key_bytes[..8].try_into().unwrap_or([0; 8]),
                ));
                let vault_id = inferadb_ledger_types::VaultId::new(i64::from_be_bytes(
                    key_bytes[8..16].try_into().unwrap_or([0; 8]),
                ));
                let client_id = match String::from_utf8(key_bytes[16..].to_vec()) {
                    Ok(id) => inferadb_ledger_types::ClientId::new(id),
                    Err(e) => {
                        tracing::warn!(error = %e, "Skipping ClientSequence with invalid UTF-8 client_id during state rebuild");
                        continue;
                    },
                };
                match decode::<super::types::ClientSequenceEntry>(&value_bytes) {
                    Ok(entry) => {
                        state.client_sequences.insert((org_id, vault_id, client_id), entry);
                    },
                    Err(e) => {
                        tracing::warn!(error = %e, "Skipping corrupt ClientSequenceEntry during state rebuild")
                    },
                }
            }
        }

        // Load slug indexes
        let slug_iter =
            read_txn.iter::<tables::OrganizationSlugIndex>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in slug_iter {
            if let (Some(slug_val), Some(org_id_val)) = (
                <u64 as inferadb_ledger_store::Key>::decode(&key_bytes),
                decode::<i64>(&value_bytes).ok(),
            ) {
                let slug = inferadb_ledger_types::OrganizationSlug::new(slug_val);
                let org_id = inferadb_ledger_types::OrganizationId::new(org_id_val);
                state.slug_index.insert(slug, org_id);
                state.id_to_slug.insert(org_id, slug);
            }
        }

        let vault_slug_iter =
            read_txn.iter::<tables::VaultSlugIndex>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in vault_slug_iter {
            // Values are `(OrganizationId, VaultId)` tuples post-γ.
            if let (Some(slug_val), Some(pair)) = (
                <u64 as inferadb_ledger_store::Key>::decode(&key_bytes),
                decode::<(inferadb_ledger_types::OrganizationId, inferadb_ledger_types::VaultId)>(
                    &value_bytes,
                )
                .ok(),
            ) {
                let slug = inferadb_ledger_types::VaultSlug::new(slug_val);
                state.vault_slug_index.insert(slug, pair);
                state.vault_id_to_slug.insert(pair, slug);
            }
        }

        let user_slug_iter =
            read_txn.iter::<tables::UserSlugIndex>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in user_slug_iter {
            if let (Some(slug_val), Some(user_id_val)) = (
                <u64 as inferadb_ledger_store::Key>::decode(&key_bytes),
                decode::<i64>(&value_bytes).ok(),
            ) {
                let slug = inferadb_ledger_types::UserSlug::new(slug_val);
                let user_id = inferadb_ledger_types::UserId::new(user_id_val);
                state.user_slug_index.insert(slug, user_id);
                state.user_id_to_slug.insert(user_id, slug);
            }
        }

        // Load team slug index
        let team_slug_iter =
            read_txn.iter::<tables::TeamSlugIndex>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in team_slug_iter {
            if let (Some(slug_val), Ok((org_id, team_id))) = (
                <u64 as inferadb_ledger_store::Key>::decode(&key_bytes),
                decode::<(inferadb_ledger_types::OrganizationId, TeamId)>(&value_bytes),
            ) {
                let slug = TeamSlug::new(slug_val);
                state.team_slug_index.insert(slug, (org_id, team_id));
                state.team_id_to_slug.insert(team_id, slug);
            }
        }

        // Load app slug index
        let app_slug_iter =
            read_txn.iter::<tables::AppSlugIndex>().map_err(|e| to_storage_error(&e))?;
        for (key_bytes, value_bytes) in app_slug_iter {
            if let (Some(slug_val), Ok((org_id, app_id))) = (
                <u64 as inferadb_ledger_store::Key>::decode(&key_bytes),
                decode::<(inferadb_ledger_types::OrganizationId, AppId)>(&value_bytes),
            ) {
                let slug = AppSlug::new(slug_val);
                state.app_slug_index.insert(slug, (org_id, app_id));
                state.app_id_to_slug.insert(app_id, slug);
            }
        }

        Ok(state)
    }

    /// Verifies a leader's state root commitment against the local block archive.
    fn verify_state_root_commitment(&self, commitment: &crate::types::StateRootCommitment) {
        let archive = match &self.block_archive {
            Some(a) => a,
            None => return,
        };

        let region_height = match archive.find_region_height(
            commitment.organization,
            commitment.vault,
            commitment.vault_height,
        ) {
            Ok(Some(h)) => h,
            Ok(None) => return,
            Err(e) => {
                tracing::warn!(
                    organization = commitment.organization.value(),
                    vault = commitment.vault.value(),
                    vault_height = commitment.vault_height,
                    error = %e,
                    "Failed to look up vault entry for state root verification"
                );
                return;
            },
        };

        let block = match archive.read_block(region_height) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    region_height,
                    error = %e,
                    "Failed to read block for state root verification"
                );
                return;
            },
        };

        for entry in &block.vault_entries {
            if entry.organization == commitment.organization
                && entry.vault == commitment.vault
                && entry.vault_height == commitment.vault_height
            {
                if entry.state_root == commitment.state_root {
                    metrics::record_state_root_verification();
                } else {
                    let local_hex: String = inferadb_ledger_types::bytes_to_hex(&entry.state_root);
                    let leader_hex: String =
                        inferadb_ledger_types::bytes_to_hex(&commitment.state_root);
                    tracing::error!(
                        organization = commitment.organization.value(),
                        vault = commitment.vault.value(),
                        vault_height = commitment.vault_height,
                        local_state_root = %local_hex,
                        leader_state_root = %leader_hex,
                        region = self.region.as_str(),
                        "STATE ROOT DIVERGENCE DETECTED: local state root does not match leader commitment"
                    );
                    metrics::record_state_root_divergence(
                        commitment.organization,
                        commitment.vault,
                    );

                    if let Some(sender) = &self.divergence_sender {
                        let _ = sender.send(crate::types::StateRootDivergence {
                            organization: commitment.organization,
                            vault: commitment.vault,
                            vault_height: commitment.vault_height,
                            local_state_root: entry.state_root,
                            leader_state_root: commitment.state_root,
                        });
                    }
                }
                return;
            }
        }
    }
}

// ============================================================================
// Error Helpers
// ============================================================================

pub(super) fn to_storage_error<E: std::error::Error>(e: &E) -> StoreError {
    StoreError::from_error(e)
}

pub(super) fn to_serde_error<E: std::error::Error>(e: &E) -> StoreError {
    StoreError::from_error(e)
}

/// Emits one `info!`/`warn!` per DB for the post-replay sync fan-out in
/// [`RaftLogStore::replay_crash_gap`]. Returns nothing — the caller
/// propagates the underlying `Result` separately.
fn log_replay_sync_outcome<E: std::fmt::Display>(
    region: &inferadb_ledger_types::Region,
    db: &str,
    result: &Result<(), E>,
) {
    match result {
        Ok(()) => tracing::info!(
            region = region.as_str(),
            db,
            "replay_crash_gap: post-replay sync complete"
        ),
        Err(e) => tracing::warn!(
            region = region.as_str(),
            db,
            error = %e,
            "replay_crash_gap: post-replay sync failed — recovery will propagate error"
        ),
    }
}
