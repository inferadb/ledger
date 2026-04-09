//! Apply logic, snapshot creation, and snapshot installation for [`RaftLogStore`].
//!
//! Contains the state machine apply path (adapted for [`CommittedEntry`]) and
//! snapshot builder/installer. The openraft trait implementations have been
//! removed — all consensus integration goes through [`CommittedEntry`] batches.

use std::sync::Arc;

use chrono::DateTime;
use inferadb_ledger_consensus::{committed::CommittedEntry, types::EntryKind};
use inferadb_ledger_state::EventsDatabase;
use inferadb_ledger_store::{FileBackend, TableId, tables};
use inferadb_ledger_types::{
    AppId, AppSlug, ClientAssertionId, EmailVerifyTokenId, TeamId, TeamSlug, UserEmailId, UserId,
    decode, encode, events::EventConfig,
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
        SNAPSHOT_TABLE_IDS, SnapshotError, SnapshotReader, SnapshotWriter, SyncSnapshotReader,
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
/// Created by `RaftLogStore::get_snapshot_builder()`. Contains references to
/// the database and events layer needed for snapshot creation.
pub struct LedgerSnapshotBuilder {
    /// Shared B-tree database (log + state tables).
    db: Arc<inferadb_ledger_store::Database<FileBackend>>,
    /// State layer for entity/relationship storage.
    #[allow(dead_code)]
    state_layer: Option<Arc<inferadb_ledger_state::StateLayer<FileBackend>>>,
    /// Events database for apply-phase event snapshot (separate from main db).
    events_db: Option<Arc<EventsDatabase<FileBackend>>>,
    /// Event config for snapshot event limits.
    event_config: Option<EventConfig>,
}

impl LedgerSnapshotBuilder {
    /// Builds a snapshot file and returns the file handle plus metadata.
    pub async fn build_snapshot(&mut self) -> Result<(tokio::fs::File, SnapshotMeta), StoreError> {
        let (org_ids, last_ts) = extract_snapshot_event_context(&self.db)?;

        let event_entries = collect_snapshot_events(
            self.events_db.as_deref(),
            self.event_config.as_ref(),
            &org_ids,
            last_ts,
        );

        write_snapshot_to_file(&self.db, event_entries)
            .await
            .map_err(|e| StoreError::msg(e.to_string()))
    }
}

// ============================================================================
// Shared snapshot creation helper
// ============================================================================

/// Creates a complete snapshot file and returns the open file handle plus metadata.
///
/// Separates sync B-tree reads from async file I/O for `Send` safety:
/// 1. (sync) Open `ReadTransaction`, read `AppliedStateCore` + table + entity data
/// 2. (async) Write all pre-collected data to the snapshot file via `SnapshotWriter`
///
/// All data comes from a single `ReadTransaction` for transactional consistency.
///
/// Event entries must be collected by the caller before invoking this function,
/// because `EventsDatabase` contains raw pointers (`!Send`). Passing pre-collected
/// `Vec<Vec<u8>>` keeps this async function's future `Send`-safe.
async fn write_snapshot_to_file(
    db: &inferadb_ledger_store::Database<FileBackend>,
    event_entries: Vec<Vec<u8>>,
) -> Result<(tokio::fs::File, SnapshotMeta), SnapshotError> {
    // --- Sync phase: collect all data from a single ReadTransaction ---

    let (core_bytes, last_applied, membership, snapshot_id, table_data, entity_entries) = {
        let read_txn = db.read().map_err(|e| SnapshotError::InvalidEntry {
            reason: format!("Failed to open read transaction: {e}"),
        })?;

        // 1. Read AppliedStateCore from the RaftState table
        let state_data = read_txn
            .get::<tables::RaftState>(&super::KEY_APPLIED_STATE.to_string())
            .map_err(|e| SnapshotError::InvalidEntry {
            reason: format!("Failed to read AppliedStateCore from RaftState: {e}"),
        })?;

        let (core, core_bytes) = match state_data {
            Some(data) if data.len() >= 2 && data[0..2] == STATE_CORE_VERSION_SENTINEL => {
                // New format: version sentinel + postcard(AppliedStateCore)
                let core: AppliedStateCore =
                    decode(&data[2..]).map_err(|e| SnapshotError::InvalidEntry {
                        reason: format!("Failed to decode AppliedStateCore: {e}"),
                    })?;
                let core_bytes = encode(&core).map_err(|e| SnapshotError::InvalidEntry {
                    reason: format!("Failed to re-encode AppliedStateCore: {e}"),
                })?;
                (core, core_bytes)
            },
            Some(data) => {
                // Old format: full AppliedState blob (pre-migration)
                let state: AppliedState =
                    decode(&data).map_err(|e| SnapshotError::InvalidEntry {
                        reason: format!("Failed to decode old-format AppliedState: {e}"),
                    })?;
                let core = AppliedStateCore::from(&state);
                let core_bytes = encode(&core).map_err(|e| SnapshotError::InvalidEntry {
                    reason: format!("Failed to encode AppliedStateCore: {e}"),
                })?;
                (core, core_bytes)
            },
            None => {
                // Fresh database — empty state
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
                (core, core_bytes)
            },
        };

        let snapshot_id = format!(
            "snapshot-{}-{}",
            core.last_applied.as_ref().map_or(0, |l| l.index),
            chrono::Utc::now().timestamp()
        );

        // 2. Collect all table data from the same ReadTransaction
        let mut tables = Vec::with_capacity(SNAPSHOT_TABLE_IDS.len());
        for &table_id_u8 in SNAPSHOT_TABLE_IDS {
            let table_id = TableId::from_u8(table_id_u8)
                .ok_or(SnapshotError::UnknownTableId { table_id: table_id_u8 })?;
            let entries = iter_table_raw(&read_txn, table_id)?;
            tables.push((table_id_u8, entries));
        }

        // Collect entities
        let entities = iter_table_raw(&read_txn, TableId::Entities)?;

        // ReadTransaction is dropped here — no longer held across awaits
        (core_bytes, core.last_applied, core.membership.clone(), snapshot_id, tables, entities)
    };

    // --- Async phase: write all collected data to the snapshot file ---

    let std_file = tempfile::tempfile()
        .map_err(|e| SnapshotError::Io { source: e, location: snafu::location!() })?;
    let file = tokio::fs::File::from_std(std_file);
    let mut writer = SnapshotWriter::new(file);

    // Header
    writer.write_header(&core_bytes).await?;

    // Table sections
    writer.write_table_count(table_data.len() as u32).await?;
    for (table_id_u8, entries) in &table_data {
        writer.write_table_header(*table_id_u8, entries.len() as u32).await?;
        for (key, value) in entries {
            writer.write_table_entry(key, value).await?;
        }
    }

    // Entity section
    writer.write_entity_count(entity_entries.len() as u64).await?;
    for (key, value) in &entity_entries {
        writer.write_entity_entry(key, value).await?;
    }

    // Event section
    writer.write_event_count(event_entries.len() as u64).await?;
    for entry_bytes in &event_entries {
        writer.write_event_entry(entry_bytes).await?;
    }

    // Finalize: flush zstd stream and write checksum footer
    let mut file = writer.finish().await?;

    // Seek to start for reading
    use tokio::io::AsyncSeekExt;
    file.seek(std::io::SeekFrom::Start(0))
        .await
        .map_err(|e| SnapshotError::Io { source: e, location: snafu::location!() })?;

    let meta = SnapshotMeta { last_log_id: last_applied, last_membership: membership, snapshot_id };

    tracing::info!(
        last_applied = ?last_applied,
        table_count = table_data.len(),
        entity_count = entity_entries.len(),
        event_count = event_entries.len(),
        "Snapshot file created"
    );

    Ok((file, meta))
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

/// Extracts organization IDs and the last applied deterministic timestamp from
/// the database. Used by snapshot creation to parameterize event collection.
fn extract_snapshot_event_context(
    db: &inferadb_ledger_store::Database<FileBackend>,
) -> Result<(Vec<inferadb_ledger_types::OrganizationId>, i64), StoreError> {
    let read_txn = db.read().map_err(|e| to_storage_error(&e))?;

    // Extract org IDs from OrganizationMeta table
    let org_iter = read_txn.iter::<tables::OrganizationMeta>().map_err(|e| to_storage_error(&e))?;
    let org_ids: Vec<_> = org_iter
        .filter_map(|(key, _)| {
            <i64 as inferadb_ledger_store::Key>::decode(&key)
                .map(inferadb_ledger_types::OrganizationId::new)
        })
        .collect();

    // Extract last_applied_timestamp_ns from AppliedStateCore
    let timestamp_ns = read_txn
        .get::<tables::RaftState>(&super::KEY_APPLIED_STATE.to_string())
        .map_err(|e| to_storage_error(&e))?
        .and_then(|data| {
            if data.len() >= 2 && data[0..2] == STATE_CORE_VERSION_SENTINEL {
                decode::<AppliedStateCore>(&data[2..]).ok()
            } else {
                None
            }
        })
        .map_or(0, |core| core.last_applied_timestamp_ns);

    Ok((org_ids, timestamp_ns))
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

impl RaftLogStore {
    /// Applies a batch of committed entries from the consensus engine.
    ///
    /// This is the core state machine apply path. For each entry:
    /// - Normal entries: deserialize as `RaftPayload`, apply via `apply_request_with_events`
    /// - Membership entries: update `state.membership`
    ///
    /// Returns a response for each entry in the batch.
    pub async fn apply_committed_entries(
        &mut self,
        entries: &[CommittedEntry],
        leader_node: Option<u64>,
    ) -> Result<Vec<crate::types::LedgerResponse>, StoreError> {
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

        // Pre-decode all Normal entry payloads once. Membership entries get None.
        // This avoids redundant deserialization — the same payload was previously
        // decoded up to 3 times (timestamp extraction, commitment verification,
        // and the main apply loop).
        let decoded_payloads: Vec<Option<crate::types::RaftPayload>> = entries
            .iter()
            .map(|e| match &e.kind {
                // Empty data = Raft no-op entry (§5.4.2) or barrier — skip decode.
                EntryKind::Normal if e.data.is_empty() => None,
                EntryKind::Normal => inferadb_ledger_types::decode(&e.data).ok(),
                EntryKind::Membership(_) => None,
            })
            .collect();

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
        // already committed before a crash.
        let state_layer_sentinel: Option<LogId> = self
            .state_layer
            .as_ref()
            .and_then(|sl| match sl.read_last_applied() {
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
                    self.apply_request_with_events(
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

            if let Some(archive) = &self.block_archive
                && let Err(e) = archive.append_block(&region_block)
            {
                tracing::error!("Failed to store block: {}", e);
            }

            // Broadcast block announcements for real-time subscribers
            if let Some(sender) = &self.block_announcements {
                for entry in &vault_entries {
                    let block_hash = inferadb_ledger_types::vault_entry_hash(entry);
                    let announcement = inferadb_ledger_proto::proto::BlockAnnouncement {
                        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                            slug: state
                                .id_to_slug
                                .get(&entry.organization)
                                .map_or(entry.organization.value() as u64, |s| s.value()),
                        }),
                        vault: Some(inferadb_ledger_proto::proto::VaultSlug {
                            slug: state
                                .vault_id_to_slug
                                .get(&entry.vault)
                                .map_or(entry.vault.value() as u64, |s| s.value()),
                        }),
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

    /// Returns a snapshot builder.
    pub fn get_snapshot_builder(&self) -> LedgerSnapshotBuilder {
        LedgerSnapshotBuilder {
            db: Arc::clone(&self.db),
            state_layer: self.state_layer.as_ref().map(Arc::clone),
            events_db: self.event_writer.as_ref().map(|ew| Arc::clone(ew.events_db())),
            event_config: self.event_writer.as_ref().map(|ew| ew.config().clone()),
        }
    }

    /// Installs a snapshot received from the leader.
    ///
    /// Streams decompressed data directly into B-trees and restores the
    /// in-memory state from the committed tables.
    pub async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Box<tokio::fs::File>,
    ) -> Result<(), StoreError> {
        use tokio::io::AsyncSeekExt;

        let mut file = *snapshot;

        // Async phase: verify SHA-256 checksum over compressed bytes
        let file_size = file.seek(std::io::SeekFrom::End(0)).await.map_err(StoreError::from_io)?;
        file.seek(std::io::SeekFrom::Start(0)).await.map_err(StoreError::from_io)?;

        SnapshotReader::verify_checksum(&mut file, file_size)
            .await
            .map_err(|e| StoreError::msg(e.to_string()))?;

        // Convert to std::fs::File for synchronous streaming
        let std_file = file.into_std().await;
        let compressed_size = file_size - crate::snapshot::CHECKSUM_SIZE as u64;

        // Sync phase: stream decompressed data directly into B-trees
        let db = &self.db;
        let event_writer_ref = self.event_writer.as_ref();

        let (core, table_count, entity_count, event_count) =
            tokio::task::block_in_place(|| -> Result<_, StoreError> {
                use std::io::{BufReader, Read, Seek, SeekFrom};

                let mut file = std_file;
                file.seek(SeekFrom::Start(0)).map_err(StoreError::from_io)?;

                let reader = BufReader::new(file.take(compressed_size));
                let mut decoder =
                    zstd::stream::read::Decoder::new(reader).map_err(StoreError::from_io)?;

                // Read header → AppliedStateCore
                let core_bytes = SyncSnapshotReader::read_header(&mut decoder)
                    .map_err(|e| StoreError::msg(e.to_string()))?;
                let core: AppliedStateCore =
                    decode(&core_bytes).map_err(|e| StoreError::from_error(&e))?;

                // Open WriteTransaction
                let mut write_txn = db.write().map_err(|e| to_storage_error(&e))?;

                // Stream table entries
                let table_count = SyncSnapshotReader::read_u32(&mut decoder)
                    .map_err(|e| StoreError::msg(e.to_string()))?;

                for _ in 0..table_count {
                    let (table_id_u8, entry_count) =
                        SyncSnapshotReader::read_table_header(&mut decoder)
                            .map_err(|e| StoreError::msg(e.to_string()))?;

                    let table_id = TableId::from_u8(table_id_u8).ok_or_else(|| {
                        StoreError::msg(format!("unknown table ID: {table_id_u8}"))
                    })?;

                    for _ in 0..entry_count {
                        let (key, value) = SyncSnapshotReader::read_kv_entry(&mut decoder)
                            .map_err(|e| StoreError::msg(e.to_string()))?;
                        write_txn
                            .insert_raw(table_id, &key, &value)
                            .map_err(|e| to_storage_error(&e))?;
                    }
                }

                // Stream entity entries
                let entity_count = SyncSnapshotReader::read_u64(&mut decoder)
                    .map_err(|e| StoreError::msg(e.to_string()))?;

                for _ in 0..entity_count {
                    let (key, value) = SyncSnapshotReader::read_kv_entry(&mut decoder)
                        .map_err(|e| StoreError::msg(e.to_string()))?;
                    inferadb_ledger_state::StateLayer::restore_entity(&mut write_txn, &key, &value)
                        .map_err(|e| to_storage_error(&e))?;
                }

                // Persist the AppliedStateCore blob with version sentinel
                let core_data = encode(&core).map_err(|e| to_serde_error(&e))?;
                let mut state_data =
                    Vec::with_capacity(STATE_CORE_VERSION_SENTINEL.len() + core_data.len());
                state_data.extend_from_slice(&STATE_CORE_VERSION_SENTINEL);
                state_data.extend_from_slice(&core_data);
                write_txn
                    .insert::<tables::RaftState>(&super::KEY_APPLIED_STATE.to_string(), &state_data)
                    .map_err(|e| to_storage_error(&e))?;

                // Atomic commit
                write_txn.commit().map_err(|e| to_storage_error(&e))?;

                // Best-effort event restoration
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

                Ok((core, table_count, entity_count, event_count))
            })?;

        // Restore in-memory state from the committed tables
        let loaded_state = self.load_applied_state_from_db(&core)?;
        self.applied_state.store(Arc::new(loaded_state));

        // Restore region chain tracking
        *self.region_chain.write() = RegionChainState {
            height: core.region_height,
            previous_hash: core.previous_region_hash,
        };

        tracing::info!(
            snapshot_id = %meta.snapshot_id,
            last_log_id = ?meta.last_log_id,
            table_count,
            entity_count,
            event_count,
            "Snapshot installed (streaming)"
        );

        Ok(())
    }

    /// Gets the current snapshot, if any state has been applied.
    pub async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<(tokio::fs::File, SnapshotMeta)>, StoreError> {
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

        let (org_ids, last_ts) = extract_snapshot_event_context(&self.db)?;

        let event_entries = {
            let events_db = self.event_writer.as_ref().map(|ew| Arc::clone(ew.events_db()));
            let event_config = self.event_writer.as_ref().map(|ew| ew.config().clone());
            collect_snapshot_events(events_db.as_deref(), event_config.as_ref(), &org_ids, last_ts)
        };

        let (file, meta) = write_snapshot_to_file(&self.db, event_entries)
            .await
            .map_err(|e| StoreError::msg(e.to_string()))?;

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
                        state.vault_id_to_slug.insert(vault_id, meta.slug);
                        state.vault_slug_index.insert(meta.slug, vault_id);
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
            if let (Some(slug_val), Some(vault_id_val)) = (
                <u64 as inferadb_ledger_store::Key>::decode(&key_bytes),
                decode::<i64>(&value_bytes).ok(),
            ) {
                let slug = inferadb_ledger_types::VaultSlug::new(slug_val);
                let vault_id = inferadb_ledger_types::VaultId::new(vault_id_val);
                state.vault_slug_index.insert(slug, vault_id);
                state.vault_id_to_slug.insert(vault_id, slug);
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
