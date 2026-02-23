//! OpenRaft trait implementations for [`RaftLogStore`].
//!
//! Implements [`RaftLogReader`], [`RaftStorage`], and [`RaftSnapshotBuilder`]
//! using the B-tree-backed storage engine.

use std::{collections::HashMap, fmt::Debug, io::Cursor, ops::RangeBounds, sync::Arc};

use inferadb_ledger_store::tables;
use inferadb_ledger_types::{decode, encode};
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftStorage, SnapshotMeta, StorageError,
    StoredMembership, Vote,
    storage::{LogState, RaftLogReader, RaftSnapshotBuilder, Snapshot},
};
use parking_lot::RwLock;

use super::{
    ShardChainState,
    store::RaftLogStore,
    types::{AppliedState, CombinedSnapshot},
};
use crate::{
    metrics,
    types::{LedgerNodeId, LedgerTypeConfig},
};

// ============================================================================
// RaftLogReader Implementation
// ============================================================================

impl RaftLogReader<LedgerTypeConfig> for RaftLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<LedgerTypeConfig>>, StorageError<LedgerNodeId>> {
        use std::ops::Bound;

        let read_txn = self.db.read().map_err(|e| to_storage_error(&e))?;

        // Convert RangeBounds to start/end indices
        let start_idx = match range.start_bound() {
            Bound::Included(&idx) => idx,
            Bound::Excluded(&idx) => idx.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let end_idx = match range.end_bound() {
            Bound::Included(&idx) => Some(idx.saturating_add(1)),
            Bound::Excluded(&idx) => Some(idx),
            Bound::Unbounded => None,
        };

        // Use range iteration from inferadb-ledger-store
        let start_key = start_idx;
        let end_key = end_idx.unwrap_or(u64::MAX);

        let iter = read_txn
            .range::<tables::RaftLog>(Some(&start_key), Some(&end_key))
            .map_err(|e| to_storage_error(&e))?;

        let mut entries = Vec::new();
        let mut total_bytes = 0usize;
        let deserialize_start = std::time::Instant::now();

        for (_, entry_data) in iter {
            total_bytes += entry_data.len();
            let entry: Entry<LedgerTypeConfig> =
                decode(&entry_data).map_err(|e| to_serde_error(&e))?;
            entries.push(entry);
        }

        // Record batch deserialization metrics
        if !entries.is_empty() {
            let deserialize_secs = deserialize_start.elapsed().as_secs_f64();
            // Record per-entry average to make metrics comparable to encode path
            let per_entry_secs = deserialize_secs / entries.len() as f64;
            metrics::record_postcard_decode(per_entry_secs, "raft_entry");
            metrics::record_serialization_bytes(
                total_bytes / entries.len(),
                "decode",
                "raft_entry",
            );
        }

        Ok(entries)
    }

    /// Returns log entries for replication.
    ///
    /// OpenRaft contract: this must not return empty for non-empty range.
    /// If entries are purged, return error to trigger snapshot replication.
    /// If entries are not yet written (race condition), wait for them.
    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<Entry<LedgerTypeConfig>>, StorageError<LedgerNodeId>> {
        // Quick path: if range is empty, return empty
        if start >= end {
            return Ok(Vec::new());
        }

        // Retry loop for transient conditions (entries not yet visible)
        // OpenRaft may try to replicate entries before they're fully appended,
        // especially under high concurrent load. We retry with exponential backoff.
        let mut attempts = 0;
        const MAX_ATTEMPTS: u32 = 500; // 5 seconds max wait
        const RETRY_DELAY_MS: u64 = 10;

        loop {
            let entries = self.try_get_log_entries(start..end).await?;

            // Success: got entries
            if !entries.is_empty() {
                return Ok(entries);
            }

            // Empty result - check if purged
            let last_purged = *self.last_purged_cache.read();
            let purged_idx = last_purged.map_or(0, |l| l.index);

            // If entries were purged, return error (need snapshot replication)
            if start <= purged_idx {
                return Err(StorageError::IO {
                    source: openraft::StorageIOError::read_logs(openraft::AnyError::error(
                        format!(
                            "Log entries [{}, {}) purged (last_purged={}), need snapshot",
                            start, end, purged_idx
                        ),
                    )),
                });
            }

            // Not purged - might be a race condition, retry
            attempts += 1;
            if attempts >= MAX_ATTEMPTS {
                // Give up and return error - something is seriously wrong
                let last_idx = self.get_last_entry().ok().flatten().map_or(0, |e| e.log_id.index);
                return Err(StorageError::IO {
                    source: openraft::StorageIOError::read_logs(openraft::AnyError::error(
                        format!(
                            "Log entries [{}, {}) not found after {}ms (last={}, purged={})",
                            start,
                            end,
                            attempts * RETRY_DELAY_MS as u32,
                            last_idx,
                            purged_idx
                        ),
                    )),
                });
            }

            // Wait briefly for entries to appear
            tokio::time::sleep(tokio::time::Duration::from_millis(RETRY_DELAY_MS)).await;
        }
    }
}

// ============================================================================
// RaftSnapshotBuilder Implementation
// ============================================================================

/// Builds Raft snapshots by serializing the current applied state and entity data.
pub struct LedgerSnapshotBuilder {
    /// State to snapshot.
    state: AppliedState,
}

impl RaftSnapshotBuilder<LedgerTypeConfig> for LedgerSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<LedgerTypeConfig>, StorageError<LedgerNodeId>> {
        let data = encode(&self.state).map_err(|e| to_serde_error(&e))?;

        let snapshot_id = format!(
            "snapshot-{}-{}",
            self.state.last_applied.as_ref().map_or(0, |l| l.index),
            chrono::Utc::now().timestamp()
        );

        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: self.state.last_applied,
                last_membership: self.state.membership.clone(),
                snapshot_id,
            },
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

// ============================================================================
// RaftStorage Implementation (deprecated but non-sealed)
// ============================================================================

#[allow(deprecated)]
impl RaftStorage<LedgerTypeConfig> for RaftLogStore {
    type LogReader = Self;
    type SnapshotBuilder = LedgerSnapshotBuilder;

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<LedgerTypeConfig>, StorageError<LedgerNodeId>> {
        let last_purged = *self.last_purged_cache.read();
        let last_entry = self.get_last_entry()?;
        let last_log_id = last_entry.map(|e| e.log_id);

        Ok(LogState { last_purged_log_id: last_purged, last_log_id })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        Self {
            db: Arc::clone(&self.db),
            vote_cache: RwLock::new(*self.vote_cache.read()),
            last_purged_cache: RwLock::new(*self.last_purged_cache.read()),
            applied_state: Arc::clone(&self.applied_state),
            state_layer: self.state_layer.clone(),
            block_archive: self.block_archive.clone(),
            shard_id: self.shard_id,
            node_id: self.node_id.clone(),
            shard_chain: RwLock::new(*self.shard_chain.read()),
            block_announcements: self.block_announcements.clone(),
            event_writer: None,
        }
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<LedgerNodeId>,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let vote_data = encode(vote).map_err(|e| to_serde_error(&e))?;

        let mut write_txn = self.db.write().map_err(|e| to_storage_error(&e))?;
        write_txn
            .insert::<tables::RaftState>(&super::KEY_VOTE.to_string(), &vote_data)
            .map_err(|e| to_storage_error(&e))?;
        write_txn.commit().map_err(|e| to_storage_error(&e))?;

        *self.vote_cache.write() = Some(*vote);
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<LedgerNodeId>>, StorageError<LedgerNodeId>> {
        Ok(*self.vote_cache.read())
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<LedgerNodeId>>
    where
        I: IntoIterator<Item = Entry<LedgerTypeConfig>> + OptionalSend,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        if entries.is_empty() {
            tracing::debug!("append_to_log called with empty entries");
            return Ok(());
        }

        let indices: Vec<u64> = entries.iter().map(|e| e.log_id.index).collect();
        tracing::info!("append_to_log: appending entries {:?}", indices);

        let mut write_txn = self.db.write().map_err(|e| to_storage_error(&e))?;

        for entry in entries {
            let index = entry.log_id.index;

            // Time postcard serialization (write path hot loop)
            let serialize_start = std::time::Instant::now();
            let entry_data = encode(&entry).map_err(|e| to_serde_error(&e))?;
            let serialize_secs = serialize_start.elapsed().as_secs_f64();

            // Record serialization metrics
            metrics::record_postcard_encode(serialize_secs, "raft_entry");
            metrics::record_serialization_bytes(entry_data.len(), "encode", "raft_entry");

            write_txn
                .insert::<tables::RaftLog>(&index, &entry_data)
                .map_err(|e| to_storage_error(&e))?;
        }

        write_txn.commit().map_err(|e| {
            tracing::error!("append_to_log: commit failed for entries {:?}: {:?}", indices, e);
            to_storage_error(&e)
        })?;

        tracing::info!("append_to_log: committed entries {:?}", indices);
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<LedgerNodeId>,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        // First, collect keys to remove using a read transaction
        let keys_to_remove: Vec<u64> = {
            let read_txn = self.db.read().map_err(|e| to_storage_error(&e))?;
            let mut keys = Vec::new();
            let iter = read_txn
                .range::<tables::RaftLog>(Some(&log_id.index), None)
                .map_err(|e| to_storage_error(&e))?;
            for (key_bytes, _) in iter {
                // Key is u64 encoded as big-endian
                if let Ok(arr) = <[u8; 8]>::try_from(key_bytes.as_ref()) {
                    keys.push(u64::from_be_bytes(arr));
                }
            }
            keys
        };

        // Then delete in a write transaction
        let mut write_txn = self.db.write().map_err(|e| to_storage_error(&e))?;
        for key in keys_to_remove {
            write_txn.delete::<tables::RaftLog>(&key).map_err(|e| to_storage_error(&e))?;
        }
        write_txn.commit().map_err(|e| to_storage_error(&e))?;

        Ok(())
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<LedgerNodeId>,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        // First, collect keys to remove using a read transaction
        let keys_to_remove: Vec<u64> = {
            let read_txn = self.db.read().map_err(|e| to_storage_error(&e))?;
            let mut keys = Vec::new();
            // Inclusive end: range from 0 to log_id.index + 1 (exclusive)
            let end_key = log_id.index.saturating_add(1);
            let iter = read_txn
                .range::<tables::RaftLog>(Some(&0u64), Some(&end_key))
                .map_err(|e| to_storage_error(&e))?;
            for (key_bytes, _) in iter {
                // Key is u64 encoded as big-endian
                if let Ok(arr) = <[u8; 8]>::try_from(key_bytes.as_ref()) {
                    keys.push(u64::from_be_bytes(arr));
                }
            }
            keys
        };

        // Then delete in a write transaction
        let mut write_txn = self.db.write().map_err(|e| to_storage_error(&e))?;
        for key in keys_to_remove {
            write_txn.delete::<tables::RaftLog>(&key).map_err(|e| to_storage_error(&e))?;
        }

        // Save the last purged log ID
        let purged_data = encode(&log_id).map_err(|e| to_serde_error(&e))?;
        write_txn
            .insert::<tables::RaftState>(&super::KEY_LAST_PURGED.to_string(), &purged_data)
            .map_err(|e| to_storage_error(&e))?;

        write_txn.commit().map_err(|e| to_storage_error(&e))?;

        *self.last_purged_cache.write() = Some(log_id);
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (Option<LogId<LedgerNodeId>>, StoredMembership<LedgerNodeId, openraft::BasicNode>),
        StorageError<LedgerNodeId>,
    > {
        let state = self.applied_state.read();
        Ok((state.last_applied, state.membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<LedgerTypeConfig>],
    ) -> Result<Vec<crate::types::LedgerResponse>, StorageError<LedgerNodeId>> {
        let _span = tracing::info_span!(
            "apply_to_state_machine",
            entry_count = entries.len(),
            shard_id = self.shard_id.value(),
        )
        .entered();

        let mut responses = Vec::new();
        let mut vault_entries = Vec::new();
        let mut state = self.applied_state.write();

        // Event accumulation — deterministic timestamp from leader's proposal.
        // All replicas apply with the same timestamp, guaranteeing byte-identical
        // event storage, B+ tree keys, and pagination cursors across the cluster.
        let block_timestamp = entries
            .last()
            .and_then(|e| match &e.payload {
                EntryPayload::Normal(payload) => Some(payload.proposed_at),
                _ => None,
            })
            .unwrap_or_else(chrono::Utc::now);
        let mut events = Vec::new();
        let mut op_index = 0u32;
        let ttl_days = self.event_writer.as_ref().map_or(0, |ew| ew.config().default_ttl_days);

        // Get the committed_index from the last entry (for ShardBlock metadata)
        let committed_index = entries.last().map_or(0, |e| e.log_id.index);
        // Term is stored in the leader_id
        let term = entries.last().map(|e| e.log_id.leader_id.get_term()).unwrap_or(0);

        for entry in entries {
            state.last_applied = Some(entry.log_id);

            let (response, vault_entry) = match &entry.payload {
                EntryPayload::Blank => (crate::types::LedgerResponse::Empty, None),
                EntryPayload::Normal(payload) => self.apply_request_with_events(
                    &payload.request,
                    &mut state,
                    block_timestamp,
                    &mut op_index,
                    &mut events,
                    ttl_days,
                ),
                EntryPayload::Membership(membership) => {
                    state.membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    (crate::types::LedgerResponse::Empty, None)
                },
            };

            responses.push(response);

            // Collect vault entries for ShardBlock creation
            if let Some(entry) = vault_entry {
                vault_entries.push(entry);
            }
        }

        // Create and store ShardBlock if we have vault entries
        if !vault_entries.is_empty() {
            let timestamp = block_timestamp;

            // Read shard chain state (single lock acquisition)
            let chain_state = *self.shard_chain.read();
            let new_shard_height = chain_state.height + 1;

            let shard_block = inferadb_ledger_types::ShardBlock {
                shard_id: self.shard_id,
                shard_height: new_shard_height,
                previous_shard_hash: chain_state.previous_hash,
                vault_entries: vault_entries.clone(),
                timestamp,
                leader_id: self.node_id.clone(),
                term,
                committed_index,
            };

            // Store in block archive if configured
            if let Some(archive) = &self.block_archive
                && let Err(e) = archive.append_block(&shard_block)
            {
                tracing::error!("Failed to store block: {}", e);
                // Continue - block storage failure is logged but doesn't fail the operation
            }

            // Broadcast block announcements for real-time subscribers
            if let Some(sender) = &self.block_announcements {
                for entry in &vault_entries {
                    let block_hash = inferadb_ledger_types::vault_entry_hash(entry);
                    let announcement = inferadb_ledger_proto::proto::BlockAnnouncement {
                        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                            slug: state
                                .id_to_slug
                                .get(&entry.organization_id)
                                .map_or(entry.organization_id.value() as u64, |s| s.value()),
                        }),
                        vault: Some(inferadb_ledger_proto::proto::VaultSlug {
                            slug: state
                                .vault_id_to_slug
                                .get(&entry.vault_id)
                                .map_or(0, |s| s.value()),
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
                    // Ignore send errors - no receivers is valid (fire-and-forget)
                    let _ = sender.send(announcement);
                    tracing::debug!(
                        organization_id = entry.organization_id.value(),
                        vault_id = entry.vault_id.value(),
                        height = entry.vault_height,
                        "Block announcement broadcast"
                    );
                }
            }

            // Update previous vault hashes for each entry
            for entry in &vault_entries {
                let vault_block =
                    shard_block.extract_vault_block(entry.organization_id, entry.vault_id);
                if let Some(vb) = vault_block {
                    let block_hash = inferadb_ledger_types::hash::block_hash(&vb.header);
                    state
                        .previous_vault_hashes
                        .insert((entry.organization_id, entry.vault_id), block_hash);
                }
            }

            // Update shard chain tracking (single lock acquisition)
            let shard_hash =
                inferadb_ledger_types::sha256(&encode(&shard_block).unwrap_or_default());
            *self.shard_chain.write() =
                ShardChainState { height: new_shard_height, previous_hash: shard_hash };

            // Also update AppliedState for snapshot persistence
            state.shard_height = new_shard_height;
            state.previous_shard_hash = shard_hash;
        }

        // Persist state
        drop(state);
        let state_ref = self.applied_state.read().clone();
        self.save_applied_state(&state_ref)?;

        // Write accumulated events to events.db (best-effort: log failures but
        // don't fail the Raft apply — events are audit data, not consensus-critical).
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
                Ok(_) => {}, // All filtered by config — no-op
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

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        LedgerSnapshotBuilder { state: self.applied_state.read().clone() }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<LedgerNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<LedgerNodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let data = snapshot.into_inner();

        // Try to deserialize as CombinedSnapshot first (new format)
        let combined: CombinedSnapshot = decode(&data).map_err(|e| to_serde_error(&e))?;

        // Restore AppliedState
        *self.applied_state.write() = combined.applied_state.clone();
        self.save_applied_state(&combined.applied_state)?;

        // Restore shard chain tracking from snapshot (single lock)
        *self.shard_chain.write() = ShardChainState {
            height: combined.applied_state.shard_height,
            previous_hash: combined.applied_state.previous_shard_hash,
        };

        // Restore StateLayer entities if StateLayer is configured
        if let Some(state_layer) = &self.state_layer {
            for (vault_id, entities) in &combined.vault_entities {
                for entity in entities {
                    // Convert entity to SetEntity operation
                    let ops = vec![inferadb_ledger_types::Operation::SetEntity {
                        key: String::from_utf8_lossy(&entity.key).to_string(),
                        value: entity.value.clone(),
                        condition: None, // No condition for snapshot restore
                        expires_at: if entity.expires_at == 0 {
                            None
                        } else {
                            Some(entity.expires_at)
                        },
                    }];
                    // Apply at the entity's version height
                    if let Err(e) = state_layer.apply_operations(*vault_id, &ops, entity.version) {
                        tracing::warn!(
                            vault_id = vault_id.value(),
                            key = %String::from_utf8_lossy(&entity.key),
                            error = %e,
                            "Failed to restore entity from snapshot"
                        );
                    }
                }
            }
            tracing::info!(
                vault_count = combined.vault_entities.len(),
                "Restored StateLayer from snapshot"
            );
        }

        // Restore apply-phase events from snapshot (if event_writer is configured)
        if !combined.event_entries.is_empty()
            && let Some(ew) = &self.event_writer
        {
            match ew.events_db().write() {
                Ok(mut txn) => {
                    let mut written = 0usize;
                    for entry in &combined.event_entries {
                        if let Err(e) = inferadb_ledger_state::EventStore::write(&mut txn, entry) {
                            tracing::warn!(
                                event_id = %uuid::Uuid::from_bytes(entry.event_id),
                                error = %e,
                                "Failed to restore event from snapshot"
                            );
                        } else {
                            written += 1;
                        }
                    }
                    if let Err(e) = txn.commit() {
                        tracing::warn!(
                            error = %e,
                            "Failed to commit restored events from snapshot"
                        );
                    } else {
                        tracing::info!(
                            count = written,
                            total = combined.event_entries.len(),
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

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<LedgerTypeConfig>>, StorageError<LedgerNodeId>> {
        let state = self.applied_state.read();

        if state.last_applied.is_none() {
            return Ok(None);
        }

        // Collect entities from StateLayer if configured
        // StateLayer is internally thread-safe via inferadb-ledger-store's MVCC, so no lock needed
        let vault_entities = if let Some(state_layer) = &self.state_layer {
            let mut entities_map = HashMap::new();

            // Get entities for each known vault
            for &(organization_id, vault_id) in state.vault_heights.keys() {
                // List all entities in this vault (up to 10000 per vault for snapshot)
                match state_layer.list_entities(vault_id, None, None, 10000) {
                    Ok(entities) => {
                        if !entities.is_empty() {
                            entities_map.insert(vault_id, entities);
                            tracing::debug!(
                                organization_id = organization_id.value(),
                                vault_id = vault_id.value(),
                                count = entities_map.get(&vault_id).map(|e| e.len()).unwrap_or(0),
                                "Collected entities for snapshot"
                            );
                        }
                    },
                    Err(e) => {
                        tracing::warn!(
                            organization_id = organization_id.value(),
                            vault_id = vault_id.value(),
                            error = %e,
                            "Failed to list entities for snapshot"
                        );
                    },
                }
            }

            entities_map
        } else {
            HashMap::new()
        };

        // Collect apply-phase events from events.db for snapshot (if available)
        let event_entries = if let Some(ew) = &self.event_writer {
            let max_events = ew.config().max_snapshot_events;
            match ew.events_db().read() {
                Ok(txn) => {
                    match inferadb_ledger_state::EventStore::scan_apply_phase(&txn, max_events) {
                        Ok(entries) => {
                            if !entries.is_empty() {
                                tracing::debug!(
                                    count = entries.len(),
                                    max = max_events,
                                    "Collected apply-phase events for snapshot"
                                );
                            }
                            entries
                        },
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "Failed to scan apply-phase events for snapshot"
                            );
                            Vec::new()
                        },
                    }
                },
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Failed to open read transaction for event snapshot"
                    );
                    Vec::new()
                },
            }
        } else {
            Vec::new()
        };

        // Create combined snapshot
        let combined =
            CombinedSnapshot { applied_state: state.clone(), vault_entities, event_entries };

        let data = encode(&combined).map_err(|e| to_serde_error(&e))?;

        let snapshot_id = format!(
            "snapshot-{}-{}",
            state.last_applied.as_ref().map_or(0, |l| l.index),
            chrono::Utc::now().timestamp()
        );

        Ok(Some(Snapshot {
            meta: SnapshotMeta {
                last_log_id: state.last_applied,
                last_membership: state.membership.clone(),
                snapshot_id,
            },
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}

// ============================================================================
// Error Helpers
// ============================================================================

pub(super) fn to_storage_error<E: std::error::Error>(e: &E) -> StorageError<LedgerNodeId> {
    StorageError::from_io_error(
        openraft::ErrorSubject::Store,
        openraft::ErrorVerb::Write,
        std::io::Error::other(e.to_string()),
    )
}

pub(super) fn to_serde_error<E: std::error::Error>(e: &E) -> StorageError<LedgerNodeId> {
    StorageError::from_io_error(
        openraft::ErrorSubject::Store,
        openraft::ErrorVerb::Read,
        std::io::Error::other(e.to_string()),
    )
}
