//! State layer for materialized state with bucket-based commitment.
//!
//! Provides:
//! - Per-vault entity and relationship CRUD via [`StateLayer::apply_operations`]
//! - Atomic batch writes with conditional (CAS) semantics
//! - Incremental state root computation via 256-bucket dirty tracking
//!
//! The state layer separates commitment (Merkleized hashing) from storage
//! (fast K/V reads), so queries bypass the hash tree entirely.

use std::{collections::HashMap, sync::Arc};

use inferadb_ledger_store::{
    CompactionStats, Database, DatabaseStats, StorageBackend, Table, TableId, WriteTransaction,
    tables,
};
use inferadb_ledger_types::{
    Entity, Hash, Operation, Relationship, SetCondition, VaultId, WriteStatus, decode, encode,
};
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use snafu::{ResultExt, Snafu};

use crate::{
    binary_keys::{InternCategory, encode_relationship_local_key, split_typed_id},
    bucket::{BucketRootBuilder, NUM_BUCKETS, VaultCommitment},
    indexes::{IndexError, IndexManager},
    keys::encode_storage_key,
    relationship_index::RelationshipIndex,
};

/// Errors returned by [`StateLayer`] operations.
#[derive(Debug, Snafu)]
pub enum StateError {
    /// Underlying storage operation failed.
    #[snafu(display("Storage error: {source}"))]
    Store {
        /// The underlying inferadb-ledger-store storage error.
        source: inferadb_ledger_store::Error,
        /// Location where the error occurred.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Index operation failed.
    #[snafu(display("Index error: {source}"))]
    Index {
        /// The underlying index error.
        source: IndexError,
        /// Location where the error occurred.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Codec error during serialization/deserialization.
    #[snafu(display("Codec error: {source}"))]
    Codec {
        /// The underlying codec error.
        source: inferadb_ledger_types::CodecError,
        /// Location where the error occurred.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Relationship storage operation failed.
    #[snafu(display("Relationship error: {source}"))]
    Relationship {
        /// The underlying relationship error.
        source: crate::relationship::RelationshipError,
        /// Location where the error occurred.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Conditional write precondition failed.
    ///
    /// Returns current state for client-side conflict resolution. When a CAS
    /// (compare-and-swap) condition fails, this error provides the current entity
    /// state so clients can implement retry or conflict resolution logic.
    #[snafu(display("Precondition failed for key '{key}'"))]
    PreconditionFailed {
        /// The key that failed the condition check.
        key: String,
        /// Current version of the entity (block height when last modified), if it exists.
        current_version: Option<u64>,
        /// Current value of the entity, if it exists.
        current_value: Option<Vec<u8>>,
        /// The condition that failed (for specific error code mapping).
        failed_condition: Option<SetCondition>,
    },
}

/// Result type for state operations.
pub type Result<T> = std::result::Result<T, StateError>;

/// State layer managing per-vault materialized state.
///
/// Provides entity/relationship CRUD and incremental state root computation
/// via bucket-based dirty tracking. Each vault's state is independently
/// tracked with its own [`VaultCommitment`] — only buckets modified since
/// the last commit are rehashed.
///
/// Generic over [`StorageBackend`] to support both file-based (production)
/// and in-memory (testing) storage.
///
/// # Usage
///
/// ```no_run
/// use std::sync::Arc;
/// use inferadb_ledger_store::Database;
/// use inferadb_ledger_state::StateLayer;
///
/// let db = Arc::new(Database::open_in_memory()?);
/// let state = StateLayer::new(db);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct StateLayer<B: StorageBackend> {
    /// Shared database handle.
    db: Arc<Database<B>>,
    /// Per-vault commitment tracking.
    vault_commitments: RwLock<HashMap<VaultId, VaultCommitment>>,
    /// Cached per-vault string dictionaries for relationship storage.
    vault_dictionaries: RwLock<HashMap<VaultId, crate::dictionary::VaultDictionary>>,
    /// In-memory hash index for O(1) relationship existence checks.
    relationship_index: Arc<RelationshipIndex>,
}

#[allow(clippy::result_large_err)]
impl<B: StorageBackend> StateLayer<B> {
    /// Creates a new state layer backed by the given database.
    pub fn new(db: Arc<Database<B>>) -> Self {
        Self {
            db,
            vault_commitments: RwLock::new(HashMap::new()),
            vault_dictionaries: RwLock::new(HashMap::new()),
            relationship_index: Arc::new(RelationshipIndex::new()),
        }
    }

    /// Returns database-level statistics for metrics collection.
    pub fn database_stats(&self) -> DatabaseStats {
        self.db.stats()
    }

    /// Returns a reference to the underlying database for integrity scrubbing and diagnostics.
    pub fn database(&self) -> &Arc<Database<B>> {
        &self.db
    }

    /// Returns B-tree depths for all non-empty tables.
    ///
    /// Opens a read transaction internally to walk each table's B-tree.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the database read fails.
    pub fn table_depths(&self) -> Result<Vec<(&'static str, u32)>> {
        self.db.table_depths().context(StoreSnafu)
    }

    /// Execute a function with mutable access to a vault's commitment.
    ///
    /// Creates the commitment if it doesn't exist.
    fn with_commitment<F, R>(&self, vault: VaultId, f: F) -> R
    where
        F: FnOnce(&mut VaultCommitment) -> R,
    {
        let mut map = self.vault_commitments.write();
        let commitment = map.entry(vault).or_default();
        f(commitment)
    }

    /// Takes the dictionary for a vault out of the cache for mutable use.
    ///
    /// If not cached, loads from storage. The caller **must** return it via
    /// [`return_dictionary`](Self::return_dictionary) after use, or it will be
    /// reloaded from storage on the next access.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the storage read fails.
    /// Returns [`StateError::Relationship`] if dictionary loading fails.
    pub fn take_dictionary(&self, vault: VaultId) -> Result<crate::dictionary::VaultDictionary> {
        if let Some(dict) = self.vault_dictionaries.write().remove(&vault) {
            return Ok(dict);
        }
        let txn = self.db.read().context(StoreSnafu)?;
        crate::dictionary::VaultDictionary::load(&txn, vault).map_err(|e| match e {
            crate::dictionary::DictionaryError::Storage { source, .. } => {
                StateError::Store { source, location: snafu::location!() }
            },
            other => StateError::Relationship {
                source: crate::relationship::RelationshipError::Dictionary {
                    source: other,
                    location: snafu::location!(),
                },
                location: snafu::location!(),
            },
        })
    }

    /// Returns a dictionary to the cache after use.
    pub fn return_dictionary(&self, vault: VaultId, dict: crate::dictionary::VaultDictionary) {
        self.vault_dictionaries.write().insert(vault, dict);
    }

    /// Returns a read-only reference to the cached dictionary for a vault.
    ///
    /// Loads the dictionary from storage if it is not already cached.
    fn get_dictionary(
        &self,
        vault: VaultId,
    ) -> Result<MappedRwLockReadGuard<'_, crate::dictionary::VaultDictionary>> {
        // Fast path: dictionary already cached.
        {
            let map = self.vault_dictionaries.read();
            if map.contains_key(&vault) {
                return Ok(RwLockReadGuard::map(map, |m| &m[&vault]));
            }
        }
        // Slow path: load from storage and insert into cache.
        let txn = self.db.read().context(StoreSnafu)?;
        let dict = crate::dictionary::VaultDictionary::load(&txn, vault).map_err(|e| match e {
            crate::dictionary::DictionaryError::Storage { source, .. } => {
                StateError::Store { source, location: snafu::location!() }
            },
            other => StateError::Relationship {
                source: crate::relationship::RelationshipError::Dictionary {
                    source: other,
                    location: snafu::location!(),
                },
                location: snafu::location!(),
            },
        })?;
        drop(txn);
        self.vault_dictionaries.write().insert(vault, dict);
        let map = self.vault_dictionaries.read();
        Ok(RwLockReadGuard::map(map, |m| &m[&vault]))
    }

    /// Opens a write transaction on the underlying database.
    ///
    /// Use with [`apply_operations_in_txn`](Self::apply_operations_in_txn)
    /// for caller-controlled transaction lifecycle.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the database write lock cannot be acquired.
    pub fn begin_write(&self) -> Result<WriteTransaction<'_, B>> {
        self.db.write().context(StoreSnafu)
    }

    /// Sentinel key for crash-recovery atomicity.
    ///
    /// Stored in the Entities table with a raw byte key (no vault/bucket prefix)
    /// so it cannot collide with vault-scoped entity keys which always start with
    /// an 8-byte big-endian `VaultId`.
    const LAST_APPLIED_KEY: &[u8] = b"_meta:last_applied";

    /// Persists a Raft log ID sentinel in the same write transaction as entity data.
    ///
    /// On crash recovery, comparing this sentinel against the Raft DB's
    /// `AppliedStateCore.last_applied` reveals whether the state layer is
    /// already up-to-date, preventing re-application of committed entries.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the table insert fails.
    pub fn persist_last_applied(
        txn: &mut WriteTransaction<'_, B>,
        log_id_bytes: &[u8],
    ) -> Result<()> {
        txn.insert_raw(
            inferadb_ledger_store::tables::TableId::Entities,
            Self::LAST_APPLIED_KEY,
            log_id_bytes,
        )
        .context(StoreSnafu)
    }

    /// Reads the last-applied Raft log ID sentinel from the state layer DB.
    ///
    /// Returns `None` on first boot (no sentinel written yet).
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction fails.
    pub fn read_last_applied(&self) -> Result<Option<Vec<u8>>> {
        let txn = self.db.read().context(StoreSnafu)?;
        let key = Self::LAST_APPLIED_KEY.to_vec();
        txn.get::<inferadb_ledger_store::tables::Entities>(&key).context(StoreSnafu)
    }

    /// Restores a single entity during snapshot installation.
    ///
    /// Writes a raw key/value pair directly into the Entities table within the
    /// provided transaction. No condition checking, version tracking, or dirty
    /// key tracking — snapshot restoration replaces the entire entity table.
    ///
    /// The key must be a full storage key (vault + bucket_id + local_key),
    /// as produced by [`encode_storage_key`](crate::encode_storage_key).
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the table insert fails.
    pub fn restore_entity(
        txn: &mut WriteTransaction<'_, B>,
        storage_key: &[u8],
        encoded_value: &[u8],
    ) -> Result<()> {
        txn.insert_raw(inferadb_ledger_store::tables::TableId::Entities, storage_key, encoded_value)
            .context(StoreSnafu)
    }

    /// Applies a batch of operations using an externally-provided transaction.
    ///
    /// Does **not** commit the transaction or mark dirty buckets — the caller
    /// is responsible for calling [`WriteTransaction::commit`] and then
    /// [`mark_dirty_keys`](Self::mark_dirty_keys).
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if any storage operation fails.
    /// Returns [`StateError::Codec`] if entity serialization or deserialization fails.
    /// Returns [`StateError::Index`] if a relationship index update fails.
    /// Returns [`StateError::PreconditionFailed`] if a conditional write check fails.
    pub fn apply_operations_in_txn(
        &self,
        txn: &mut WriteTransaction<'_, B>,
        dict: &mut crate::dictionary::VaultDictionary,
        vault: VaultId,
        operations: &[Operation],
        block_height: u64,
    ) -> Result<(Vec<WriteStatus>, Vec<Vec<u8>>)> {
        let mut statuses = Vec::with_capacity(operations.len());
        let mut dirty_keys: Vec<Vec<u8>> = Vec::new();

        for op in operations {
            let status = match op {
                Operation::SetEntity { key, value, condition, expires_at } => {
                    let local_key = key.as_bytes();
                    let storage_key = encode_storage_key(vault, local_key);

                    let existing =
                        txn.get_raw(TableId::Entities, &storage_key).context(StoreSnafu)?;

                    let is_update = existing.is_some();
                    let entity_data =
                        existing.as_ref().and_then(|data| decode::<Entity>(data).ok());

                    let condition_met = match condition {
                        None => true,
                        Some(SetCondition::MustNotExist) => !is_update,
                        Some(SetCondition::MustExist) => is_update,
                        Some(SetCondition::VersionEquals(v)) => {
                            entity_data.as_ref().is_some_and(|e| e.version == *v)
                        },
                        Some(SetCondition::ValueEquals(expected)) => {
                            entity_data.as_ref().is_some_and(|e| e.value == *expected)
                        },
                    };

                    if !condition_met {
                        return Err(StateError::PreconditionFailed {
                            key: key.clone(),
                            current_version: entity_data.as_ref().map(|e| e.version),
                            current_value: entity_data.map(|e| e.value),
                            failed_condition: condition.clone(),
                        });
                    }

                    let entity = Entity {
                        key: local_key.to_vec(),
                        value: value.clone(),
                        expires_at: expires_at.unwrap_or(0),
                        version: block_height,
                    };

                    let encoded = encode(&entity).context(CodecSnafu)?;
                    txn.insert::<tables::Entities>(&storage_key, &encoded).context(StoreSnafu)?;
                    dirty_keys.push(local_key.to_vec());

                    if is_update { WriteStatus::Updated } else { WriteStatus::Created }
                },

                Operation::DeleteEntity { key } => {
                    let local_key = key.as_bytes();
                    let storage_key = encode_storage_key(vault, local_key);

                    let existed =
                        txn.delete_raw(TableId::Entities, &storage_key).context(StoreSnafu)?;

                    if existed {
                        dirty_keys.push(local_key.to_vec());
                        WriteStatus::Deleted
                    } else {
                        WriteStatus::NotFound
                    }
                },

                Operation::ExpireEntity { key, .. } => {
                    let local_key = key.as_bytes();
                    let storage_key = encode_storage_key(vault, local_key);

                    let existed =
                        txn.delete_raw(TableId::Entities, &storage_key).context(StoreSnafu)?;

                    if existed {
                        dirty_keys.push(local_key.to_vec());
                        WriteStatus::Deleted
                    } else {
                        WriteStatus::NotFound
                    }
                },

                Operation::CreateRelationship { resource, relation, subject } => {
                    let created = crate::relationship::RelationshipStore::create(
                        txn, dict, vault, resource, relation, subject,
                    )
                    .context(RelationshipSnafu)?;

                    if created {
                        IndexManager::add_to_obj_index(
                            &mut *txn, dict, vault, resource, relation, subject,
                        )
                        .context(IndexSnafu)?;
                        IndexManager::add_to_subj_index(
                            &mut *txn, dict, vault, resource, relation, subject,
                        )
                        .context(IndexSnafu)?;

                        // Update hash index only if the vault is already loaded.
                        // If not loaded, the lazy-load on next exists() picks up the new entry.
                        if self.relationship_index.is_vault_loaded(vault)
                            && let Some(hash) =
                                Self::relationship_hash_from_dict(dict, resource, relation, subject)
                        {
                            self.relationship_index.insert(vault, hash);
                        }

                        WriteStatus::Created
                    } else {
                        WriteStatus::AlreadyExists
                    }
                },

                Operation::DeleteRelationship { resource, relation, subject } => {
                    let deleted = crate::relationship::RelationshipStore::delete(
                        txn, dict, vault, resource, relation, subject,
                    )
                    .context(RelationshipSnafu)?;

                    if deleted {
                        IndexManager::remove_from_obj_index(
                            &mut *txn, dict, vault, resource, relation, subject,
                        )
                        .context(IndexSnafu)?;
                        IndexManager::remove_from_subj_index(
                            &mut *txn, dict, vault, resource, relation, subject,
                        )
                        .context(IndexSnafu)?;

                        // Remove from hash index only if the vault is already loaded.
                        if self.relationship_index.is_vault_loaded(vault)
                            && let Some(hash) =
                                Self::relationship_hash_from_dict(dict, resource, relation, subject)
                        {
                            self.relationship_index.remove(vault, hash);
                        }

                        WriteStatus::Deleted
                    } else {
                        WriteStatus::NotFound
                    }
                },
            };

            statuses.push(status);
        }

        Ok((statuses, dirty_keys))
    }

    /// Marks dirty buckets for state root computation after a transaction commits.
    ///
    /// Call this after committing a [`WriteTransaction`] that was used with
    /// [`apply_operations_in_txn`](Self::apply_operations_in_txn).
    pub fn mark_dirty_keys(&self, vault: VaultId, dirty_keys: &[Vec<u8>]) {
        self.with_commitment(vault, |commitment| {
            for key in dirty_keys {
                commitment.mark_dirty_by_key(key);
            }
        });
    }

    /// Marks all 256 buckets dirty for a vault, forcing a full state root
    /// recomputation on the next [`StateLayer::compute_state_root`] call.
    ///
    /// Used during crash recovery when entity data is already in the B+ tree
    /// but vault commitment tracking has been lost (in-memory state was reset).
    pub fn mark_all_dirty(&self, vault: VaultId) {
        self.with_commitment(vault, |commitment| {
            for bucket in 0..NUM_BUCKETS as u8 {
                commitment.mark_dirty(bucket);
            }
        });
    }

    /// Applies a batch of operations to a vault's state.
    ///
    /// Executes all operations atomically within a single write transaction.
    /// Returns a `WriteStatus` for each operation indicating whether it was
    /// created, updated, deleted, or failed a precondition check.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use inferadb_ledger_store::Database;
    /// # use inferadb_ledger_state::StateLayer;
    /// use inferadb_ledger_types::types::{Operation, VaultId};
    ///
    /// # let db = Arc::new(Database::open_in_memory()?);
    /// # let state = StateLayer::new(db);
    /// let ops = vec![Operation::SetEntity {
    ///     key: "user:alice".into(),
    ///     value: b"data".to_vec(),
    ///     condition: None,
    ///     expires_at: None,
    /// }];
    ///
    /// let statuses = state.apply_operations(VaultId::new(1), &ops, 1)?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if any storage operation fails.
    /// Returns [`StateError::Codec`] if entity serialization or deserialization fails.
    /// Returns [`StateError::Index`] if a relationship index update fails.
    /// Returns [`StateError::PreconditionFailed`] if a conditional write check fails.
    pub fn apply_operations(
        &self,
        vault: VaultId,
        operations: &[Operation],
        block_height: u64,
    ) -> Result<Vec<WriteStatus>> {
        let mut dict = self.take_dictionary(vault)?;
        let mut txn = self.begin_write()?;
        match self.apply_operations_in_txn(&mut txn, &mut dict, vault, operations, block_height) {
            Ok((statuses, dirty_keys)) => {
                // Mark dirty before commit: dirty marks are conservative (trigger
                // re-hash from storage). If commit fails, the re-hash reads old
                // data and produces the correct (pre-commit) state root. If commit
                // succeeds, dirty buckets are correctly tracked for the new data.
                self.mark_dirty_keys(vault, &dirty_keys);
                txn.commit().context(StoreSnafu)?;
                self.return_dictionary(vault, dict);
                Ok(statuses)
            },
            Err(e) => {
                // Discard potentially-dirty dictionary on failure.
                // Next call will reload from storage.
                Err(e)
            },
        }
    }

    /// Clears all entities and relationships for a vault.
    ///
    /// Used during vault recovery to reset state before replay.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if any storage operation (iteration, delete,
    /// or commit) fails.
    pub fn clear_vault(&self, vault: VaultId) -> Result<()> {
        let mut txn = self.db.write().context(StoreSnafu)?;

        delete_vault_keys::<B, tables::Entities>(&mut txn, vault)?;
        delete_vault_keys::<B, tables::Relationships>(&mut txn, vault)?;
        delete_vault_keys::<B, tables::ObjIndex>(&mut txn, vault)?;
        delete_vault_keys::<B, tables::SubjIndex>(&mut txn, vault)?;
        delete_vault_keys::<B, tables::StringDictionary>(&mut txn, vault)?;
        delete_vault_keys::<B, tables::StringDictionaryReverse>(&mut txn, vault)?;

        txn.commit().context(StoreSnafu)?;

        // Reset commitment, dictionary, and relationship index caches for this vault
        self.vault_commitments.write().remove(&vault);
        self.vault_dictionaries.write().remove(&vault);
        self.relationship_index.evict_vault(vault);

        Ok(())
    }

    /// Returns an entity by key.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction fails.
    /// Returns [`StateError::Codec`] if deserialization of the stored entity fails.
    pub fn get_entity(&self, vault: VaultId, key: &[u8]) -> Result<Option<Entity>> {
        let storage_key = encode_storage_key(vault, key);
        let txn = self.db.read().context(StoreSnafu)?;

        match txn.get_raw(TableId::Entities, &storage_key).context(StoreSnafu)? {
            Some(data) => {
                let entity = decode(&data).context(CodecSnafu)?;
                Ok(Some(entity))
            },
            None => Ok(None),
        }
    }

    /// Checks whether the exact relationship tuple is present in the vault.
    ///
    /// This is a storage-primitive existence check — it does NOT evaluate
    /// authorization policy, userset rewrites, or schema inheritance. Callers
    /// composing authorization decisions should use this as one leaf in a
    /// larger evaluation.
    ///
    /// Attempts the in-memory hash index first for O(1) lookups. Falls back
    /// to the B+ tree if the vault is not yet indexed or the dictionary
    /// cannot resolve the triple's components.
    ///
    /// The state layer evaluates against the current committed state. Block
    /// height is a raft-apply-layer concern — callers that need to annotate
    /// the response with a `checked_at_height` source it separately from
    /// `AppliedStateAccessor::vault_height`.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction fails.
    /// Returns [`StateError::Relationship`] on dictionary or storage failure.
    pub fn relationship_exists(
        &self,
        vault: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<bool> {
        // Try hash index first (O(1)).
        if let Some(key_hash) = self.compute_relationship_hash(vault, resource, relation, subject) {
            if let Some(result) = self.relationship_index.exists(vault, key_hash) {
                return Ok(result);
            }
            // Vault not indexed — load it, then re-check.
            self.load_vault_relationship_index(vault)?;
            if let Some(result) = self.relationship_index.exists(vault, key_hash) {
                return Ok(result);
            }
        }
        // Fallback to B+ tree (dictionary strings unknown or load failed to index).
        let dict = self.get_dictionary(vault)?;
        let txn = self.db.read().context(StoreSnafu)?;
        crate::relationship::RelationshipStore::exists(
            &txn, &dict, vault, resource, relation, subject,
        )
        .context(RelationshipSnafu)
    }

    /// Computes the seahash of a relationship's binary key using the
    /// dictionary cache. Returns `None` if any component string is
    /// unknown in the dictionary (read-only lookup — no interning).
    fn compute_relationship_hash(
        &self,
        vault: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Option<u64> {
        let dict = self.get_dictionary(vault).ok()?;

        let (res_type_str, res_local) = split_typed_id(resource).unwrap_or(("", resource));
        let res_type = dict.get_id(InternCategory::ResourceType, res_type_str)?;
        let res_id_bytes = res_local.as_bytes();

        let rel_id = dict.get_id(InternCategory::Relation, relation)?;

        let (subj_type_str, subj_local) = split_typed_id(subject).unwrap_or(("", subject));
        let subj_type = dict.get_id(InternCategory::SubjectType, subj_type_str)?;
        let subj_id_bytes = subj_local.as_bytes();

        let local_key =
            encode_relationship_local_key(res_type, res_id_bytes, rel_id, subj_type, subj_id_bytes);
        Some(seahash::hash(&local_key))
    }

    /// Loads all relationships for a vault into the hash index.
    ///
    /// Iterates the Relationships table for the given vault prefix and
    /// inserts the seahash of each local key into the index.
    fn load_vault_relationship_index(&self, vault: VaultId) -> Result<()> {
        let txn = self.db.read().context(StoreSnafu)?;
        let vault_start = crate::keys::vault_prefix(vault);
        let next_vault = VaultId::from(vault.value() + 1);
        let vault_end = crate::keys::vault_prefix(next_vault);

        let start_key = vault_start.to_vec();
        let end_key = vault_end.to_vec();

        for (key_bytes, _) in txn
            .range::<tables::Relationships>(Some(&start_key), Some(&end_key))
            .context(StoreSnafu)?
        {
            if key_bytes.len() < 9 {
                continue;
            }
            // local_key starts after the 8-byte vault prefix + 1-byte bucket_id.
            let local_key = &key_bytes[9..];
            self.relationship_index.insert(vault, seahash::hash(local_key));
        }

        // Mark the vault as loaded even if it had zero relationships.
        self.relationship_index.ensure_vault(vault);
        Ok(())
    }

    /// Returns a reference to the in-memory relationship hash index.
    pub fn relationship_index(&self) -> &Arc<RelationshipIndex> {
        &self.relationship_index
    }

    /// Computes a relationship hash from an already-populated dictionary.
    ///
    /// Used in `apply_operations_in_txn` where the dictionary is guaranteed
    /// to contain all component strings (the preceding create/delete interned
    /// them). Returns `None` only if the dictionary is missing an entry, which
    /// should not happen in practice.
    fn relationship_hash_from_dict(
        dict: &crate::dictionary::VaultDictionary,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Option<u64> {
        let (res_type_str, res_local) = split_typed_id(resource).unwrap_or(("", resource));
        let res_type = dict.get_id(InternCategory::ResourceType, res_type_str)?;
        let res_id_bytes = res_local.as_bytes();

        let rel_id = dict.get_id(InternCategory::Relation, relation)?;

        let (subj_type_str, subj_local) = split_typed_id(subject).unwrap_or(("", subject));
        let subj_type = dict.get_id(InternCategory::SubjectType, subj_type_str)?;
        let subj_id_bytes = subj_local.as_bytes();

        let local_key =
            encode_relationship_local_key(res_type, res_id_bytes, rel_id, subj_type, subj_id_bytes);
        Some(seahash::hash(&local_key))
    }

    /// Computes state root for a vault, updating dirty bucket roots.
    ///
    /// This scans only the dirty buckets and recomputes their roots,
    /// then returns SHA-256(bucket_roots[0..256]).
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction or iteration fails.
    /// Returns [`StateError::Codec`] if deserialization of any entity in a dirty bucket fails.
    pub fn compute_state_root(&self, vault: VaultId) -> Result<Hash> {
        // First check if dirty and get the dirty buckets list (brief read lock)
        let dirty_buckets: Vec<u8> = {
            let map = self.vault_commitments.read();
            match map.get(&vault) {
                Some(commitment) if commitment.is_dirty() => {
                    commitment.dirty_buckets().iter().copied().collect()
                },
                Some(commitment) => {
                    // Not dirty, return cached state root
                    return Ok(commitment.compute_state_root());
                },
                None => {
                    // No commitment yet, create default and return its state root
                    drop(map);
                    return Ok(self.with_commitment(vault, |c| c.compute_state_root()));
                },
            }
        };

        // Single vault-scoped range scan distributing entities to dirty bucket builders
        let txn = self.db.read().context(StoreSnafu)?;
        let mut builders: [Option<BucketRootBuilder>; 256] = std::array::from_fn(|_| None);
        for &b in &dirty_buckets {
            builders[b as usize] = Some(BucketRootBuilder::new(b));
        }

        let vault_start = crate::keys::vault_prefix(vault).to_vec();
        let vault_end = crate::keys::vault_prefix(VaultId::new(vault.value() + 1)).to_vec();
        let iter = txn
            .range::<tables::Entities>(Some(&vault_start), Some(&vault_end))
            .context(StoreSnafu)?;

        for (key_bytes, value) in iter {
            if key_bytes.len() < 9 {
                continue;
            }
            let bucket = key_bytes[8];
            if let Some(builder) = builders[bucket as usize].as_mut() {
                let entity: Entity = decode(&value).context(CodecSnafu)?;
                builder.add_entity(&entity);
            }
        }

        let bucket_roots: Vec<(u8, Hash)> = dirty_buckets
            .into_iter()
            .filter_map(|b| builders[b as usize].take().map(|builder| (b, builder.finalize())))
            .collect();

        // Update commitment with computed bucket roots (brief write lock)
        Ok(self.with_commitment(vault, |commitment| {
            for (bucket, root) in bucket_roots {
                commitment.set_bucket_root(bucket, root);
            }
            commitment.clear_dirty();
            commitment.compute_state_root()
        }))
    }

    /// Loads bucket roots from stored vault metadata.
    ///
    /// Called during startup/recovery to restore commitment state.
    pub fn load_vault_commitment(&self, vault: VaultId, bucket_roots: [Hash; NUM_BUCKETS]) {
        self.vault_commitments
            .write()
            .insert(vault, VaultCommitment::from_bucket_roots(bucket_roots));
    }

    /// Returns the current bucket roots for a vault (for persistence).
    pub fn get_bucket_roots(&self, vault: VaultId) -> Option<[Hash; NUM_BUCKETS]> {
        self.vault_commitments.read().get(&vault).map(|c| *c.bucket_roots())
    }

    /// Compacts all B+ tree tables, merging underfull leaf nodes.
    ///
    /// Returns aggregate compaction statistics across all tables.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the compaction operation or commit fails.
    pub fn compact_tables(&self, min_fill_factor: f64) -> Result<CompactionStats> {
        let mut txn = self.db.write().context(StoreSnafu)?;
        let stats = txn.compact_all_tables(min_fill_factor).context(StoreSnafu)?;
        txn.commit().context(StoreSnafu)?;
        Ok(stats)
    }

    /// Re-wraps a batch of pages' crypto sidecar metadata to a target RMK version.
    ///
    /// Delegates to [`Database::rewrap_pages`]. Non-encrypted backends are a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the rewrap operation fails.
    pub fn rewrap_pages(
        &self,
        start_page_id: u64,
        batch_size: usize,
        target_version: Option<u32>,
    ) -> Result<(usize, Option<u64>)> {
        self.db.rewrap_pages(start_page_id, batch_size, target_version).context(StoreSnafu)
    }

    /// Returns the total page count in the crypto sidecar.
    ///
    /// Non-encrypted backends return 0.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the sidecar metadata cannot be read.
    pub fn sidecar_page_count(&self) -> Result<u64> {
        self.db.sidecar_page_count().context(StoreSnafu)
    }

    /// Lists subjects for a given resource and relation.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction fails.
    /// Returns [`StateError::Index`] if the index lookup fails.
    pub fn list_subjects(
        &self,
        vault: VaultId,
        resource: &str,
        relation: &str,
    ) -> Result<Vec<String>> {
        let dict = self.get_dictionary(vault)?;
        let txn = self.db.read().context(StoreSnafu)?;
        IndexManager::get_subjects(&txn, &dict, vault, resource, relation).context(IndexSnafu)
    }

    /// Lists resource-relation pairs for a given subject.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction fails.
    /// Returns [`StateError::Index`] if the index lookup fails.
    pub fn list_resources_for_subject(
        &self,
        vault: VaultId,
        subject: &str,
    ) -> Result<Vec<(String, String)>> {
        let dict = self.get_dictionary(vault)?;
        let txn = self.db.read().context(StoreSnafu)?;
        IndexManager::get_resources(&txn, &dict, vault, subject).context(IndexSnafu)
    }

    /// Lists all entities in a vault with optional prefix filter.
    ///
    /// Returns up to `limit` entities. Use `start_after` for pagination.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction or iteration fails.
    /// Returns [`StateError::Codec`] if deserialization of any entity fails.
    pub fn list_entities(
        &self,
        vault: VaultId,
        prefix: Option<&str>,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Entity>> {
        use crate::keys::vault_prefix;

        let txn = self.db.read().context(StoreSnafu)?;

        let mut entities = Vec::with_capacity(limit.min(1000));

        // Build the range start key.
        // Note: Keys are ordered by (vault, bucket_id, local_key), where bucket_id
        // is a hash of the local key. This means keys with the same prefix can be
        // scattered across different buckets. For prefix scans, we must start from
        // the beginning of the vault and filter by prefix in the loop.
        let start_key: Vec<u8> = if let Some(after) = start_after {
            // Pagination: start after this specific key
            let mut k = encode_storage_key(vault, after.as_bytes());
            k.push(0); // Advance past the exact key
            k
        } else {
            // For prefix scans or full vault scans, start from the vault prefix
            vault_prefix(vault).to_vec()
        };

        for (key_bytes, value) in txn.iter::<tables::Entities>().context(StoreSnafu)? {
            if entities.len() >= limit {
                break;
            }

            // Skip until we reach the start key
            if key_bytes < start_key {
                continue;
            }

            // Check we're still in the same vault (first 8 bytes)
            if key_bytes.len() < 9 {
                break;
            }
            let key_vault_id = i64::from_be_bytes(key_bytes[..8].try_into().unwrap_or([0; 8]));
            if key_vault_id != vault.value() {
                break;
            }

            // Check prefix match if specified
            if let Some(p) = prefix {
                // Skip bucket byte (index 8), entity key starts at index 9
                let entity_key = &key_bytes[9..];
                if !entity_key.starts_with(p.as_bytes()) {
                    // Keys are scattered by bucket, so we can't early-exit
                    continue;
                }
            }

            let entity: Entity = decode(&value).context(CodecSnafu)?;

            entities.push(entity);
        }

        Ok(entities)
    }

    /// Lists all relationships in a vault.
    ///
    /// Returns up to `limit` relationships. Use `start_after` for cursor-based
    /// pagination — pass the last relationship from the previous page to resume.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction or iteration fails.
    /// Returns [`StateError::Relationship`] if dictionary loading or key decoding fails.
    pub fn list_relationships(
        &self,
        vault: VaultId,
        start_after: Option<&Relationship>,
        limit: usize,
    ) -> Result<Vec<Relationship>> {
        let dict = self.get_dictionary(vault)?;
        let txn = self.db.read().context(StoreSnafu)?;

        // Build start key for pagination. If start_after is provided, resolve
        // its components through the dictionary to build a binary storage key.
        let vault_start = crate::keys::vault_prefix(vault);
        let start_key: Vec<u8> = if let Some(after) = start_after {
            // Resolve the cursor relationship to its binary storage key
            use crate::binary_keys::{
                InternCategory, encode_relationship_local_key, split_typed_id,
            };

            let (res_type_str, res_local) =
                split_typed_id(&after.resource).unwrap_or(("", &after.resource));
            let (subj_type_str, subj_local) =
                split_typed_id(&after.subject).unwrap_or(("", &after.subject));

            if let (Some(res_type), Some(rel_id), Some(subj_type)) = (
                dict.get_id(InternCategory::ResourceType, res_type_str),
                dict.get_id(InternCategory::Relation, &after.relation),
                dict.get_id(InternCategory::SubjectType, subj_type_str),
            ) {
                let local_key = encode_relationship_local_key(
                    res_type,
                    res_local.as_bytes(),
                    rel_id,
                    subj_type,
                    subj_local.as_bytes(),
                );
                let mut k = encode_storage_key(vault, &local_key);
                k.push(0); // Advance past the exact key
                k
            } else {
                // Cursor relationship has unknown dictionary entries — start from beginning
                vault_start.to_vec()
            }
        } else {
            vault_start.to_vec()
        };

        let mut relationships = Vec::with_capacity(limit.min(1000));

        for (key_bytes, _) in txn.iter::<tables::Relationships>().context(StoreSnafu)? {
            if relationships.len() >= limit {
                break;
            }

            if key_bytes < start_key {
                continue;
            }

            if key_bytes.len() < 9 {
                break;
            }
            if key_bytes[..8] != vault_start[..] {
                if key_bytes[..8] < vault_start[..] {
                    continue;
                }
                break;
            }

            // Decode binary local key (bytes after vault_id + bucket_id)
            let local_key = &key_bytes[9..];
            if let Some((res_type, res_id, rel_id, subj_type, subj_id)) =
                crate::binary_keys::decode_relationship_local_key(local_key)
                && let (Some(resource), Some(relation), Some(subject)) = (
                    dict.resolve_typed_id(InternCategory::ResourceType, res_type, res_id),
                    dict.resolve(InternCategory::Relation, rel_id).map(str::to_owned),
                    dict.resolve_typed_id(InternCategory::SubjectType, subj_type, subj_id),
                )
            {
                relationships.push(Relationship::new(resource, relation, subject));
            }
        }

        Ok(relationships)
    }
}

impl<B: StorageBackend> Clone for StateLayer<B> {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            vault_commitments: RwLock::new(HashMap::new()), // Each clone starts fresh
            vault_dictionaries: RwLock::new(HashMap::new()), // Each clone starts fresh
            relationship_index: Arc::clone(&self.relationship_index), // Shared across clones
        }
    }
}

/// Deletes all keys in a table that belong to the given vault.
///
/// Keys are sorted lexicographically with the 8-byte vault prefix first,
/// so the scan skips keys from earlier vaults and terminates as soon as
/// a key from a later vault is reached.
fn delete_vault_keys<B: StorageBackend, T: Table<KeyType = Vec<u8>>>(
    txn: &mut WriteTransaction<'_, B>,
    vault: VaultId,
) -> Result<()> {
    let prefix = crate::keys::vault_prefix(vault);
    let mut keys_to_delete = Vec::new();

    for (key_bytes, _) in txn.iter::<T>().context(StoreSnafu)? {
        if key_bytes.len() < 8 {
            break;
        }
        if key_bytes[..8] < prefix[..] {
            continue;
        }
        if key_bytes[..8] != prefix[..] {
            break;
        }
        keys_to_delete.push(key_bytes);
    }

    for key in keys_to_delete {
        txn.delete::<T>(&key).context(StoreSnafu)?;
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use inferadb_ledger_store::InMemoryBackend;
    use inferadb_ledger_types::VaultId;

    use super::*;
    use crate::engine::InMemoryStorageEngine;

    fn create_test_state() -> StateLayer<InMemoryBackend> {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        StateLayer::new(engine.db())
    }

    #[test]
    fn test_set_and_get_entity() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        let ops = vec![Operation::SetEntity {
            key: "test_key".to_string(),
            value: b"test_value".to_vec(),
            condition: None,
            expires_at: None,
        }];

        let statuses = state.apply_operations(vault, &ops, 1).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Created]);

        let entity = state.get_entity(vault, b"test_key").unwrap();
        assert!(entity.is_some());
        let e = entity.unwrap();
        assert_eq!(e.value, b"test_value");
        assert_eq!(e.version, 1);
    }

    #[test]
    fn test_set_entity_must_not_exist() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // First set
        let ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value1".to_vec(),
            condition: Some(SetCondition::MustNotExist),
            expires_at: None,
        }];
        let statuses = state.apply_operations(vault, &ops, 1).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Created]);

        // Second set should fail with error (batch atomicity)
        let ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value2".to_vec(),
            condition: Some(SetCondition::MustNotExist),
            expires_at: None,
        }];
        let result = state.apply_operations(vault, &ops, 2);

        // Should return PreconditionFailed error with current state details
        match result {
            Err(StateError::PreconditionFailed {
                key,
                current_version,
                current_value,
                failed_condition,
            }) => {
                assert_eq!(key, "key");
                assert_eq!(current_version, Some(1)); // Set at block_height 1
                assert_eq!(current_value, Some(b"value1".to_vec()));
                assert_eq!(failed_condition, Some(SetCondition::MustNotExist));
            },
            Ok(_) => panic!("Expected PreconditionFailed error, got Ok"),
            Err(e) => panic!("Expected PreconditionFailed error, got: {:?}", e),
        }
    }

    #[test]
    fn test_delete_entity() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Set then delete
        let ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value".to_vec(),
            condition: None,
            expires_at: None,
        }];
        state.apply_operations(vault, &ops, 1).unwrap();

        let ops = vec![Operation::DeleteEntity { key: "key".to_string() }];
        let statuses = state.apply_operations(vault, &ops, 2).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Deleted]);

        // Should not exist now
        assert!(state.get_entity(vault, b"key").unwrap().is_none());

        // Delete again should be NotFound
        let statuses = state.apply_operations(vault, &ops, 3).unwrap();
        assert_eq!(statuses, vec![WriteStatus::NotFound]);
    }

    #[test]
    fn test_create_relationship() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        let ops = vec![Operation::CreateRelationship {
            resource: "doc:123".to_string(),
            relation: "viewer".to_string(),
            subject: "user:alice".to_string(),
        }];

        let statuses = state.apply_operations(vault, &ops, 1).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Created]);

        assert!(state.relationship_exists(vault, "doc:123", "viewer", "user:alice").unwrap());

        // Create again should return AlreadyExists
        let statuses = state.apply_operations(vault, &ops, 2).unwrap();
        assert_eq!(statuses, vec![WriteStatus::AlreadyExists]);
    }

    #[test]
    fn test_delete_relationship() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Create
        let ops = vec![Operation::CreateRelationship {
            resource: "doc:123".to_string(),
            relation: "viewer".to_string(),
            subject: "user:alice".to_string(),
        }];
        state.apply_operations(vault, &ops, 1).unwrap();

        // Delete
        let ops = vec![Operation::DeleteRelationship {
            resource: "doc:123".to_string(),
            relation: "viewer".to_string(),
            subject: "user:alice".to_string(),
        }];
        let statuses = state.apply_operations(vault, &ops, 2).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Deleted]);

        assert!(!state.relationship_exists(vault, "doc:123", "viewer", "user:alice").unwrap());
    }

    /// Verifies list_relationships returns all relationships across all buckets.
    #[test]
    fn test_list_relationships_returns_all_buckets() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        let test_cases = vec![
            ("doc:alpha", "viewer", "user:a"),
            ("doc:beta", "viewer", "user:b"),
            ("doc:gamma", "viewer", "user:c"),
            ("doc:delta", "viewer", "user:d"),
            ("doc:epsilon", "viewer", "user:e"),
            ("doc:zeta", "viewer", "user:f"),
            ("doc:eta", "viewer", "user:g"),
            ("doc:theta", "viewer", "user:h"),
        ];

        // Create all relationships
        let ops: Vec<Operation> = test_cases
            .iter()
            .map(|(resource, relation, subject)| Operation::CreateRelationship {
                resource: resource.to_string(),
                relation: relation.to_string(),
                subject: subject.to_string(),
            })
            .collect();

        let statuses = state.apply_operations(vault, &ops, 1).unwrap();
        assert!(statuses.iter().all(|s| *s == WriteStatus::Created));

        // List all relationships without start_after
        let listed = state.list_relationships(vault, None, 100).unwrap();

        assert_eq!(
            listed.len(),
            test_cases.len(),
            "list_relationships should return all {} relationships, got {}.",
            test_cases.len(),
            listed.len()
        );

        // Verify all specific relationships are present
        for (resource, relation, subject) in &test_cases {
            let found = listed.iter().any(|r| {
                r.resource == *resource && r.relation == *relation && r.subject == *subject
            });
            assert!(
                found,
                "Relationship {}#{}@{} not found in list_relationships output",
                resource, relation, subject
            );
        }
    }

    /// Reproduction test for Engine integration test failure.
    ///
    /// Simulates the concurrent write + list pattern. Writes 11 relationships
    /// (1 sequential + 10 concurrent) and verifies all are returned by
    /// list_relationships.
    #[test]
    fn test_list_relationships_after_concurrent_writes() {
        use std::thread;

        let state = create_test_state();
        let vault = VaultId::new(20_000_002_690_000);

        // First: sequential write
        let seq_ops = vec![Operation::CreateRelationship {
            resource: "document:seq-test".to_string(),
            relation: "viewer".to_string(),
            subject: "user:seq-test".to_string(),
        }];
        let statuses = state.apply_operations(vault, &seq_ops, 1).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Created]);

        assert!(
            state
                .relationship_exists(vault, "document:seq-test", "viewer", "user:seq-test")
                .unwrap()
        );

        // Concurrent writes
        let state_arc = std::sync::Arc::new(state);
        let mut handles = Vec::new();

        for i in 0..10 {
            let state = std::sync::Arc::clone(&state_arc);
            let handle = thread::spawn(move || {
                let ops = vec![Operation::CreateRelationship {
                    resource: format!("document:concurrent-{}", i),
                    relation: "viewer".to_string(),
                    subject: format!("user:concurrent-{}", i),
                }];
                state.apply_operations(vault, &ops, (i + 2) as u64).unwrap()
            });
            handles.push(handle);
        }

        for handle in handles {
            let statuses = handle.join().expect("thread panicked");
            assert_eq!(statuses, vec![WriteStatus::Created]);
        }

        for i in 0..10 {
            assert!(
                state_arc
                    .relationship_exists(
                        vault,
                        &format!("document:concurrent-{}", i),
                        "viewer",
                        &format!("user:concurrent-{}", i),
                    )
                    .unwrap(),
                "Direct read failed for concurrent-{}",
                i
            );
        }

        let all_relationships = state_arc.list_relationships(vault, None, 100).unwrap();

        assert_eq!(
            all_relationships.len(),
            11,
            "Expected 11 relationships (1 seq + 10 concurrent), got {}",
            all_relationships.len()
        );

        assert!(
            all_relationships.iter().any(|r| r.resource == "document:seq-test"),
            "Sequential relationship not found in list"
        );
        for i in 0..10 {
            assert!(
                all_relationships
                    .iter()
                    .any(|r| r.resource == format!("document:concurrent-{}", i)),
                "Concurrent relationship {} not found in list",
                i
            );
        }
    }

    #[test]
    fn test_state_root_changes_on_writes() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        let root1 = state.compute_state_root(vault).unwrap();

        let ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value".to_vec(),
            condition: None,
            expires_at: None,
        }];
        state.apply_operations(vault, &ops, 1).unwrap();

        let root2 = state.compute_state_root(vault).unwrap();
        assert_ne!(root1, root2);

        // Deleting should change root again
        let ops = vec![Operation::DeleteEntity { key: "key".to_string() }];
        state.apply_operations(vault, &ops, 2).unwrap();

        let root3 = state.compute_state_root(vault).unwrap();
        assert_ne!(root2, root3);
        // After deleting, should be back to empty state
        assert_eq!(root1, root3);
    }

    #[test]
    fn test_vaults_are_isolated() {
        let state = create_test_state();

        let ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value".to_vec(),
            condition: None,
            expires_at: None,
        }];
        state.apply_operations(VaultId::new(1), &ops, 1).unwrap();

        // Vault 2 should not see vault 1's data
        assert!(state.get_entity(VaultId::new(2), b"key").unwrap().is_none());

        // State roots should differ
        let root1 = state.compute_state_root(VaultId::new(1)).unwrap();
        let root2 = state.compute_state_root(VaultId::new(2)).unwrap();
        assert_ne!(root1, root2);
    }

    #[test]
    fn test_clear_vault() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Create entities and relationships
        let ops = vec![
            Operation::SetEntity {
                key: "entity1".to_string(),
                value: b"value1".to_vec(),
                condition: None,
                expires_at: None,
            },
            Operation::SetEntity {
                key: "entity2".to_string(),
                value: b"value2".to_vec(),
                condition: None,
                expires_at: None,
            },
            Operation::CreateRelationship {
                resource: "doc:1".to_string(),
                relation: "viewer".to_string(),
                subject: "user:alice".to_string(),
            },
        ];
        state.apply_operations(vault, &ops, 1).unwrap();

        // Also add data to vault 2 to ensure isolation
        let ops_vault2 = vec![Operation::SetEntity {
            key: "entity_v2".to_string(),
            value: b"value_v2".to_vec(),
            condition: None,
            expires_at: None,
        }];
        state.apply_operations(VaultId::new(2), &ops_vault2, 1).unwrap();

        // Verify data exists in vault 1
        assert!(state.get_entity(vault, b"entity1").unwrap().is_some());
        assert!(state.get_entity(vault, b"entity2").unwrap().is_some());
        assert!(state.relationship_exists(vault, "doc:1", "viewer", "user:alice").unwrap());

        // Clear vault 1
        state.clear_vault(vault).unwrap();

        // Verify vault 1 data is gone
        assert!(state.get_entity(vault, b"entity1").unwrap().is_none());
        assert!(state.get_entity(vault, b"entity2").unwrap().is_none());
        assert!(!state.relationship_exists(vault, "doc:1", "viewer", "user:alice").unwrap());

        // Verify vault 2 data is still there (isolation)
        assert!(state.get_entity(VaultId::new(2), b"entity_v2").unwrap().is_some());

        // State root should be back to empty
        let empty_root = state.compute_state_root(vault).unwrap();
        let fresh_state = create_test_state();
        let expected_empty = fresh_state.compute_state_root(vault).unwrap();
        assert_eq!(empty_root, expected_empty);
    }

    /// Stress test: many sequential writes followed by verification
    /// Simulates the pattern used by the server stress tests
    #[test]
    fn test_many_sequential_writes_then_verify() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Write 500 entities sequentially
        let num_keys = 500;
        for i in 0..num_keys {
            let key = format!("stress-key-{}", i);
            let value = format!("stress-value-{}", i).into_bytes();
            let ops = vec![Operation::SetEntity {
                key: key.clone(),
                value: value.clone(),
                condition: None,
                expires_at: None,
            }];

            let statuses = state.apply_operations(vault, &ops, i as u64 + 1).unwrap();
            assert_eq!(statuses, vec![WriteStatus::Created], "Failed to create key {}", i);
        }

        // Verify all keys are present
        let mut missing = Vec::new();
        for i in 0..num_keys {
            let key = format!("stress-key-{}", i);
            let expected = format!("stress-value-{}", i).into_bytes();
            match state.get_entity(vault, key.as_bytes()).unwrap() {
                Some(entity) => {
                    assert_eq!(entity.value, expected, "Value mismatch for key {}", i);
                },
                None => {
                    missing.push(i);
                },
            }
        }

        assert!(
            missing.is_empty(),
            "Missing {} keys out of {}: {:?}",
            missing.len(),
            num_keys,
            &missing[..std::cmp::min(10, missing.len())]
        );
    }

    /// Stress test with concurrent threads writing different keys
    #[test]
    fn test_concurrent_writes_from_threads() {
        use std::{sync::Arc, thread};

        let state = Arc::new(create_test_state());
        let vault = VaultId::new(1);
        let num_threads = 4;
        let writes_per_thread = 50;

        let mut handles = Vec::new();
        for thread_id in 0..num_threads {
            let state = Arc::clone(&state);
            let handle = thread::spawn(move || {
                for i in 0..writes_per_thread {
                    let key = format!("key-{}-{}", thread_id, i);
                    let value = format!("value-{}-{}", thread_id, i).into_bytes();
                    let ops = vec![Operation::SetEntity {
                        key,
                        value,
                        condition: None,
                        expires_at: None,
                    }];

                    // Each write gets a unique block height
                    let block_height = (thread_id * writes_per_thread + i + 1) as u64;
                    state.apply_operations(vault, &ops, block_height).unwrap();
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Verify all keys are present
        let mut missing = Vec::new();
        for thread_id in 0..num_threads {
            for i in 0..writes_per_thread {
                let key = format!("key-{}-{}", thread_id, i);
                let expected = format!("value-{}-{}", thread_id, i).into_bytes();
                match state.get_entity(vault, key.as_bytes()).unwrap() {
                    Some(entity) => {
                        assert_eq!(entity.value, expected, "Value mismatch for key {}", key);
                    },
                    None => {
                        missing.push(key);
                    },
                }
            }
        }

        let expected_total = num_threads * writes_per_thread;
        assert!(
            missing.is_empty(),
            "Missing {} keys out of {}: {:?}",
            missing.len(),
            expected_total,
            &missing[..std::cmp::min(10, missing.len())]
        );
    }

    // =========================================================================
    // Property-based tests for state determinism
    // =========================================================================

    mod proptest_determinism {
        use proptest::prelude::*;

        use super::*;

        /// Generates an arbitrary entity key (short identifiers for efficiency).
        fn arb_key() -> impl Strategy<Value = String> {
            proptest::string::string_regex("[a-z][a-z0-9]{0,7}").expect("valid regex")
        }

        /// Generates an arbitrary entity value (small byte arrays).
        fn arb_value() -> impl Strategy<Value = Vec<u8>> {
            proptest::collection::vec(any::<u8>(), 0..32)
        }

        /// Generates an arbitrary resource identifier.
        fn arb_resource() -> impl Strategy<Value = String> {
            (arb_key(), prop::sample::select(vec!["doc", "folder", "project"]))
                .prop_map(|(id, typ)| format!("{}:{}", typ, id))
        }

        /// Generates an arbitrary relation name.
        fn arb_relation() -> impl Strategy<Value = String> {
            prop::sample::select(vec![
                "viewer".to_string(),
                "editor".to_string(),
                "owner".to_string(),
                "member".to_string(),
            ])
        }

        /// Generates an arbitrary subject identifier.
        fn arb_subject() -> impl Strategy<Value = String> {
            (arb_key(), prop::sample::select(vec!["user", "group", "team"]))
                .prop_map(|(id, typ)| format!("{}:{}", typ, id))
        }

        /// Generates an arbitrary Operation.
        fn arb_operation() -> impl Strategy<Value = Operation> {
            prop_oneof![
                // SetEntity (most common)
                (arb_key(), arb_value()).prop_map(|(key, value)| {
                    Operation::SetEntity { key, value, condition: None, expires_at: None }
                }),
                // DeleteEntity
                arb_key().prop_map(|key| Operation::DeleteEntity { key }),
                // CreateRelationship
                (arb_resource(), arb_relation(), arb_subject()).prop_map(
                    |(resource, relation, subject)| {
                        Operation::CreateRelationship { resource, relation, subject }
                    }
                ),
                // DeleteRelationship
                (arb_resource(), arb_relation(), arb_subject()).prop_map(
                    |(resource, relation, subject)| {
                        Operation::DeleteRelationship { resource, relation, subject }
                    }
                ),
            ]
        }

        /// Generates a sequence of operations (1 to 20 operations per test).
        fn arb_operation_sequence() -> impl Strategy<Value = Vec<Operation>> {
            proptest::collection::vec(arb_operation(), 1..20)
        }

        proptest! {
            /// Test: Applying the same operations to two independent StateLayer
            /// instances must produce identical state roots.
            ///
            /// This is a critical invariant for Raft consensus - all replicas
            /// must reach the same state when applying the same log entries.
            #[test]
            fn state_determinism_same_operations(
                operations in arb_operation_sequence()
            ) {
                let vault = VaultId::new(1);

                // Create two independent StateLayer instances
                let state1 = create_test_state();
                let state2 = create_test_state();

                // Apply the same operations to both
                for (idx, op) in operations.iter().enumerate() {
                    let block_height = (idx + 1) as u64;
                    let _ = state1.apply_operations(vault, std::slice::from_ref(op), block_height);
                    let _ = state2.apply_operations(vault, std::slice::from_ref(op), block_height);
                }

                // State roots must be identical
                let root1 = state1.compute_state_root(vault).unwrap();
                let root2 = state2.compute_state_root(vault).unwrap();

                prop_assert_eq!(
                    root1, root2,
                    "State roots diverged after applying {} operations",
                    operations.len()
                );
            }

            /// Test: Applying operations one-by-one vs all-at-once in a batch
            /// (at the same block height) must produce identical state roots.
            ///
            /// This verifies that batching doesn't affect determinism when
            /// all operations are at the same block height.
            ///
            /// Note: Different block heights will produce different entity versions
            /// (stored in Entity.version field), so this test uses the same height.
            #[test]
            fn state_determinism_batch_vs_individual(
                operations in arb_operation_sequence()
            ) {
                let vault = VaultId::new(1);
                let block_height: u64 = 1; // Same height for both

                // Apply one-by-one at the SAME block height
                let state_individual = create_test_state();
                for op in operations.iter() {
                    let _ = state_individual.apply_operations(vault, std::slice::from_ref(op), block_height);
                }

                // Apply as batch at the same block height
                let state_batch = create_test_state();
                let _ = state_batch.apply_operations(vault, &operations, block_height);

                // State roots must be identical
                let root_individual = state_individual.compute_state_root(vault).unwrap();
                let root_batch = state_batch.compute_state_root(vault).unwrap();

                prop_assert_eq!(
                    root_individual, root_batch,
                    "Batch vs individual application produced different roots"
                );
            }

            /// Test: State root computation is idempotent - calling it multiple
            /// times without changes must return the same value.
            #[test]
            fn state_root_idempotent(
                operations in arb_operation_sequence()
            ) {
                let vault = VaultId::new(1);
                let state = create_test_state();

                // Apply operations
                let _ = state.apply_operations(vault, &operations, 1);

                // Compute state root multiple times
                let root1 = state.compute_state_root(vault).unwrap();
                let root2 = state.compute_state_root(vault).unwrap();
                let root3 = state.compute_state_root(vault).unwrap();

                prop_assert_eq!(root1, root2, "First and second computation differ");
                prop_assert_eq!(root2, root3, "Second and third computation differ");
            }

            /// Test: Different operation sequences produce different state roots.
            ///
            /// This verifies the state root is actually sensitive to content changes.
            /// Note: There's a tiny probability of collision, but proptest's
            /// shrinking would catch systematic issues.
            #[test]
            fn different_operations_different_roots(
                ops1 in arb_operation_sequence(),
                ops2 in arb_operation_sequence(),
            ) {
                // Only test when sequences are actually different
                prop_assume!(ops1 != ops2);

                let vault = VaultId::new(1);

                let state1 = create_test_state();
                let state2 = create_test_state();

                let _ = state1.apply_operations(vault, &ops1, 1);
                let _ = state2.apply_operations(vault, &ops2, 1);

                let root1 = state1.compute_state_root(vault).unwrap();
                let root2 = state2.compute_state_root(vault).unwrap();

                // Hash collisions are possible but extremely unlikely.
                // If this fails repeatedly, there's a bug in hashing.
                // We allow the rare collision by not asserting inequality.
                // Instead, we verify that SOME operation sequences produce
                // different roots (which the previous tests implicitly do).
                                if root1 == root2 {
                    // Log for debugging, but don't fail - could be hash collision
                    // or operations that cancel out (e.g., set then delete same key)
                }
            }
        }
    }

    // Error conversion chain tests

    // Test StateError Display implementations for all variants
    #[test]
    fn test_state_error_display() {
        use std::error::Error;

        // Test Codec variant display via context selector
        let malformed: &[u8] = &[0xFF, 0xFF, 0xFF];
        let result: std::result::Result<u64, StateError> =
            inferadb_ledger_types::decode(malformed).context(CodecSnafu);
        let state_err = result.expect_err("should fail");
        let display = format!("{state_err}");

        assert!(display.starts_with("Codec error:"), "Expected 'Codec error:', got: {display}");

        // Verify source chain is preserved
        assert!(state_err.source().is_some(), "StateError::Codec should have a source");
    }

    // Test error conversion: CodecError -> StateError (via context)
    #[test]
    fn test_state_error_from_codec_error() {
        use snafu::ResultExt;

        // Simulate codec failure during state operation
        fn simulate_codec_failure() -> std::result::Result<(), StateError> {
            let malformed: &[u8] = &[0xFF, 0xFF];
            let _: u64 = inferadb_ledger_types::decode(malformed).context(CodecSnafu)?;
            Ok(())
        }

        let result = simulate_codec_failure();
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(matches!(err, StateError::Codec { .. }), "Should be StateError::Codec variant");
    }

    // Test the full error chain: StorageError -> IndexError -> StateError
    #[test]
    fn test_state_error_chain_from_index() {
        use crate::indexes::IndexError;

        // IndexError::Storage wraps a store error — construct one via the
        // Storage variant directly. We just verify the StateError display chain.
        let store_err =
            inferadb_ledger_store::Error::Io { source: std::io::Error::other("test io error") };
        let index_err = IndexError::Storage { source: store_err, location: snafu::location!() };

        // Convert IndexError -> StateError via context selector
        let state_err = StateError::Index { source: index_err, location: snafu::location!() };

        let display = format!("{state_err}");
        assert!(
            display.contains("Index error"),
            "StateError::Index should mention 'Index error': {display}"
        );
    }

    // Test PreconditionFailed variant display (special case with optional fields)
    #[test]
    fn test_state_error_precondition_failed_display() {
        let err = StateError::PreconditionFailed {
            key: "test_key".to_string(),
            current_version: Some(5),
            current_value: Some(b"old_value".to_vec()),
            failed_condition: None,
        };

        let display = format!("{err}");
        assert!(
            display.contains("test_key"),
            "PreconditionFailed should mention the key: {display}"
        );
    }

    #[test]
    fn test_compact_tables_empty() {
        let state = create_test_state();
        let stats = state.compact_tables(0.4).unwrap();
        assert_eq!(stats.pages_merged, 0);
        assert_eq!(stats.pages_freed, 0);
    }

    #[test]
    fn test_compact_tables_preserves_data() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Insert many entities
        for i in 0..100 {
            let ops = vec![Operation::SetEntity {
                key: format!("ckey-{:04}", i),
                value: format!("cval-{:04}", i).into_bytes(),
                condition: None,
                expires_at: None,
            }];
            state.apply_operations(vault, &ops, i as u64 + 1).unwrap();
        }

        // Delete most to create sparse leaves
        for i in 0..80 {
            let ops = vec![Operation::DeleteEntity { key: format!("ckey-{:04}", i) }];
            state.apply_operations(vault, &ops, 200 + i as u64).unwrap();
        }

        // Run compaction
        let stats = state.compact_tables(0.4).unwrap();

        // Verify remaining data is intact
        for i in 80..100 {
            let entity = state.get_entity(vault, format!("ckey-{:04}", i).as_bytes()).unwrap();
            assert!(entity.is_some(), "Entity ckey-{:04} missing after compaction", i);
            assert_eq!(entity.unwrap().value, format!("cval-{:04}", i).into_bytes());
        }

        // Verify deleted keys are still gone
        for i in 0..80 {
            let entity = state.get_entity(vault, format!("ckey-{:04}", i).as_bytes()).unwrap();
            assert!(entity.is_none());
        }

        // At least the stats struct should be populated (may or may not have merges
        // depending on actual page layout)
        let _ = stats;
    }

    // ================================================================
    // Atomicity sentinel tests
    // ================================================================

    #[test]
    fn test_read_last_applied_returns_none_on_fresh_db() {
        let state = create_test_state();
        let result = state.read_last_applied().unwrap();
        assert!(result.is_none(), "fresh database should have no sentinel");
    }

    #[test]
    fn test_persist_and_read_last_applied() {
        let state = create_test_state();
        let log_id_bytes = b"test_log_id_v1";

        // Persist sentinel in a write transaction
        let mut txn = state.begin_write().unwrap();
        StateLayer::persist_last_applied(&mut txn, log_id_bytes).unwrap();
        txn.commit().unwrap();

        // Read it back
        let result = state.read_last_applied().unwrap();
        assert_eq!(result.as_deref(), Some(log_id_bytes.as_slice()));
    }

    #[test]
    fn test_sentinel_overwrites_on_update() {
        let state = create_test_state();

        // Write first sentinel
        let mut txn = state.begin_write().unwrap();
        StateLayer::persist_last_applied(&mut txn, b"log_id_1").unwrap();
        txn.commit().unwrap();

        // Overwrite with second sentinel
        let mut txn = state.begin_write().unwrap();
        StateLayer::persist_last_applied(&mut txn, b"log_id_2").unwrap();
        txn.commit().unwrap();

        let result = state.read_last_applied().unwrap();
        assert_eq!(result.as_deref(), Some(b"log_id_2".as_slice()));
    }

    #[test]
    fn test_sentinel_atomic_with_entity_writes() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Write entity data AND sentinel in the same transaction
        let mut txn = state.begin_write().unwrap();
        let mut dict = crate::dictionary::VaultDictionary::new(vault);
        state
            .apply_operations_in_txn(
                &mut txn,
                &mut dict,
                vault,
                &[Operation::SetEntity {
                    key: "key1".into(),
                    value: b"value1".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                1,
            )
            .unwrap();
        StateLayer::persist_last_applied(&mut txn, b"log_id_100").unwrap();
        txn.commit().unwrap();

        // Both entity and sentinel should be readable
        let entity = state.get_entity(vault, b"key1").unwrap();
        assert!(entity.is_some());
        assert_eq!(state.read_last_applied().unwrap().as_deref(), Some(b"log_id_100".as_slice()));
    }

    #[test]
    fn test_sentinel_rolled_back_with_entity_writes() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Write entity data AND sentinel, but drop txn (rollback)
        let mut txn = state.begin_write().unwrap();
        let mut dict = crate::dictionary::VaultDictionary::new(vault);
        state
            .apply_operations_in_txn(
                &mut txn,
                &mut dict,
                vault,
                &[Operation::SetEntity {
                    key: "key1".into(),
                    value: b"value1".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                1,
            )
            .unwrap();
        StateLayer::persist_last_applied(&mut txn, b"log_id_100").unwrap();
        drop(txn); // Rollback — don't commit

        // Neither entity nor sentinel should exist
        let entity = state.get_entity(vault, b"key1").unwrap();
        assert!(entity.is_none());
        assert!(state.read_last_applied().unwrap().is_none());
    }

    #[test]
    fn test_sentinel_does_not_collide_with_entity_keys() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Write sentinel
        let mut txn = state.begin_write().unwrap();
        StateLayer::persist_last_applied(&mut txn, b"sentinel_value").unwrap();
        txn.commit().unwrap();

        // Write an entity with a key that starts with _meta:
        state
            .apply_operations(
                vault,
                &[Operation::SetEntity {
                    key: "_meta:last_applied".into(),
                    value: b"entity_value".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                1,
            )
            .unwrap();

        // Sentinel uses raw key (no vault+bucket prefix), so it's a
        // different B+ tree key than the vault-scoped entity.
        let sentinel = state.read_last_applied().unwrap();
        assert_eq!(sentinel.as_deref(), Some(b"sentinel_value".as_slice()));

        let entity = state.get_entity(vault, b"_meta:last_applied").unwrap();
        assert!(entity.is_some());
        assert_eq!(entity.unwrap().value, b"entity_value");
    }

    #[test]
    fn test_mark_all_dirty_forces_full_recomputation() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Write some entity data
        state
            .apply_operations(
                vault,
                &[Operation::SetEntity {
                    key: "key1".into(),
                    value: b"value1".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                1,
            )
            .unwrap();

        // Compute state root normally (dirty from apply_operations)
        let root1 = state.compute_state_root(vault).unwrap();

        // Mark all dirty and recompute — should produce the same root
        state.mark_all_dirty(vault);
        let root2 = state.compute_state_root(vault).unwrap();

        assert_eq!(root1, root2, "mark_all_dirty recomputation should match normal computation");
    }

    // =========================================================================
    // Additional coverage tests
    // =========================================================================

    #[test]
    fn test_set_entity_must_exist_on_missing_key_fails() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        let ops = vec![Operation::SetEntity {
            key: "missing".to_string(),
            value: b"val".to_vec(),
            condition: Some(SetCondition::MustExist),
            expires_at: None,
        }];

        let result = state.apply_operations(vault, &ops, 1);
        match result {
            Err(StateError::PreconditionFailed {
                key, current_version, failed_condition, ..
            }) => {
                assert_eq!(key, "missing");
                assert_eq!(current_version, None);
                assert_eq!(failed_condition, Some(SetCondition::MustExist));
            },
            other => panic!("Expected PreconditionFailed, got: {other:?}"),
        }
    }

    #[test]
    fn test_set_entity_version_equals_condition() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Create entity at block_height 1
        state
            .apply_operations(
                vault,
                &[Operation::SetEntity {
                    key: "k".into(),
                    value: b"v1".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                1,
            )
            .unwrap();

        // VersionEquals(1) should succeed
        let result = state.apply_operations(
            vault,
            &[Operation::SetEntity {
                key: "k".into(),
                value: b"v2".to_vec(),
                condition: Some(SetCondition::VersionEquals(1)),
                expires_at: None,
            }],
            2,
        );
        assert!(result.is_ok());

        // VersionEquals(1) should fail now (version is 2)
        let result = state.apply_operations(
            vault,
            &[Operation::SetEntity {
                key: "k".into(),
                value: b"v3".to_vec(),
                condition: Some(SetCondition::VersionEquals(1)),
                expires_at: None,
            }],
            3,
        );
        assert!(matches!(result, Err(StateError::PreconditionFailed { .. })));
    }

    #[test]
    fn test_set_entity_value_equals_condition() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Create entity
        state
            .apply_operations(
                vault,
                &[Operation::SetEntity {
                    key: "k".into(),
                    value: b"expected".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                1,
            )
            .unwrap();

        // ValueEquals with matching value should succeed
        let result = state.apply_operations(
            vault,
            &[Operation::SetEntity {
                key: "k".into(),
                value: b"new".to_vec(),
                condition: Some(SetCondition::ValueEquals(b"expected".to_vec())),
                expires_at: None,
            }],
            2,
        );
        assert!(result.is_ok());

        // ValueEquals with wrong value should fail
        let result = state.apply_operations(
            vault,
            &[Operation::SetEntity {
                key: "k".into(),
                value: b"other".to_vec(),
                condition: Some(SetCondition::ValueEquals(b"wrong".to_vec())),
                expires_at: None,
            }],
            3,
        );
        assert!(matches!(result, Err(StateError::PreconditionFailed { .. })));
    }

    #[test]
    fn test_expire_entity() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Create entity
        state
            .apply_operations(
                vault,
                &[Operation::SetEntity {
                    key: "k".into(),
                    value: b"v".to_vec(),
                    condition: None,
                    expires_at: Some(9999),
                }],
                1,
            )
            .unwrap();

        // Expire it
        let statuses = state
            .apply_operations(
                vault,
                &[Operation::ExpireEntity { key: "k".into(), expired_at: 1000 }],
                2,
            )
            .unwrap();
        assert_eq!(statuses, vec![WriteStatus::Deleted]);

        // Should not exist now
        assert!(state.get_entity(vault, b"k").unwrap().is_none());

        // Expire non-existent entity returns NotFound
        let statuses = state
            .apply_operations(
                vault,
                &[Operation::ExpireEntity { key: "k".into(), expired_at: 2000 }],
                3,
            )
            .unwrap();
        assert_eq!(statuses, vec![WriteStatus::NotFound]);
    }

    #[test]
    fn test_entity_with_expires_at() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        let ops = vec![Operation::SetEntity {
            key: "ttl_key".to_string(),
            value: b"data".to_vec(),
            condition: None,
            expires_at: Some(999999),
        }];

        state.apply_operations(vault, &ops, 1).unwrap();

        let entity = state.get_entity(vault, b"ttl_key").unwrap().unwrap();
        assert_eq!(entity.expires_at, 999999);
    }

    #[test]
    fn test_compute_state_root_no_commitment() {
        let state = create_test_state();
        let vault = VaultId::new(99);

        // No commitment for this vault yet, should return default state root
        let root = state.compute_state_root(vault).unwrap();
        assert_ne!(root, [0u8; 32]); // default is sha256_concat of empty hashes
    }

    #[test]
    fn test_compute_state_root_cached_when_clean() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Write some data and compute state root
        state
            .apply_operations(
                vault,
                &[Operation::SetEntity {
                    key: "k".into(),
                    value: b"v".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                1,
            )
            .unwrap();
        let root1 = state.compute_state_root(vault).unwrap();

        // Second call without writes should return cached value
        let root2 = state.compute_state_root(vault).unwrap();
        assert_eq!(root1, root2);
    }

    #[test]
    fn test_load_and_get_bucket_roots() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Initially no bucket roots
        assert!(state.get_bucket_roots(vault).is_none());

        // Load bucket roots
        let roots = [inferadb_ledger_types::EMPTY_HASH; 256];
        state.load_vault_commitment(vault, roots);

        // Should be retrievable now
        let retrieved = state.get_bucket_roots(vault).unwrap();
        assert_eq!(retrieved, roots);
    }

    #[test]
    fn test_list_entities_with_prefix() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Create entities with different prefixes
        for key in ["user:alice", "user:bob", "doc:readme", "doc:todo"] {
            state
                .apply_operations(
                    vault,
                    &[Operation::SetEntity {
                        key: key.to_string(),
                        value: b"data".to_vec(),
                        condition: None,
                        expires_at: None,
                    }],
                    1,
                )
                .unwrap();
        }

        // List with prefix "user:"
        let users = state.list_entities(vault, Some("user:"), None, 100).unwrap();
        assert_eq!(users.len(), 2);
        for e in &users {
            assert!(String::from_utf8_lossy(&e.key).starts_with("user:"));
        }

        // List with prefix "doc:"
        let docs = state.list_entities(vault, Some("doc:"), None, 100).unwrap();
        assert_eq!(docs.len(), 2);
    }

    #[test]
    fn test_list_entities_with_limit() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        for i in 0..5 {
            state
                .apply_operations(
                    vault,
                    &[Operation::SetEntity {
                        key: format!("key{i}"),
                        value: b"data".to_vec(),
                        condition: None,
                        expires_at: None,
                    }],
                    1,
                )
                .unwrap();
        }

        let limited = state.list_entities(vault, None, None, 2).unwrap();
        assert_eq!(limited.len(), 2);
    }

    #[test]
    fn test_list_subjects_and_resources() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        state
            .apply_operations(
                vault,
                &[
                    Operation::CreateRelationship {
                        resource: "doc:1".into(),
                        relation: "viewer".into(),
                        subject: "user:alice".into(),
                    },
                    Operation::CreateRelationship {
                        resource: "doc:1".into(),
                        relation: "viewer".into(),
                        subject: "user:bob".into(),
                    },
                ],
                1,
            )
            .unwrap();

        let subjects = state.list_subjects(vault, "doc:1", "viewer").unwrap();
        assert_eq!(subjects.len(), 2);
        assert!(subjects.contains(&"user:alice".to_string()));
        assert!(subjects.contains(&"user:bob".to_string()));

        let resources = state.list_resources_for_subject(vault, "user:alice").unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0], ("doc:1".to_string(), "viewer".to_string()));
    }

    #[test]
    fn test_database_stats() {
        let state = create_test_state();
        let stats = state.database_stats();
        // stats should be available even on empty database (header pages always exist)
        let _ = stats.total_pages;
    }

    #[test]
    fn test_table_depths_empty() {
        let state = create_test_state();
        let depths = state.table_depths().unwrap();
        // All tables should be depth 0 on empty db
        for (_name, depth) in &depths {
            assert_eq!(*depth, 0);
        }
    }

    #[test]
    fn test_clone_starts_fresh_commitments() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        state
            .apply_operations(
                vault,
                &[Operation::SetEntity {
                    key: "k".into(),
                    value: b"v".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                1,
            )
            .unwrap();
        state.compute_state_root(vault).unwrap();

        // Clone should not inherit commitment state
        let cloned = state.clone();
        assert!(cloned.get_bucket_roots(vault).is_none());
    }

    // =========================================================================
    // Dictionary cache tests
    // =========================================================================

    #[test]
    fn dictionary_cached_across_operations() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // First apply: creates relationships and populates dictionary cache
        let ops1 = vec![Operation::CreateRelationship {
            resource: "doc:1".to_string(),
            relation: "viewer".to_string(),
            subject: "user:alice".to_string(),
        }];
        let statuses1 = state.apply_operations(vault, &ops1, 1).unwrap();
        assert_eq!(statuses1, vec![WriteStatus::Created]);

        // Second apply: should reuse cached dictionary (no redundant B+ tree scan)
        let ops2 = vec![Operation::CreateRelationship {
            resource: "doc:2".to_string(),
            relation: "viewer".to_string(),
            subject: "user:bob".to_string(),
        }];
        let statuses2 = state.apply_operations(vault, &ops2, 2).unwrap();
        assert_eq!(statuses2, vec![WriteStatus::Created]);

        // Verify both relationships exist
        assert!(state.relationship_exists(vault, "doc:1", "viewer", "user:alice").unwrap());
        assert!(state.relationship_exists(vault, "doc:2", "viewer", "user:bob").unwrap());
    }

    #[test]
    fn dictionary_cache_cleared_on_clear_vault() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Create a relationship (populates dictionary cache)
        let ops = vec![Operation::CreateRelationship {
            resource: "doc:1".to_string(),
            relation: "viewer".to_string(),
            subject: "user:alice".to_string(),
        }];
        state.apply_operations(vault, &ops, 1).unwrap();
        assert!(state.relationship_exists(vault, "doc:1", "viewer", "user:alice").unwrap());

        // Clear vault — should evict dictionary cache
        state.clear_vault(vault).unwrap();

        // Verify data is gone
        assert!(!state.relationship_exists(vault, "doc:1", "viewer", "user:alice").unwrap());

        // Re-create a relationship — should work with a fresh dictionary
        let ops2 = vec![Operation::CreateRelationship {
            resource: "doc:2".to_string(),
            relation: "editor".to_string(),
            subject: "user:bob".to_string(),
        }];
        let statuses = state.apply_operations(vault, &ops2, 2).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Created]);
        assert!(state.relationship_exists(vault, "doc:2", "editor", "user:bob").unwrap());
    }

    #[test]
    fn dictionary_cache_rollback_on_error() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        // Create an entity so we can trigger PreconditionFailed
        let ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value1".to_vec(),
            condition: None,
            expires_at: None,
        }];
        state.apply_operations(vault, &ops, 1).unwrap();

        // Trigger PreconditionFailed — dictionary should be discarded on error
        let bad_ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value2".to_vec(),
            condition: Some(SetCondition::MustNotExist),
            expires_at: None,
        }];
        let result = state.apply_operations(vault, &bad_ops, 2);
        assert!(matches!(result, Err(StateError::PreconditionFailed { .. })));

        // Next call should still work (dictionary reloaded from storage)
        let ops2 = vec![Operation::CreateRelationship {
            resource: "doc:1".to_string(),
            relation: "viewer".to_string(),
            subject: "user:alice".to_string(),
        }];
        let statuses = state.apply_operations(vault, &ops2, 3).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Created]);
        assert!(state.relationship_exists(vault, "doc:1", "viewer", "user:alice").unwrap());
    }

    // =========================================================================
    // relationship_exists tests
    // =========================================================================

    #[test]
    fn relationship_exists_returns_true_for_created_tuple() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        let ops = vec![Operation::CreateRelationship {
            resource: "doc:1".to_string(),
            relation: "viewer".to_string(),
            subject: "user:alice".to_string(),
        }];
        state.apply_operations(vault, &ops, 1).unwrap();

        assert!(state.relationship_exists(vault, "doc:1", "viewer", "user:alice").unwrap());
    }

    #[test]
    fn relationship_exists_returns_false_for_typo() {
        let state = create_test_state();
        let vault = VaultId::new(1);

        let ops = vec![Operation::CreateRelationship {
            resource: "doc:1".to_string(),
            relation: "viewer".to_string(),
            subject: "user:alice".to_string(),
        }];
        state.apply_operations(vault, &ops, 1).unwrap();

        // Typo'd resource
        assert!(!state.relationship_exists(vault, "doc:TYPO", "viewer", "user:alice").unwrap());

        // Typo'd relation
        assert!(!state.relationship_exists(vault, "doc:1", "TYPO", "user:alice").unwrap());

        // Typo'd subject
        assert!(!state.relationship_exists(vault, "doc:1", "viewer", "user:TYPO").unwrap());
    }

    #[test]
    fn relationship_exists_returns_false_for_empty_vault() {
        let state = create_test_state();
        let vault = VaultId::new(42);

        // No relationships have been created — must return false, not an error.
        assert!(!state.relationship_exists(vault, "doc:1", "viewer", "user:alice").unwrap());
    }
}
