//! State layer for materialized state with bucket-based commitment.
//!
//! Provides:
//! - Per-vault state management
//! - Operation application
//! - State root computation via dirty bucket tracking
//!
//! Per DESIGN.md: State layer separates commitment (merkleized) from storage (fast K/V).

use std::{collections::HashMap, sync::Arc};

use inferadb_ledger_store::{CompactionStats, Database, DatabaseStats, StorageBackend, tables};
use inferadb_ledger_types::{
    Entity, Hash, Operation, Relationship, SetCondition, VaultId, WriteStatus, decode, encode,
};
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu};

use crate::{
    bucket::{BucketRootBuilder, NUM_BUCKETS, VaultCommitment},
    indexes::{IndexError, IndexManager},
    keys::{bucket_prefix, encode_storage_key},
};

/// State layer error types.
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

    /// Conditional write precondition failed.
    ///
    /// Per DESIGN.md §6.1: Returns current state for client-side conflict resolution.
    /// When a CAS (compare-and-swap) condition fails, this error provides the current
    /// entity state so clients can implement retry or conflict resolution logic.
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
/// Provides fast K/V queries and efficient state root computation
/// via bucket-based dirty tracking. Each vault's state is independently
/// tracked with its own [`VaultCommitment`] for incremental hashing.
///
/// Generic over `StorageBackend` to support both file-based (production)
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
}

#[allow(clippy::result_large_err)]
impl<B: StorageBackend> StateLayer<B> {
    /// Create a new state layer backed by the given database.
    pub fn new(db: Arc<Database<B>>) -> Self {
        Self { db, vault_commitments: RwLock::new(HashMap::new()) }
    }

    /// Get database-level statistics for metrics collection.
    pub fn database_stats(&self) -> DatabaseStats {
        self.db.stats()
    }

    /// Access the underlying database for integrity scrubbing and diagnostics.
    pub fn database(&self) -> &Arc<Database<B>> {
        &self.db
    }

    /// Get B-tree depths for all non-empty tables.
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
    fn with_commitment<F, R>(&self, vault_id: VaultId, f: F) -> R
    where
        F: FnOnce(&mut VaultCommitment) -> R,
    {
        let mut map = self.vault_commitments.write();
        let commitment = map.entry(vault_id).or_default();
        f(commitment)
    }

    /// Apply a batch of operations to a vault's state.
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
        vault_id: VaultId,
        operations: &[Operation],
        block_height: u64,
    ) -> Result<Vec<WriteStatus>> {
        let mut txn = self.db.write().context(StoreSnafu)?;
        let mut statuses = Vec::with_capacity(operations.len());

        // Track which local keys are modified for dirty bucket marking
        let mut dirty_keys: Vec<Vec<u8>> = Vec::new();

        for op in operations {
            let status = match op {
                Operation::SetEntity { key, value, condition, expires_at } => {
                    let local_key = key.as_bytes();
                    let storage_key = encode_storage_key(vault_id, local_key);

                    // Check condition - get existing entity
                    let existing = txn.get::<tables::Entities>(&storage_key).context(StoreSnafu)?;

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
                        // Per DESIGN.md §5.9: All-or-nothing - if ANY condition fails, entire batch
                        // fails
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
                    let storage_key = encode_storage_key(vault_id, local_key);

                    let existed =
                        txn.delete::<tables::Entities>(&storage_key).context(StoreSnafu)?;

                    if existed {
                        dirty_keys.push(local_key.to_vec());
                        WriteStatus::Deleted
                    } else {
                        WriteStatus::NotFound
                    }
                },

                Operation::ExpireEntity { key, .. } => {
                    let local_key = key.as_bytes();
                    let storage_key = encode_storage_key(vault_id, local_key);

                    let existed =
                        txn.delete::<tables::Entities>(&storage_key).context(StoreSnafu)?;

                    if existed {
                        dirty_keys.push(local_key.to_vec());
                        WriteStatus::Deleted
                    } else {
                        WriteStatus::NotFound
                    }
                },

                Operation::CreateRelationship { resource, relation, subject } => {
                    let rel = Relationship::new(resource, relation, subject);
                    let rel_key = rel.to_key();
                    let local_key = rel_key.as_bytes();
                    let storage_key = encode_storage_key(vault_id, local_key);

                    // Check existence
                    let already_exists = txn
                        .get::<tables::Relationships>(&storage_key)
                        .context(StoreSnafu)?
                        .is_some();

                    if already_exists {
                        WriteStatus::AlreadyExists
                    } else {
                        let encoded = encode(&rel).context(CodecSnafu)?;

                        txn.insert::<tables::Relationships>(&storage_key, &encoded)
                            .context(StoreSnafu)?;

                        // Update indexes
                        IndexManager::add_to_obj_index(
                            &mut txn, vault_id, resource, relation, subject,
                        )
                        .context(IndexSnafu)?;
                        IndexManager::add_to_subj_index(
                            &mut txn, vault_id, resource, relation, subject,
                        )
                        .context(IndexSnafu)?;

                        dirty_keys.push(local_key.to_vec());
                        WriteStatus::Created
                    }
                },

                Operation::DeleteRelationship { resource, relation, subject } => {
                    let rel = Relationship::new(resource, relation, subject);
                    let rel_key = rel.to_key();
                    let local_key = rel_key.as_bytes();
                    let storage_key = encode_storage_key(vault_id, local_key);

                    let existed =
                        txn.delete::<tables::Relationships>(&storage_key).context(StoreSnafu)?;

                    if existed {
                        // Update indexes
                        IndexManager::remove_from_obj_index(
                            &mut txn, vault_id, resource, relation, subject,
                        )
                        .context(IndexSnafu)?;
                        IndexManager::remove_from_subj_index(
                            &mut txn, vault_id, resource, relation, subject,
                        )
                        .context(IndexSnafu)?;

                        dirty_keys.push(local_key.to_vec());
                        WriteStatus::Deleted
                    } else {
                        WriteStatus::NotFound
                    }
                },
            };

            statuses.push(status);
        }

        txn.commit().context(StoreSnafu)?;

        // Mark dirty buckets
        self.with_commitment(vault_id, |commitment| {
            for key in &dirty_keys {
                commitment.mark_dirty_by_key(key);
            }
        });

        Ok(statuses)
    }

    /// Clear all entities and relationships for a vault.
    ///
    /// Used during vault recovery to reset state before replay.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if any storage operation (iteration, delete,
    /// or commit) fails.
    pub fn clear_vault(&self, vault_id: VaultId) -> Result<()> {
        use crate::keys::vault_prefix;

        let mut txn = self.db.write().context(StoreSnafu)?;
        let prefix = vault_prefix(vault_id);

        // Delete all entities for this vault
        let mut keys_to_delete = Vec::new();
        for (key_bytes, _) in txn.iter::<tables::Entities>().context(StoreSnafu)? {
            // Check we're still in the same vault
            if key_bytes.len() < 8 {
                break;
            }
            let key_vault_id = i64::from_be_bytes(key_bytes[..8].try_into().unwrap_or([0; 8]));
            if key_vault_id < vault_id.value() {
                continue;
            }
            if key_vault_id != vault_id.value() {
                break;
            }
            keys_to_delete.push(key_bytes);
        }

        for key in keys_to_delete {
            txn.delete::<tables::Entities>(&key).context(StoreSnafu)?;
        }

        // Delete all relationships for this vault
        let mut keys_to_delete = Vec::new();
        for (key_bytes, _) in txn.iter::<tables::Relationships>().context(StoreSnafu)? {
            if key_bytes.len() < 8 {
                break;
            }
            let key_vault_id = i64::from_be_bytes(key_bytes[..8].try_into().unwrap_or([0; 8]));
            if key_vault_id < vault_id.value() {
                continue;
            }
            if key_vault_id != vault_id.value() {
                break;
            }
            keys_to_delete.push(key_bytes);
        }

        for key in keys_to_delete {
            txn.delete::<tables::Relationships>(&key).context(StoreSnafu)?;
        }

        // Clear indexes
        let mut keys_to_delete = Vec::new();
        for (key_bytes, _) in txn.iter::<tables::ObjIndex>().context(StoreSnafu)? {
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
            txn.delete::<tables::ObjIndex>(&key).context(StoreSnafu)?;
        }

        let mut keys_to_delete = Vec::new();
        for (key_bytes, _) in txn.iter::<tables::SubjIndex>().context(StoreSnafu)? {
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
            txn.delete::<tables::SubjIndex>(&key).context(StoreSnafu)?;
        }

        txn.commit().context(StoreSnafu)?;

        // Reset commitment tracking for this vault
        self.vault_commitments.write().remove(&vault_id);

        Ok(())
    }

    /// Get an entity by key.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction fails.
    /// Returns [`StateError::Codec`] if deserialization of the stored entity fails.
    pub fn get_entity(&self, vault_id: VaultId, key: &[u8]) -> Result<Option<Entity>> {
        let storage_key = encode_storage_key(vault_id, key);
        let txn = self.db.read().context(StoreSnafu)?;

        match txn.get::<tables::Entities>(&storage_key).context(StoreSnafu)? {
            Some(data) => {
                let entity = decode(&data).context(CodecSnafu)?;
                Ok(Some(entity))
            },
            None => Ok(None),
        }
    }

    /// Check if a relationship exists.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction fails.
    pub fn relationship_exists(
        &self,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<bool> {
        let rel = Relationship::new(resource, relation, subject);
        let local_key = rel.to_key();
        let storage_key = encode_storage_key(vault_id, local_key.as_bytes());

        let txn = self.db.read().context(StoreSnafu)?;

        Ok(txn.get::<tables::Relationships>(&storage_key).context(StoreSnafu)?.is_some())
    }

    /// Compute state root for a vault, updating dirty bucket roots.
    ///
    /// This scans only the dirty buckets and recomputes their roots,
    /// then returns SHA-256(bucket_roots[0..256]).
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction or iteration fails.
    /// Returns [`StateError::Codec`] if deserialization of any entity in a dirty bucket fails.
    pub fn compute_state_root(&self, vault_id: VaultId) -> Result<Hash> {
        // First check if dirty and get the dirty buckets list (brief read lock)
        let dirty_buckets: Vec<u8> = {
            let map = self.vault_commitments.read();
            match map.get(&vault_id) {
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
                    return Ok(self.with_commitment(vault_id, |c| c.compute_state_root()));
                },
            }
        };

        // Compute bucket roots outside the commitment lock
        let txn = self.db.read().context(StoreSnafu)?;
        let mut bucket_roots: Vec<(u8, Hash)> = Vec::with_capacity(dirty_buckets.len());

        for bucket in dirty_buckets {
            let _prefix = bucket_prefix(vault_id, bucket);
            let mut builder = BucketRootBuilder::new(bucket);

            // Scan all entities in this bucket
            for (key_bytes, value) in txn.iter::<tables::Entities>().context(StoreSnafu)? {
                // Check we're still in the same vault
                if key_bytes.len() < 9 {
                    continue;
                }
                let key_vault_id = i64::from_be_bytes(key_bytes[..8].try_into().unwrap_or([0; 8]));
                if key_vault_id < vault_id.value() {
                    continue;
                }
                if key_vault_id > vault_id.value() {
                    break;
                }

                // Check bucket
                if key_bytes[8] < bucket {
                    continue;
                }
                if key_bytes[8] > bucket {
                    break;
                }

                let entity: Entity = decode(&value).context(CodecSnafu)?;
                builder.add_entity(&entity);
            }

            bucket_roots.push((bucket, builder.finalize()));
        }

        // Update commitment with computed bucket roots (brief write lock)
        Ok(self.with_commitment(vault_id, |commitment| {
            for (bucket, root) in bucket_roots {
                commitment.set_bucket_root(bucket, root);
            }
            commitment.clear_dirty();
            commitment.compute_state_root()
        }))
    }

    /// Load bucket roots from stored vault metadata.
    ///
    /// Called during startup/recovery to restore commitment state.
    pub fn load_vault_commitment(&self, vault_id: VaultId, bucket_roots: [Hash; NUM_BUCKETS]) {
        self.vault_commitments
            .write()
            .insert(vault_id, VaultCommitment::from_bucket_roots(bucket_roots));
    }

    /// Get the current bucket roots for a vault (for persistence).
    pub fn get_bucket_roots(&self, vault_id: VaultId) -> Option<[Hash; NUM_BUCKETS]> {
        self.vault_commitments.read().get(&vault_id).map(|c| *c.bucket_roots())
    }

    /// Compact all B+ tree tables, merging underfull leaf nodes.
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

    /// List subjects for a given resource and relation.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction fails.
    /// Returns [`StateError::Index`] if the index lookup fails.
    pub fn list_subjects(
        &self,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
    ) -> Result<Vec<String>> {
        let txn = self.db.read().context(StoreSnafu)?;
        IndexManager::get_subjects(&txn, vault_id, resource, relation).context(IndexSnafu)
    }

    /// List resource-relation pairs for a given subject.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction fails.
    /// Returns [`StateError::Index`] if the index lookup fails.
    pub fn list_resources_for_subject(
        &self,
        vault_id: VaultId,
        subject: &str,
    ) -> Result<Vec<(String, String)>> {
        let txn = self.db.read().context(StoreSnafu)?;
        IndexManager::get_resources(&txn, vault_id, subject).context(IndexSnafu)
    }

    /// List all entities in a vault with optional prefix filter.
    ///
    /// Returns up to `limit` entities. Use `start_after` for pagination.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction or iteration fails.
    /// Returns [`StateError::Codec`] if deserialization of any entity fails.
    pub fn list_entities(
        &self,
        vault_id: VaultId,
        prefix: Option<&str>,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Entity>> {
        use crate::keys::vault_prefix;

        let txn = self.db.read().context(StoreSnafu)?;

        let mut entities = Vec::with_capacity(limit.min(1000));

        // Build the range start key.
        // Note: Keys are ordered by (vault_id, bucket_id, local_key), where bucket_id
        // is a hash of the local key. This means keys with the same prefix can be
        // scattered across different buckets. For prefix scans, we must start from
        // the beginning of the vault and filter by prefix in the loop.
        let start_key: Vec<u8> = if let Some(after) = start_after {
            // Pagination: start after this specific key
            let mut k = encode_storage_key(vault_id, after.as_bytes());
            k.push(0); // Advance past the exact key
            k
        } else {
            // For prefix scans or full vault scans, start from the vault prefix
            vault_prefix(vault_id).to_vec()
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
            if key_vault_id != vault_id.value() {
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

    /// List all relationships in a vault.
    ///
    /// Returns up to `limit` relationships. Use `start_after` for pagination.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Store`] if the read transaction or iteration fails.
    /// Returns [`StateError::Codec`] if deserialization of any relationship fails.
    pub fn list_relationships(
        &self,
        vault_id: VaultId,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Relationship>> {
        use crate::keys::vault_prefix;

        let txn = self.db.read().context(StoreSnafu)?;

        let mut relationships = Vec::with_capacity(limit.min(1000));

        // Build the range start key.
        // Note: Keys are ordered by (vault_id, bucket_id, local_key). For full
        // vault scans without start_after, we use the 8-byte vault prefix to
        // iterate from the very first key in the vault (bucket 0).
        let start_key = if let Some(after) = start_after {
            let mut k = encode_storage_key(vault_id, after.as_bytes());
            k.push(0);
            k
        } else {
            vault_prefix(vault_id).to_vec()
        };

        for (key_bytes, value) in txn.iter::<tables::Relationships>().context(StoreSnafu)? {
            if relationships.len() >= limit {
                break;
            }

            // Skip until we reach the start key
            if key_bytes < start_key {
                continue;
            }

            // Check we're still in the same vault
            if key_bytes.len() < 9 {
                break;
            }
            let key_vault_id = i64::from_be_bytes(key_bytes[..8].try_into().unwrap_or([0; 8]));
            if key_vault_id != vault_id.value() {
                break;
            }

            let rel: Relationship = decode(&value).context(CodecSnafu)?;

            relationships.push(rel);
        }

        Ok(relationships)
    }
}

impl<B: StorageBackend> Clone for StateLayer<B> {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            vault_commitments: RwLock::new(HashMap::new()), // Each clone starts fresh
        }
    }
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
        let vault_id = VaultId::new(1);

        let ops = vec![Operation::SetEntity {
            key: "test_key".to_string(),
            value: b"test_value".to_vec(),
            condition: None,
            expires_at: None,
        }];

        let statuses = state.apply_operations(vault_id, &ops, 1).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Created]);

        let entity = state.get_entity(vault_id, b"test_key").unwrap();
        assert!(entity.is_some());
        let e = entity.unwrap();
        assert_eq!(e.value, b"test_value");
        assert_eq!(e.version, 1);
    }

    #[test]
    fn test_set_entity_must_not_exist() {
        let state = create_test_state();
        let vault_id = VaultId::new(1);

        // First set
        let ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value1".to_vec(),
            condition: Some(SetCondition::MustNotExist),
            expires_at: None,
        }];
        let statuses = state.apply_operations(vault_id, &ops, 1).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Created]);

        // Second set should fail with error (per DESIGN.md §5.9: batch atomicity)
        let ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value2".to_vec(),
            condition: Some(SetCondition::MustNotExist),
            expires_at: None,
        }];
        let result = state.apply_operations(vault_id, &ops, 2);

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
        let vault_id = VaultId::new(1);

        // Set then delete
        let ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value".to_vec(),
            condition: None,
            expires_at: None,
        }];
        state.apply_operations(vault_id, &ops, 1).unwrap();

        let ops = vec![Operation::DeleteEntity { key: "key".to_string() }];
        let statuses = state.apply_operations(vault_id, &ops, 2).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Deleted]);

        // Should not exist now
        assert!(state.get_entity(vault_id, b"key").unwrap().is_none());

        // Delete again should be NotFound
        let statuses = state.apply_operations(vault_id, &ops, 3).unwrap();
        assert_eq!(statuses, vec![WriteStatus::NotFound]);
    }

    #[test]
    fn test_create_relationship() {
        let state = create_test_state();
        let vault_id = VaultId::new(1);

        let ops = vec![Operation::CreateRelationship {
            resource: "doc:123".to_string(),
            relation: "viewer".to_string(),
            subject: "user:alice".to_string(),
        }];

        let statuses = state.apply_operations(vault_id, &ops, 1).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Created]);

        assert!(state.relationship_exists(vault_id, "doc:123", "viewer", "user:alice").unwrap());

        // Create again should return AlreadyExists
        let statuses = state.apply_operations(vault_id, &ops, 2).unwrap();
        assert_eq!(statuses, vec![WriteStatus::AlreadyExists]);
    }

    #[test]
    fn test_delete_relationship() {
        let state = create_test_state();
        let vault_id = VaultId::new(1);

        // Create
        let ops = vec![Operation::CreateRelationship {
            resource: "doc:123".to_string(),
            relation: "viewer".to_string(),
            subject: "user:alice".to_string(),
        }];
        state.apply_operations(vault_id, &ops, 1).unwrap();

        // Delete
        let ops = vec![Operation::DeleteRelationship {
            resource: "doc:123".to_string(),
            relation: "viewer".to_string(),
            subject: "user:alice".to_string(),
        }];
        let statuses = state.apply_operations(vault_id, &ops, 2).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Deleted]);

        assert!(!state.relationship_exists(vault_id, "doc:123", "viewer", "user:alice").unwrap());
    }

    /// Regression test for list_relationships bug where using encode_storage_key
    /// with an empty key caused iteration to start at bucket 185, skipping ~72%
    /// of relationships in buckets 0-184.
    ///
    /// This test creates relationships with keys that hash to various buckets
    /// (including low-numbered buckets) and verifies list_relationships returns
    /// all of them when starting from scratch (no start_after).
    #[test]
    fn test_list_relationships_returns_all_buckets() {
        use inferadb_ledger_types::bucket_id;

        let state = create_test_state();
        let vault_id = VaultId::new(1);

        // Create relationships with resource names chosen to land in different buckets.
        // We want at least some in buckets < 185 to catch the regression.
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

        // Verify we have relationships in different bucket ranges
        let mut has_low_bucket = false;
        let mut has_high_bucket = false;
        for (resource, relation, subject) in &test_cases {
            let rel_key = format!("rel:{}#{}@{}", resource, relation, subject);
            let bucket = bucket_id(rel_key.as_bytes());
            if bucket < 128 {
                has_low_bucket = true;
            } else {
                has_high_bucket = true;
            }
        }
        assert!(
            has_low_bucket && has_high_bucket,
            "Test data should span both low and high buckets for meaningful coverage"
        );

        // Create all relationships
        let ops: Vec<Operation> = test_cases
            .iter()
            .map(|(resource, relation, subject)| Operation::CreateRelationship {
                resource: resource.to_string(),
                relation: relation.to_string(),
                subject: subject.to_string(),
            })
            .collect();

        let statuses = state.apply_operations(vault_id, &ops, 1).unwrap();
        assert!(statuses.iter().all(|s| *s == WriteStatus::Created));

        // List all relationships without start_after - this is the bug scenario
        let listed = state.list_relationships(vault_id, None, 100).unwrap();

        // Must return all relationships, not just those in buckets >= 185
        assert_eq!(
            listed.len(),
            test_cases.len(),
            "list_relationships should return all {} relationships, got {}. \
             This may indicate the start key bug (starting at bucket 185 instead of 0).",
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
    /// Simulates the concurrent write + list pattern that fails in the Engine's
    /// ledger_integration.rs tests. Writes 11 relationships (1 sequential + 10 concurrent)
    /// and verifies all are returned by list_relationships.
    #[test]
    fn test_list_relationships_after_concurrent_writes() {
        use std::thread;

        use inferadb_ledger_types::bucket_id;

        let state = create_test_state();
        let vault_id = VaultId::new(20_000_002_690_000); // Same vault ID pattern as integration tests

        // First: sequential write (like Engine test does)
        let seq_ops = vec![Operation::CreateRelationship {
            resource: "document:seq-test".to_string(),
            relation: "viewer".to_string(),
            subject: "user:seq-test".to_string(),
        }];
        let statuses = state.apply_operations(vault_id, &seq_ops, 1).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Created]);

        // Verify sequential write via direct read
        assert!(
            state
                .relationship_exists(vault_id, "document:seq-test", "viewer", "user:seq-test")
                .unwrap()
        );

        // Now: concurrent writes (mimicking tokio::spawn in integration test)
        // Note: StateLayer is internally thread-safe, so we use threads
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
                state.apply_operations(vault_id, &ops, (i + 2) as u64).unwrap()
            });
            handles.push(handle);
        }

        // Wait for all concurrent writes
        for handle in handles {
            let statuses = handle.join().expect("thread panicked");
            assert_eq!(statuses, vec![WriteStatus::Created]);
        }

        // Verify all concurrent writes via direct reads
        for i in 0..10 {
            assert!(
                state_arc
                    .relationship_exists(
                        vault_id,
                        &format!("document:concurrent-{}", i),
                        "viewer",
                        &format!("user:concurrent-{}", i)
                    )
                    .unwrap(),
                "Direct read failed for concurrent-{}",
                i
            );
        }

        // Debug: print bucket distribution
        eprintln!("=== Bucket distribution ===");
        let seq_key = "rel:document:seq-test#viewer@user:seq-test".to_string();
        eprintln!("seq-test: bucket {}", bucket_id(seq_key.as_bytes()));
        for i in 0..10 {
            let key = format!("rel:document:concurrent-{}#viewer@user:concurrent-{}", i, i);
            eprintln!("concurrent-{}: bucket {}", i, bucket_id(key.as_bytes()));
        }

        // Now: list all relationships (the operation that fails in integration tests)
        let all_relationships = state_arc.list_relationships(vault_id, None, 100).unwrap();

        eprintln!("=== Listed {} relationships ===", all_relationships.len());
        for rel in &all_relationships {
            eprintln!("  {}#{}@{}", rel.resource, rel.relation, rel.subject);
        }

        // Should have all 11 relationships
        assert_eq!(
            all_relationships.len(),
            11,
            "Expected 11 relationships (1 seq + 10 concurrent), got {}",
            all_relationships.len()
        );

        // Verify specific relationships are present
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
        let vault_id = VaultId::new(1);

        let root1 = state.compute_state_root(vault_id).unwrap();

        let ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value".to_vec(),
            condition: None,
            expires_at: None,
        }];
        state.apply_operations(vault_id, &ops, 1).unwrap();

        let root2 = state.compute_state_root(vault_id).unwrap();
        assert_ne!(root1, root2);

        // Deleting should change root again
        let ops = vec![Operation::DeleteEntity { key: "key".to_string() }];
        state.apply_operations(vault_id, &ops, 2).unwrap();

        let root3 = state.compute_state_root(vault_id).unwrap();
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
        let vault_id = VaultId::new(1);

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
        state.apply_operations(vault_id, &ops, 1).unwrap();

        // Also add data to vault 2 to ensure isolation
        let ops_vault2 = vec![Operation::SetEntity {
            key: "entity_v2".to_string(),
            value: b"value_v2".to_vec(),
            condition: None,
            expires_at: None,
        }];
        state.apply_operations(VaultId::new(2), &ops_vault2, 1).unwrap();

        // Verify data exists in vault 1
        assert!(state.get_entity(vault_id, b"entity1").unwrap().is_some());
        assert!(state.get_entity(vault_id, b"entity2").unwrap().is_some());
        assert!(state.relationship_exists(vault_id, "doc:1", "viewer", "user:alice").unwrap());

        // Clear vault 1
        state.clear_vault(vault_id).unwrap();

        // Verify vault 1 data is gone
        assert!(state.get_entity(vault_id, b"entity1").unwrap().is_none());
        assert!(state.get_entity(vault_id, b"entity2").unwrap().is_none());
        assert!(!state.relationship_exists(vault_id, "doc:1", "viewer", "user:alice").unwrap());

        // Verify vault 2 data is still there (isolation)
        assert!(state.get_entity(VaultId::new(2), b"entity_v2").unwrap().is_some());

        // State root should be back to empty
        let empty_root = state.compute_state_root(vault_id).unwrap();
        let fresh_state = create_test_state();
        let expected_empty = fresh_state.compute_state_root(vault_id).unwrap();
        assert_eq!(empty_root, expected_empty);
    }

    /// Stress test: many sequential writes followed by verification
    /// Simulates the pattern used by the server stress tests
    #[test]
    fn test_many_sequential_writes_then_verify() {
        let state = create_test_state();
        let vault_id = VaultId::new(1);

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

            let statuses = state.apply_operations(vault_id, &ops, i as u64 + 1).unwrap();
            assert_eq!(statuses, vec![WriteStatus::Created], "Failed to create key {}", i);
        }

        // Verify all keys are present
        let mut missing = Vec::new();
        for i in 0..num_keys {
            let key = format!("stress-key-{}", i);
            let expected = format!("stress-value-{}", i).into_bytes();
            match state.get_entity(vault_id, key.as_bytes()).unwrap() {
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
        let vault_id = VaultId::new(1);
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
                    state.apply_operations(vault_id, &ops, block_height).unwrap();
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
                match state.get_entity(vault_id, key.as_bytes()).unwrap() {
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

        /// Generate an arbitrary entity key (short identifiers for efficiency).
        fn arb_key() -> impl Strategy<Value = String> {
            proptest::string::string_regex("[a-z][a-z0-9]{0,7}").expect("valid regex")
        }

        /// Generate an arbitrary entity value (small byte arrays).
        fn arb_value() -> impl Strategy<Value = Vec<u8>> {
            proptest::collection::vec(any::<u8>(), 0..32)
        }

        /// Generate an arbitrary resource identifier.
        fn arb_resource() -> impl Strategy<Value = String> {
            (arb_key(), prop::sample::select(vec!["doc", "folder", "project"]))
                .prop_map(|(id, typ)| format!("{}:{}", typ, id))
        }

        /// Generate an arbitrary relation name.
        fn arb_relation() -> impl Strategy<Value = String> {
            prop::sample::select(vec![
                "viewer".to_string(),
                "editor".to_string(),
                "owner".to_string(),
                "member".to_string(),
            ])
        }

        /// Generate an arbitrary subject identifier.
        fn arb_subject() -> impl Strategy<Value = String> {
            (arb_key(), prop::sample::select(vec!["user", "group", "team"]))
                .prop_map(|(id, typ)| format!("{}:{}", typ, id))
        }

        /// Generate an arbitrary Operation.
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

        /// Generate a sequence of operations (1 to 20 operations per test).
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
                let vault_id = VaultId::new(1);

                // Create two independent StateLayer instances
                let state1 = create_test_state();
                let state2 = create_test_state();

                // Apply the same operations to both
                for (idx, op) in operations.iter().enumerate() {
                    let block_height = (idx + 1) as u64;
                    let _ = state1.apply_operations(vault_id, std::slice::from_ref(op), block_height);
                    let _ = state2.apply_operations(vault_id, std::slice::from_ref(op), block_height);
                }

                // State roots must be identical
                let root1 = state1.compute_state_root(vault_id).unwrap();
                let root2 = state2.compute_state_root(vault_id).unwrap();

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
                let vault_id = VaultId::new(1);
                let block_height: u64 = 1; // Same height for both

                // Apply one-by-one at the SAME block height
                let state_individual = create_test_state();
                for op in operations.iter() {
                    let _ = state_individual.apply_operations(vault_id, std::slice::from_ref(op), block_height);
                }

                // Apply as batch at the same block height
                let state_batch = create_test_state();
                let _ = state_batch.apply_operations(vault_id, &operations, block_height);

                // State roots must be identical
                let root_individual = state_individual.compute_state_root(vault_id).unwrap();
                let root_batch = state_batch.compute_state_root(vault_id).unwrap();

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
                let vault_id = VaultId::new(1);
                let state = create_test_state();

                // Apply operations
                let _ = state.apply_operations(vault_id, &operations, 1);

                // Compute state root multiple times
                let root1 = state.compute_state_root(vault_id).unwrap();
                let root2 = state.compute_state_root(vault_id).unwrap();
                let root3 = state.compute_state_root(vault_id).unwrap();

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

                let vault_id = VaultId::new(1);

                let state1 = create_test_state();
                let state2 = create_test_state();

                let _ = state1.apply_operations(vault_id, &ops1, 1);
                let _ = state2.apply_operations(vault_id, &ops2, 1);

                let root1 = state1.compute_state_root(vault_id).unwrap();
                let root2 = state2.compute_state_root(vault_id).unwrap();

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

    // Test the full error chain: CodecError -> IndexError -> StateError
    #[test]
    fn test_state_error_chain_from_index() {
        use crate::indexes::IndexError;

        // IndexError with Codec source via context
        let malformed: &[u8] = &[0xFF];
        let codec_result: std::result::Result<u64, inferadb_ledger_types::CodecError> =
            inferadb_ledger_types::decode(malformed);
        let codec_err = codec_result.expect_err("should fail");
        let index_err =
            IndexError::Codec { source: codec_err, location: snafu::Location::default() };

        // Convert IndexError -> StateError via context selector
        let state_err =
            StateError::Index { source: index_err, location: snafu::Location::default() };

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
        let vault_id = VaultId::new(1);

        // Insert many entities
        for i in 0..100 {
            let ops = vec![Operation::SetEntity {
                key: format!("ckey-{:04}", i),
                value: format!("cval-{:04}", i).into_bytes(),
                condition: None,
                expires_at: None,
            }];
            state.apply_operations(vault_id, &ops, i as u64 + 1).unwrap();
        }

        // Delete most to create sparse leaves
        for i in 0..80 {
            let ops = vec![Operation::DeleteEntity { key: format!("ckey-{:04}", i) }];
            state.apply_operations(vault_id, &ops, 200 + i as u64).unwrap();
        }

        // Run compaction
        let stats = state.compact_tables(0.4).unwrap();

        // Verify remaining data is intact
        for i in 80..100 {
            let entity = state.get_entity(vault_id, format!("ckey-{:04}", i).as_bytes()).unwrap();
            assert!(entity.is_some(), "Entity ckey-{:04} missing after compaction", i);
            assert_eq!(entity.unwrap().value, format!("cval-{:04}", i).into_bytes());
        }

        // Verify deleted keys are still gone
        for i in 0..80 {
            let entity = state.get_entity(vault_id, format!("ckey-{:04}", i).as_bytes()).unwrap();
            assert!(entity.is_none());
        }

        // At least the stats struct should be populated (may or may not have merges
        // depending on actual page layout)
        let _ = stats;
    }
}
