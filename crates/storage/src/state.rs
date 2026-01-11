//! State layer for materialized state with bucket-based commitment.
//!
//! Provides:
//! - Per-vault state management
//! - Operation application
//! - State root computation via dirty bucket tracking
//!
//! Per DESIGN.md: State layer separates commitment (merkleized) from storage (fast K/V).

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use redb::{Database, ReadableTable};
use snafu::{ResultExt, Snafu};

use ledger_types::{
    EMPTY_HASH, Entity, Hash, Operation, Relationship, SetCondition, VaultId, WriteStatus,
    bucket_id,
};

use crate::bucket::{BucketRootBuilder, NUM_BUCKETS, VaultCommitment};
use crate::entity::EntityStore;
use crate::indexes::IndexManager;
use crate::keys::{bucket_prefix, encode_storage_key};
use crate::relationship::RelationshipStore;
use crate::tables::Tables;

/// State layer error types.
#[derive(Debug, Snafu)]
pub enum StateError {
    #[snafu(display("Storage error: {source}"))]
    Storage { source: redb::StorageError },

    #[snafu(display("Table error: {source}"))]
    Table { source: redb::TableError },

    #[snafu(display("Transaction error: {source}"))]
    Transaction { source: redb::TransactionError },

    #[snafu(display("Commit error: {source}"))]
    Commit { source: redb::CommitError },

    #[snafu(display("Serialization error: {message}"))]
    Serialization { message: String },

    #[snafu(display("Precondition failed for key: {key}"))]
    PreconditionFailed { key: String },
}

/// Result type for state operations.
pub type Result<T> = std::result::Result<T, StateError>;

/// State layer managing per-vault materialized state.
///
/// Provides fast K/V queries and efficient state root computation
/// via bucket-based dirty tracking.
pub struct StateLayer {
    /// Shared database handle.
    db: Arc<Database>,
    /// Per-vault commitment tracking.
    vault_commitments: DashMap<VaultId, VaultCommitment>,
}

impl StateLayer {
    /// Create a new state layer backed by the given database.
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            db,
            vault_commitments: DashMap::new(),
        }
    }

    /// Get or create commitment tracking for a vault.
    fn get_or_create_commitment(
        &self,
        vault_id: VaultId,
    ) -> dashmap::mapref::one::RefMut<'_, VaultId, VaultCommitment> {
        self.vault_commitments
            .entry(vault_id)
            .or_insert_with(VaultCommitment::new)
    }

    /// Apply a batch of operations to a vault's state.
    ///
    /// Returns the status for each operation.
    pub fn apply_operations(
        &self,
        vault_id: VaultId,
        operations: &[Operation],
        block_height: u64,
    ) -> Result<Vec<WriteStatus>> {
        let txn = self.db.begin_write().context(TransactionSnafu)?;
        let mut statuses = Vec::with_capacity(operations.len());

        // Track which local keys are modified for dirty bucket marking
        let mut dirty_keys: Vec<Vec<u8>> = Vec::new();

        {
            let mut entities = txn.open_table(Tables::ENTITIES).context(TableSnafu)?;
            let mut relationships = txn.open_table(Tables::RELATIONSHIPS).context(TableSnafu)?;
            let mut obj_index = txn.open_table(Tables::OBJ_INDEX).context(TableSnafu)?;
            let mut subj_index = txn.open_table(Tables::SUBJ_INDEX).context(TableSnafu)?;

            for op in operations {
                let status = match op {
                    Operation::SetEntity {
                        key,
                        value,
                        condition,
                        expires_at,
                    } => {
                        let local_key = key.as_bytes();
                        let storage_key = encode_storage_key(vault_id, local_key);

                        // Check condition - extract data and drop borrow before mutation
                        let (condition_met, is_update) = {
                            let existing = entities.get(&storage_key[..]).context(StorageSnafu)?;
                            let is_update = existing.is_some();

                            let condition_met = match condition {
                                None => true,
                                Some(SetCondition::MustNotExist) => existing.is_none(),
                                Some(SetCondition::MustExist) => existing.is_some(),
                                Some(SetCondition::VersionEquals(v)) => existing
                                    .as_ref()
                                    .and_then(|e| postcard::from_bytes::<Entity>(e.value()).ok())
                                    .map(|e| e.version == *v)
                                    .unwrap_or(false),
                                Some(SetCondition::ValueEquals(expected)) => existing
                                    .as_ref()
                                    .and_then(|e| postcard::from_bytes::<Entity>(e.value()).ok())
                                    .map(|e| e.value == *expected)
                                    .unwrap_or(false),
                            };
                            (condition_met, is_update)
                        };

                        if !condition_met {
                            WriteStatus::PreconditionFailed
                        } else {
                            let entity = Entity {
                                key: local_key.to_vec(),
                                value: value.clone(),
                                expires_at: expires_at.unwrap_or(0),
                                version: block_height,
                            };

                            let encoded = postcard::to_allocvec(&entity).map_err(|e| {
                                StateError::Serialization {
                                    message: e.to_string(),
                                }
                            })?;

                            entities
                                .insert(&storage_key[..], &encoded[..])
                                .context(StorageSnafu)?;

                            dirty_keys.push(local_key.to_vec());

                            if is_update {
                                WriteStatus::Updated
                            } else {
                                WriteStatus::Created
                            }
                        }
                    }

                    Operation::DeleteEntity { key } => {
                        let local_key = key.as_bytes();
                        let storage_key = encode_storage_key(vault_id, local_key);

                        let existed = entities.remove(&storage_key[..]).context(StorageSnafu)?;

                        if existed.is_some() {
                            dirty_keys.push(local_key.to_vec());
                            WriteStatus::Deleted
                        } else {
                            WriteStatus::NotFound
                        }
                    }

                    Operation::ExpireEntity { key, .. } => {
                        let local_key = key.as_bytes();
                        let storage_key = encode_storage_key(vault_id, local_key);

                        let existed = entities.remove(&storage_key[..]).context(StorageSnafu)?;

                        if existed.is_some() {
                            dirty_keys.push(local_key.to_vec());
                            WriteStatus::Deleted
                        } else {
                            WriteStatus::NotFound
                        }
                    }

                    Operation::CreateRelationship {
                        resource,
                        relation,
                        subject,
                    } => {
                        let rel = Relationship::new(resource, relation, subject);
                        let rel_key = rel.to_key();
                        let local_key = rel_key.as_bytes();
                        let storage_key = encode_storage_key(vault_id, local_key);

                        // Check existence - extract result and drop borrow before mutation
                        let already_exists = {
                            relationships
                                .get(&storage_key[..])
                                .context(StorageSnafu)?
                                .is_some()
                        };

                        if already_exists {
                            WriteStatus::AlreadyExists
                        } else {
                            let encoded = postcard::to_allocvec(&rel).map_err(|e| {
                                StateError::Serialization {
                                    message: e.to_string(),
                                }
                            })?;

                            relationships
                                .insert(&storage_key[..], &encoded[..])
                                .context(StorageSnafu)?;

                            // Update indexes
                            IndexManager::add_to_obj_index(
                                &mut obj_index,
                                vault_id,
                                resource,
                                relation,
                                subject,
                            )?;
                            IndexManager::add_to_subj_index(
                                &mut subj_index,
                                vault_id,
                                resource,
                                relation,
                                subject,
                            )?;

                            dirty_keys.push(local_key.to_vec());
                            WriteStatus::Created
                        }
                    }

                    Operation::DeleteRelationship {
                        resource,
                        relation,
                        subject,
                    } => {
                        let rel = Relationship::new(resource, relation, subject);
                        let rel_key = rel.to_key();
                        let local_key = rel_key.as_bytes();
                        let storage_key = encode_storage_key(vault_id, local_key);

                        let existed = relationships
                            .remove(&storage_key[..])
                            .context(StorageSnafu)?;

                        if existed.is_some() {
                            // Update indexes
                            IndexManager::remove_from_obj_index(
                                &mut obj_index,
                                vault_id,
                                resource,
                                relation,
                                subject,
                            )?;
                            IndexManager::remove_from_subj_index(
                                &mut subj_index,
                                vault_id,
                                resource,
                                relation,
                                subject,
                            )?;

                            dirty_keys.push(local_key.to_vec());
                            WriteStatus::Deleted
                        } else {
                            WriteStatus::NotFound
                        }
                    }
                };

                statuses.push(status);
            }
        }

        txn.commit().context(CommitSnafu)?;

        // Mark dirty buckets
        let mut commitment = self.get_or_create_commitment(vault_id);
        for key in dirty_keys {
            commitment.mark_dirty_by_key(&key);
        }

        Ok(statuses)
    }

    /// Get an entity by key.
    pub fn get_entity(&self, vault_id: VaultId, key: &[u8]) -> Result<Option<Entity>> {
        let storage_key = encode_storage_key(vault_id, key);
        let txn = self.db.begin_read().context(TransactionSnafu)?;
        let table = txn.open_table(Tables::ENTITIES).context(TableSnafu)?;

        match table.get(&storage_key[..]).context(StorageSnafu)? {
            Some(data) => {
                let entity =
                    postcard::from_bytes(data.value()).map_err(|e| StateError::Serialization {
                        message: e.to_string(),
                    })?;
                Ok(Some(entity))
            }
            None => Ok(None),
        }
    }

    /// Check if a relationship exists.
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

        let txn = self.db.begin_read().context(TransactionSnafu)?;
        let table = txn.open_table(Tables::RELATIONSHIPS).context(TableSnafu)?;

        Ok(table.get(&storage_key[..]).context(StorageSnafu)?.is_some())
    }

    /// Compute state root for a vault, updating dirty bucket roots.
    ///
    /// This scans only the dirty buckets and recomputes their roots,
    /// then returns SHA-256(bucket_roots[0..256]).
    pub fn compute_state_root(&self, vault_id: VaultId) -> Result<Hash> {
        let mut commitment = self.get_or_create_commitment(vault_id);

        if !commitment.is_dirty() {
            return Ok(commitment.compute_state_root());
        }

        let txn = self.db.begin_read().context(TransactionSnafu)?;
        let table = txn.open_table(Tables::ENTITIES).context(TableSnafu)?;

        // Recompute each dirty bucket
        let dirty_buckets: Vec<u8> = commitment.dirty_buckets().iter().copied().collect();

        for bucket in dirty_buckets {
            let prefix = bucket_prefix(vault_id, bucket);
            let mut builder = BucketRootBuilder::new(bucket);

            // Scan all entities in this bucket
            // Range: [prefix] to [prefix with next bucket]

            // For bucket 255, we need to handle the wrap case
            if bucket == 255 {
                // Scan from bucket 255 prefix to end of vault's keyspace
                for result in table.range(&prefix[..]..).context(StorageSnafu)? {
                    let (key, value) = result.context(StorageSnafu)?;
                    let key_bytes = key.value();

                    // Check we're still in the same vault
                    if key_bytes.len() < 9 {
                        break;
                    }
                    let key_vault_id =
                        i64::from_be_bytes(key_bytes[..8].try_into().unwrap_or([0; 8]));
                    if key_vault_id != vault_id {
                        break;
                    }
                    // Check we're still in bucket 255
                    if key_bytes[8] != 255 {
                        break;
                    }

                    let entity: Entity = postcard::from_bytes(value.value()).map_err(|e| {
                        StateError::Serialization {
                            message: e.to_string(),
                        }
                    })?;
                    builder.add_entity(&entity);
                }
            } else {
                // Normal case: scan until next bucket
                let mut range_end = prefix;
                range_end[8] = bucket + 1;

                for result in table
                    .range(&prefix[..]..&range_end[..])
                    .context(StorageSnafu)?
                {
                    let (_, value) = result.context(StorageSnafu)?;
                    let entity: Entity = postcard::from_bytes(value.value()).map_err(|e| {
                        StateError::Serialization {
                            message: e.to_string(),
                        }
                    })?;
                    builder.add_entity(&entity);
                }
            }

            let bucket_root = builder.finalize();
            commitment.set_bucket_root(bucket, bucket_root);
        }

        commitment.clear_dirty();
        Ok(commitment.compute_state_root())
    }

    /// Load bucket roots from stored vault metadata.
    ///
    /// Called during startup/recovery to restore commitment state.
    pub fn load_vault_commitment(&self, vault_id: VaultId, bucket_roots: [Hash; NUM_BUCKETS]) {
        self.vault_commitments
            .insert(vault_id, VaultCommitment::from_bucket_roots(bucket_roots));
    }

    /// Get the current bucket roots for a vault (for persistence).
    pub fn get_bucket_roots(&self, vault_id: VaultId) -> Option<[Hash; NUM_BUCKETS]> {
        self.vault_commitments
            .get(&vault_id)
            .map(|c| *c.bucket_roots())
    }

    /// List subjects for a given resource and relation.
    pub fn list_subjects(
        &self,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
    ) -> Result<Vec<String>> {
        let txn = self.db.begin_read().context(TransactionSnafu)?;
        let table = txn.open_table(Tables::OBJ_INDEX).context(TableSnafu)?;

        IndexManager::get_subjects(&table, vault_id, resource, relation)
    }

    /// List resource-relation pairs for a given subject.
    pub fn list_resources_for_subject(
        &self,
        vault_id: VaultId,
        subject: &str,
    ) -> Result<Vec<(String, String)>> {
        let txn = self.db.begin_read().context(TransactionSnafu)?;
        let table = txn.open_table(Tables::SUBJ_INDEX).context(TableSnafu)?;

        IndexManager::get_resources(&table, vault_id, subject)
    }

    /// List all entities in a vault with optional prefix filter.
    ///
    /// Returns up to `limit` entities. Use `start_after` for pagination.
    pub fn list_entities(
        &self,
        vault_id: VaultId,
        prefix: Option<&str>,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Entity>> {
        let txn = self.db.begin_read().context(TransactionSnafu)?;
        let table = txn.open_table(Tables::ENTITIES).context(TableSnafu)?;

        let mut entities = Vec::with_capacity(limit.min(1000));

        // Build the range start key
        let start_key = if let Some(after) = start_after {
            // Start after this key
            let mut k = encode_storage_key(vault_id, after.as_bytes());
            // Add a byte to get past this key
            k.push(0);
            k
        } else if let Some(p) = prefix {
            encode_storage_key(vault_id, p.as_bytes())
        } else {
            encode_storage_key(vault_id, &[])
        };

        for result in table.range(&start_key[..]..).context(StorageSnafu)? {
            if entities.len() >= limit {
                break;
            }

            let (key, value) = result.context(StorageSnafu)?;
            let key_bytes = key.value();

            // Check we're still in the same vault (first 8 bytes)
            if key_bytes.len() < 9 {
                break;
            }
            let key_vault_id = i64::from_be_bytes(key_bytes[..8].try_into().unwrap_or([0; 8]));
            if key_vault_id != vault_id {
                break;
            }

            // Check prefix match if specified
            if let Some(p) = prefix {
                // Skip bucket byte (index 8), entity key starts at index 9
                let entity_key = &key_bytes[9..];
                if !entity_key.starts_with(p.as_bytes()) {
                    // If we've passed the prefix range, stop
                    if entity_key > p.as_bytes() {
                        break;
                    }
                    continue;
                }
            }

            let entity: Entity =
                postcard::from_bytes(value.value()).map_err(|e| StateError::Serialization {
                    message: e.to_string(),
                })?;

            entities.push(entity);
        }

        Ok(entities)
    }

    /// List all relationships in a vault.
    ///
    /// Returns up to `limit` relationships. Use `start_after` for pagination.
    pub fn list_relationships(
        &self,
        vault_id: VaultId,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Relationship>> {
        let txn = self.db.begin_read().context(TransactionSnafu)?;
        let table = txn.open_table(Tables::RELATIONSHIPS).context(TableSnafu)?;

        let mut relationships = Vec::with_capacity(limit.min(1000));

        // Build the range start key
        let start_key = if let Some(after) = start_after {
            let mut k = encode_storage_key(vault_id, after.as_bytes());
            k.push(0);
            k
        } else {
            encode_storage_key(vault_id, &[])
        };

        for result in table.range(&start_key[..]..).context(StorageSnafu)? {
            if relationships.len() >= limit {
                break;
            }

            let (key, value) = result.context(StorageSnafu)?;
            let key_bytes = key.value();

            // Check we're still in the same vault
            if key_bytes.len() < 9 {
                break;
            }
            let key_vault_id = i64::from_be_bytes(key_bytes[..8].try_into().unwrap_or([0; 8]));
            if key_vault_id != vault_id {
                break;
            }

            let rel: Relationship =
                postcard::from_bytes(value.value()).map_err(|e| StateError::Serialization {
                    message: e.to_string(),
                })?;

            relationships.push(rel);
        }

        Ok(relationships)
    }
}

impl Clone for StateLayer {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            vault_commitments: DashMap::new(), // Each clone starts fresh
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::StorageEngine;

    fn create_test_state() -> StateLayer {
        let engine = StorageEngine::open_in_memory().expect("open engine");
        StateLayer::new(engine.db())
    }

    #[test]
    fn test_set_and_get_entity() {
        let state = create_test_state();
        let vault_id = 1;

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
        let vault_id = 1;

        // First set
        let ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value1".to_vec(),
            condition: Some(SetCondition::MustNotExist),
            expires_at: None,
        }];
        let statuses = state.apply_operations(vault_id, &ops, 1).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Created]);

        // Second set should fail
        let ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value2".to_vec(),
            condition: Some(SetCondition::MustNotExist),
            expires_at: None,
        }];
        let statuses = state.apply_operations(vault_id, &ops, 2).unwrap();
        assert_eq!(statuses, vec![WriteStatus::PreconditionFailed]);
    }

    #[test]
    fn test_delete_entity() {
        let state = create_test_state();
        let vault_id = 1;

        // Set then delete
        let ops = vec![Operation::SetEntity {
            key: "key".to_string(),
            value: b"value".to_vec(),
            condition: None,
            expires_at: None,
        }];
        state.apply_operations(vault_id, &ops, 1).unwrap();

        let ops = vec![Operation::DeleteEntity {
            key: "key".to_string(),
        }];
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
        let vault_id = 1;

        let ops = vec![Operation::CreateRelationship {
            resource: "doc:123".to_string(),
            relation: "viewer".to_string(),
            subject: "user:alice".to_string(),
        }];

        let statuses = state.apply_operations(vault_id, &ops, 1).unwrap();
        assert_eq!(statuses, vec![WriteStatus::Created]);

        assert!(
            state
                .relationship_exists(vault_id, "doc:123", "viewer", "user:alice")
                .unwrap()
        );

        // Create again should return AlreadyExists
        let statuses = state.apply_operations(vault_id, &ops, 2).unwrap();
        assert_eq!(statuses, vec![WriteStatus::AlreadyExists]);
    }

    #[test]
    fn test_delete_relationship() {
        let state = create_test_state();
        let vault_id = 1;

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

        assert!(
            !state
                .relationship_exists(vault_id, "doc:123", "viewer", "user:alice")
                .unwrap()
        );
    }

    #[test]
    fn test_state_root_changes_on_writes() {
        let state = create_test_state();
        let vault_id = 1;

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
        let ops = vec![Operation::DeleteEntity {
            key: "key".to_string(),
        }];
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
        state.apply_operations(1, &ops, 1).unwrap();

        // Vault 2 should not see vault 1's data
        assert!(state.get_entity(2, b"key").unwrap().is_none());

        // State roots should differ
        let root1 = state.compute_state_root(1).unwrap();
        let root2 = state.compute_state_root(2).unwrap();
        assert_ne!(root1, root2);
    }
}
