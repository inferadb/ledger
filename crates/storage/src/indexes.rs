//! Dual indexes for relationship queries.
//!
//! Provides two indexes for efficient relationship lookups:
//! - Object index: "Who can access doc:123 as viewer?" → list of subjects
//! - Subject index: "What can user:alice access?" → list of (resource, relation) pairs
//!
//! Per DESIGN.md: Indexes are NOT merkleized (no per-write amplification).

use redb::{ReadOnlyTable, ReadableTable, Table};
use snafu::Snafu;

use ledger_types::VaultId;

use crate::keys::{encode_obj_index_key, encode_storage_key, encode_subj_index_key};
use crate::state::StateError;

/// Index manager error types.
#[derive(Debug, Snafu)]
#[allow(dead_code)]
pub enum IndexError {
    #[snafu(display("Storage error: {source}"))]
    Storage { source: redb::StorageError },

    #[snafu(display("Serialization error: {message}"))]
    Serialization { message: String },
}

/// Result type for index operations.
#[allow(dead_code)]
pub type Result<T> = std::result::Result<T, IndexError>;

/// Index entry for object index: list of subjects.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct SubjectSet {
    subjects: Vec<String>,
}

/// Index entry for subject index: list of (resource, relation) pairs.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct ResourceRelationSet {
    pairs: Vec<(String, String)>,
}

/// Dual index manager for relationship queries.
pub struct IndexManager;

#[allow(clippy::result_large_err)]
impl IndexManager {
    /// Add a subject to the object index.
    ///
    /// Key: {vault_id}{bucket_id}obj_idx:{resource}#{relation}
    /// Value: serialized list of subjects
    pub fn add_to_obj_index(
        table: &mut Table<'_, &'static [u8], &'static [u8]>,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> std::result::Result<(), StateError> {
        let local_key = encode_obj_index_key(resource, relation);
        let storage_key = encode_storage_key(vault_id, &local_key);

        // Get existing set
        let mut set: SubjectSet = match table.get(&storage_key[..]) {
            Ok(Some(data)) => postcard::from_bytes(data.value()).unwrap_or_default(),
            _ => SubjectSet::default(),
        };

        // Add subject if not present
        let subject_str = subject.to_string();
        if !set.subjects.contains(&subject_str) {
            set.subjects.push(subject_str);
        }

        // Store updated set
        let encoded = postcard::to_allocvec(&set).map_err(|e| StateError::Serialization {
            message: e.to_string(),
        })?;

        table
            .insert(&storage_key[..], &encoded[..])
            .map_err(|e| StateError::Storage { source: e })?;

        Ok(())
    }

    /// Remove a subject from the object index.
    pub fn remove_from_obj_index(
        table: &mut Table<'_, &'static [u8], &'static [u8]>,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> std::result::Result<(), StateError> {
        let local_key = encode_obj_index_key(resource, relation);
        let storage_key = encode_storage_key(vault_id, &local_key);

        // Get existing set - extract data and drop borrow before mutation
        let existing_set: Option<SubjectSet> = {
            match table.get(&storage_key[..]) {
                Ok(Some(data)) => Some(postcard::from_bytes(data.value()).unwrap_or_default()),
                _ => None,
            }
        };

        let Some(mut set) = existing_set else {
            return Ok(());
        };

        // Remove subject
        set.subjects.retain(|s| s != subject);

        if set.subjects.is_empty() {
            // Remove the entire entry if no subjects left
            let _ = table.remove(&storage_key[..]);
        } else {
            // Store updated set
            let encoded = postcard::to_allocvec(&set).map_err(|e| StateError::Serialization {
                message: e.to_string(),
            })?;

            table
                .insert(&storage_key[..], &encoded[..])
                .map_err(|e| StateError::Storage { source: e })?;
        }

        Ok(())
    }

    /// Add a resource-relation pair to the subject index.
    ///
    /// Key: {vault_id}{bucket_id}subj_idx:{subject}
    /// Value: serialized list of (resource, relation) pairs
    pub fn add_to_subj_index(
        table: &mut Table<'_, &'static [u8], &'static [u8]>,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> std::result::Result<(), StateError> {
        let local_key = encode_subj_index_key(subject);
        let storage_key = encode_storage_key(vault_id, &local_key);

        // Get existing set
        let mut set: ResourceRelationSet = match table.get(&storage_key[..]) {
            Ok(Some(data)) => postcard::from_bytes(data.value()).unwrap_or_default(),
            _ => ResourceRelationSet::default(),
        };

        // Add pair if not present
        let pair = (resource.to_string(), relation.to_string());
        if !set.pairs.contains(&pair) {
            set.pairs.push(pair);
        }

        // Store updated set
        let encoded = postcard::to_allocvec(&set).map_err(|e| StateError::Serialization {
            message: e.to_string(),
        })?;

        table
            .insert(&storage_key[..], &encoded[..])
            .map_err(|e| StateError::Storage { source: e })?;

        Ok(())
    }

    /// Remove a resource-relation pair from the subject index.
    pub fn remove_from_subj_index(
        table: &mut Table<'_, &'static [u8], &'static [u8]>,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> std::result::Result<(), StateError> {
        let local_key = encode_subj_index_key(subject);
        let storage_key = encode_storage_key(vault_id, &local_key);

        // Get existing set - extract data and drop borrow before mutation
        let existing_set: Option<ResourceRelationSet> = {
            match table.get(&storage_key[..]) {
                Ok(Some(data)) => Some(postcard::from_bytes(data.value()).unwrap_or_default()),
                _ => None,
            }
        };

        let Some(mut set) = existing_set else {
            return Ok(());
        };

        // Remove pair
        let pair = (resource.to_string(), relation.to_string());
        set.pairs.retain(|p| p != &pair);

        if set.pairs.is_empty() {
            // Remove the entire entry if no pairs left
            let _ = table.remove(&storage_key[..]);
        } else {
            // Store updated set
            let encoded = postcard::to_allocvec(&set).map_err(|e| StateError::Serialization {
                message: e.to_string(),
            })?;

            table
                .insert(&storage_key[..], &encoded[..])
                .map_err(|e| StateError::Storage { source: e })?;
        }

        Ok(())
    }

    /// Get subjects for a resource and relation (object index lookup).
    pub fn get_subjects(
        table: &ReadOnlyTable<&'static [u8], &'static [u8]>,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
    ) -> std::result::Result<Vec<String>, StateError> {
        let local_key = encode_obj_index_key(resource, relation);
        let storage_key = encode_storage_key(vault_id, &local_key);

        match table.get(&storage_key[..]) {
            Ok(Some(data)) => {
                let set: SubjectSet =
                    postcard::from_bytes(data.value()).map_err(|e| StateError::Serialization {
                        message: e.to_string(),
                    })?;
                Ok(set.subjects)
            }
            Ok(None) => Ok(Vec::new()),
            Err(e) => Err(StateError::Storage { source: e }),
        }
    }

    /// Get resource-relation pairs for a subject (subject index lookup).
    pub fn get_resources(
        table: &ReadOnlyTable<&'static [u8], &'static [u8]>,
        vault_id: VaultId,
        subject: &str,
    ) -> std::result::Result<Vec<(String, String)>, StateError> {
        let local_key = encode_subj_index_key(subject);
        let storage_key = encode_storage_key(vault_id, &local_key);

        match table.get(&storage_key[..]) {
            Ok(Some(data)) => {
                let set: ResourceRelationSet =
                    postcard::from_bytes(data.value()).map_err(|e| StateError::Serialization {
                        message: e.to_string(),
                    })?;
                Ok(set.pairs)
            }
            Ok(None) => Ok(Vec::new()),
            Err(e) => Err(StateError::Storage { source: e }),
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    unused_mut
)]
mod tests {
    use super::*;
    use crate::engine::StorageEngine;
    use crate::tables::Tables;

    #[test]
    fn test_obj_index_add_remove() {
        let engine = StorageEngine::open_in_memory().expect("open engine");
        let vault_id = 1;

        // Add subjects
        {
            let mut txn = engine.begin_write().expect("begin write");
            {
                let mut table = txn.open_table(Tables::OBJ_INDEX).expect("open table");

                IndexManager::add_to_obj_index(
                    &mut table,
                    vault_id,
                    "doc:123",
                    "viewer",
                    "user:alice",
                )
                .expect("add");
                IndexManager::add_to_obj_index(
                    &mut table, vault_id, "doc:123", "viewer", "user:bob",
                )
                .expect("add");
            }
            txn.commit().expect("commit");
        }

        // Query
        {
            let txn = engine.begin_read().expect("begin read");
            let table = txn.open_table(Tables::OBJ_INDEX).expect("open table");

            let subjects =
                IndexManager::get_subjects(&table, vault_id, "doc:123", "viewer").expect("get");
            assert_eq!(subjects.len(), 2);
            assert!(subjects.contains(&"user:alice".to_string()));
            assert!(subjects.contains(&"user:bob".to_string()));
        }

        // Remove one
        {
            let mut txn = engine.begin_write().expect("begin write");
            {
                let mut table = txn.open_table(Tables::OBJ_INDEX).expect("open table");

                IndexManager::remove_from_obj_index(
                    &mut table,
                    vault_id,
                    "doc:123",
                    "viewer",
                    "user:alice",
                )
                .expect("remove");
            }
            txn.commit().expect("commit");
        }

        // Verify
        {
            let txn = engine.begin_read().expect("begin read");
            let table = txn.open_table(Tables::OBJ_INDEX).expect("open table");

            let subjects =
                IndexManager::get_subjects(&table, vault_id, "doc:123", "viewer").expect("get");
            assert_eq!(subjects.len(), 1);
            assert!(subjects.contains(&"user:bob".to_string()));
        }
    }

    #[test]
    fn test_subj_index_add_remove() {
        let engine = StorageEngine::open_in_memory().expect("open engine");
        let vault_id = 1;

        // Add resources
        {
            let mut txn = engine.begin_write().expect("begin write");
            {
                let mut table = txn.open_table(Tables::SUBJ_INDEX).expect("open table");

                IndexManager::add_to_subj_index(
                    &mut table,
                    vault_id,
                    "doc:123",
                    "viewer",
                    "user:alice",
                )
                .expect("add");
                IndexManager::add_to_subj_index(
                    &mut table,
                    vault_id,
                    "doc:456",
                    "editor",
                    "user:alice",
                )
                .expect("add");
            }
            txn.commit().expect("commit");
        }

        // Query
        {
            let txn = engine.begin_read().expect("begin read");
            let table = txn.open_table(Tables::SUBJ_INDEX).expect("open table");

            let resources =
                IndexManager::get_resources(&table, vault_id, "user:alice").expect("get");
            assert_eq!(resources.len(), 2);
            assert!(resources.contains(&("doc:123".to_string(), "viewer".to_string())));
            assert!(resources.contains(&("doc:456".to_string(), "editor".to_string())));
        }

        // Remove one
        {
            let mut txn = engine.begin_write().expect("begin write");
            {
                let mut table = txn.open_table(Tables::SUBJ_INDEX).expect("open table");

                IndexManager::remove_from_subj_index(
                    &mut table,
                    vault_id,
                    "doc:123",
                    "viewer",
                    "user:alice",
                )
                .expect("remove");
            }
            txn.commit().expect("commit");
        }

        // Verify
        {
            let txn = engine.begin_read().expect("begin read");
            let table = txn.open_table(Tables::SUBJ_INDEX).expect("open table");

            let resources =
                IndexManager::get_resources(&table, vault_id, "user:alice").expect("get");
            assert_eq!(resources.len(), 1);
            assert!(resources.contains(&("doc:456".to_string(), "editor".to_string())));
        }
    }

    #[test]
    fn test_vault_isolation() {
        let engine = StorageEngine::open_in_memory().expect("open engine");

        // Add to vault 1
        {
            let mut txn = engine.begin_write().expect("begin write");
            {
                let mut table = txn.open_table(Tables::OBJ_INDEX).expect("open table");

                IndexManager::add_to_obj_index(&mut table, 1, "doc:123", "viewer", "user:alice")
                    .expect("add");
            }
            txn.commit().expect("commit");
        }

        // Vault 2 should not see vault 1's index
        {
            let txn = engine.begin_read().expect("begin read");
            let table = txn.open_table(Tables::OBJ_INDEX).expect("open table");

            let subjects = IndexManager::get_subjects(&table, 2, "doc:123", "viewer").expect("get");
            assert!(subjects.is_empty());
        }
    }

    #[test]
    fn test_duplicate_add_is_idempotent() {
        let engine = StorageEngine::open_in_memory().expect("open engine");
        let vault_id = 1;

        // Add same subject twice
        {
            let mut txn = engine.begin_write().expect("begin write");
            {
                let mut table = txn.open_table(Tables::OBJ_INDEX).expect("open table");

                IndexManager::add_to_obj_index(
                    &mut table,
                    vault_id,
                    "doc:123",
                    "viewer",
                    "user:alice",
                )
                .expect("add");
                IndexManager::add_to_obj_index(
                    &mut table,
                    vault_id,
                    "doc:123",
                    "viewer",
                    "user:alice",
                )
                .expect("add");
            }
            txn.commit().expect("commit");
        }

        // Should only have one entry
        {
            let txn = engine.begin_read().expect("begin read");
            let table = txn.open_table(Tables::OBJ_INDEX).expect("open table");

            let subjects =
                IndexManager::get_subjects(&table, vault_id, "doc:123", "viewer").expect("get");
            assert_eq!(subjects.len(), 1);
        }
    }
}
