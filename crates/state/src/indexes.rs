//! Dual indexes for relationship queries.
//!
//! Provides two indexes for efficient relationship lookups:
//! - Object index: "Who can access doc:123 as viewer?" → list of subjects
//! - Subject index: "What can user:alice access?" → list of (resource, relation) pairs
//!
//! Per DESIGN.md: Indexes are NOT Merkleized (no per-write amplification).

use inferadb_ledger_store::{ReadTransaction, StorageBackend, WriteTransaction, tables};
use inferadb_ledger_types::{VaultId, decode, encode};
use snafu::{ResultExt, Snafu};

use crate::keys::{encode_obj_index_key, encode_storage_key, encode_subj_index_key};

/// Errors returned by [`IndexManager`] operations.
#[derive(Debug, Snafu)]
pub enum IndexError {
    /// Underlying storage operation failed.
    #[snafu(display("Storage error: {source}"))]
    Storage {
        source: inferadb_ledger_store::Error,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Serialization or deserialization failed.
    #[snafu(display("Codec error: {source}"))]
    Codec {
        source: inferadb_ledger_types::CodecError,
        #[snafu(implicit)]
        location: snafu::Location,
    },
}

/// Result type for index operations.
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
    /// Adds a subject to the object index.
    ///
    /// Key: `{vault_id}{bucket_id}obj_idx:{resource}#{relation}`
    /// Value: serialized list of subjects
    ///
    /// # Errors
    ///
    /// Returns `IndexError::Codec` if serialization of the subject set fails.
    /// Returns `IndexError::Storage` if the write transaction fails.
    pub fn add_to_obj_index<B: StorageBackend>(
        txn: &mut WriteTransaction<'_, B>,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<()> {
        let local_key = encode_obj_index_key(resource, relation);
        let storage_key = encode_storage_key(vault_id, &local_key);

        let mut set: SubjectSet = match txn.get::<tables::ObjIndex>(&storage_key) {
            Ok(Some(data)) => decode(&data).unwrap_or_default(),
            _ => SubjectSet::default(),
        };

        let subject_str = subject.to_string();
        if !set.subjects.contains(&subject_str) {
            set.subjects.push(subject_str);
        }

        let encoded = encode(&set).context(CodecSnafu)?;

        txn.insert::<tables::ObjIndex>(&storage_key, &encoded).context(StorageSnafu)?;

        Ok(())
    }

    /// Removes a subject from the object index.
    ///
    /// # Errors
    ///
    /// Returns `IndexError::Codec` if serialization of the updated subject set fails.
    /// Returns `IndexError::Storage` if the write transaction fails.
    pub fn remove_from_obj_index<B: StorageBackend>(
        txn: &mut WriteTransaction<'_, B>,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<()> {
        let local_key = encode_obj_index_key(resource, relation);
        let storage_key = encode_storage_key(vault_id, &local_key);

        let existing_set: Option<SubjectSet> = match txn.get::<tables::ObjIndex>(&storage_key) {
            Ok(Some(data)) => Some(decode(&data).unwrap_or_default()),
            _ => None,
        };

        let Some(mut set) = existing_set else {
            return Ok(());
        };

        set.subjects.retain(|s| s != subject);

        if set.subjects.is_empty() {
            let _ = txn.delete::<tables::ObjIndex>(&storage_key);
        } else {
            let encoded = encode(&set).context(CodecSnafu)?;

            txn.insert::<tables::ObjIndex>(&storage_key, &encoded).context(StorageSnafu)?;
        }

        Ok(())
    }

    /// Adds a resource-relation pair to the subject index.
    ///
    /// Key: `{vault_id}{bucket_id}subj_idx:{subject}`
    /// Value: serialized list of (resource, relation) pairs
    ///
    /// # Errors
    ///
    /// Returns `IndexError::Codec` if serialization of the resource-relation set fails.
    /// Returns `IndexError::Storage` if the write transaction fails.
    pub fn add_to_subj_index<B: StorageBackend>(
        txn: &mut WriteTransaction<'_, B>,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<()> {
        let local_key = encode_subj_index_key(subject);
        let storage_key = encode_storage_key(vault_id, &local_key);

        let mut set: ResourceRelationSet = match txn.get::<tables::SubjIndex>(&storage_key) {
            Ok(Some(data)) => decode(&data).unwrap_or_default(),
            _ => ResourceRelationSet::default(),
        };

        let pair = (resource.to_string(), relation.to_string());
        if !set.pairs.contains(&pair) {
            set.pairs.push(pair);
        }

        let encoded = encode(&set).context(CodecSnafu)?;

        txn.insert::<tables::SubjIndex>(&storage_key, &encoded).context(StorageSnafu)?;

        Ok(())
    }

    /// Removes a resource-relation pair from the subject index.
    ///
    /// # Errors
    ///
    /// Returns `IndexError::Codec` if serialization of the updated resource-relation set fails.
    /// Returns `IndexError::Storage` if the write transaction fails.
    pub fn remove_from_subj_index<B: StorageBackend>(
        txn: &mut WriteTransaction<'_, B>,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<()> {
        let local_key = encode_subj_index_key(subject);
        let storage_key = encode_storage_key(vault_id, &local_key);

        let existing_set: Option<ResourceRelationSet> =
            match txn.get::<tables::SubjIndex>(&storage_key) {
                Ok(Some(data)) => Some(decode(&data).unwrap_or_default()),
                _ => None,
            };

        let Some(mut set) = existing_set else {
            return Ok(());
        };

        let pair = (resource.to_string(), relation.to_string());
        set.pairs.retain(|p| p != &pair);

        if set.pairs.is_empty() {
            let _ = txn.delete::<tables::SubjIndex>(&storage_key);
        } else {
            let encoded = encode(&set).context(CodecSnafu)?;

            txn.insert::<tables::SubjIndex>(&storage_key, &encoded).context(StorageSnafu)?;
        }

        Ok(())
    }

    /// Returns subjects for a resource and relation (object index lookup).
    ///
    /// # Errors
    ///
    /// Returns `IndexError::Storage` if the read transaction fails.
    /// Returns `IndexError::Codec` if deserialization of the subject set fails.
    pub fn get_subjects<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        vault_id: VaultId,
        resource: &str,
        relation: &str,
    ) -> Result<Vec<String>> {
        let local_key = encode_obj_index_key(resource, relation);
        let storage_key = encode_storage_key(vault_id, &local_key);

        let data = txn.get::<tables::ObjIndex>(&storage_key).context(StorageSnafu)?;
        match data {
            Some(bytes) => {
                let set: SubjectSet = decode(&bytes).context(CodecSnafu)?;
                Ok(set.subjects)
            },
            None => Ok(Vec::new()),
        }
    }

    /// Returns resource-relation pairs for a subject (subject index lookup).
    ///
    /// # Errors
    ///
    /// Returns `IndexError::Storage` if the read transaction fails.
    /// Returns `IndexError::Codec` if deserialization of the resource-relation set fails.
    pub fn get_resources<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        vault_id: VaultId,
        subject: &str,
    ) -> Result<Vec<(String, String)>> {
        let local_key = encode_subj_index_key(subject);
        let storage_key = encode_storage_key(vault_id, &local_key);

        let data = txn.get::<tables::SubjIndex>(&storage_key).context(StorageSnafu)?;
        match data {
            Some(bytes) => {
                let set: ResourceRelationSet = decode(&bytes).context(CodecSnafu)?;
                Ok(set.pairs)
            },
            None => Ok(Vec::new()),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, unused_mut)]
mod tests {
    use super::*;
    use crate::engine::InMemoryStorageEngine;

    #[test]
    fn test_obj_index_add_remove() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();
        let vault_id = VaultId::new(1);

        {
            let mut txn = db.write().expect("begin write");

            IndexManager::add_to_obj_index(&mut txn, vault_id, "doc:123", "viewer", "user:alice")
                .expect("add");
            IndexManager::add_to_obj_index(&mut txn, vault_id, "doc:123", "viewer", "user:bob")
                .expect("add");
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");

            let subjects =
                IndexManager::get_subjects(&txn, vault_id, "doc:123", "viewer").expect("get");
            assert_eq!(subjects.len(), 2);
            assert!(subjects.contains(&"user:alice".to_string()));
            assert!(subjects.contains(&"user:bob".to_string()));
        }

        {
            let mut txn = db.write().expect("begin write");

            IndexManager::remove_from_obj_index(
                &mut txn,
                vault_id,
                "doc:123",
                "viewer",
                "user:alice",
            )
            .expect("remove");
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");

            let subjects =
                IndexManager::get_subjects(&txn, vault_id, "doc:123", "viewer").expect("get");
            assert_eq!(subjects.len(), 1);
            assert!(subjects.contains(&"user:bob".to_string()));
        }
    }

    #[test]
    fn test_subj_index_add_remove() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();
        let vault_id = VaultId::new(1);

        {
            let mut txn = db.write().expect("begin write");

            IndexManager::add_to_subj_index(&mut txn, vault_id, "doc:123", "viewer", "user:alice")
                .expect("add");
            IndexManager::add_to_subj_index(&mut txn, vault_id, "doc:456", "editor", "user:alice")
                .expect("add");
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");

            let resources = IndexManager::get_resources(&txn, vault_id, "user:alice").expect("get");
            assert_eq!(resources.len(), 2);
            assert!(resources.contains(&("doc:123".to_string(), "viewer".to_string())));
            assert!(resources.contains(&("doc:456".to_string(), "editor".to_string())));
        }

        {
            let mut txn = db.write().expect("begin write");

            IndexManager::remove_from_subj_index(
                &mut txn,
                vault_id,
                "doc:123",
                "viewer",
                "user:alice",
            )
            .expect("remove");
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");

            let resources = IndexManager::get_resources(&txn, vault_id, "user:alice").expect("get");
            assert_eq!(resources.len(), 1);
            assert!(resources.contains(&("doc:456".to_string(), "editor".to_string())));
        }
    }

    #[test]
    fn test_vault_isolation() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();

        {
            let mut txn = db.write().expect("begin write");

            IndexManager::add_to_obj_index(
                &mut txn,
                VaultId::new(1),
                "doc:123",
                "viewer",
                "user:alice",
            )
            .expect("add");
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");

            let subjects = IndexManager::get_subjects(&txn, VaultId::new(2), "doc:123", "viewer")
                .expect("get");
            assert!(subjects.is_empty());
        }
    }

    #[test]
    fn test_duplicate_add_is_idempotent() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let db = engine.db();
        let vault_id = VaultId::new(1);

        {
            let mut txn = db.write().expect("begin write");

            IndexManager::add_to_obj_index(&mut txn, vault_id, "doc:123", "viewer", "user:alice")
                .expect("add");
            IndexManager::add_to_obj_index(&mut txn, vault_id, "doc:123", "viewer", "user:alice")
                .expect("add");
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");

            let subjects =
                IndexManager::get_subjects(&txn, vault_id, "doc:123", "viewer").expect("get");
            assert_eq!(subjects.len(), 1);
        }
    }
}
