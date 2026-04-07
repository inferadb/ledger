//! Dual indexes for relationship queries.
//!
//! Provides two indexes for efficient relationship lookups:
//! - Object index: "Who can access doc:123 as viewer?" → list of subjects
//! - Subject index: "What can user:alice access?" → list of (resource, relation) pairs
//!
//! Each relationship produces one entry per index table with an empty value.
//! Queries use B+ tree prefix scans, making writes O(log n) instead of the
//! previous O(n) approach that serialized entire sets.
//!
//! Indexes are NOT Merkleized (no per-write amplification).

use inferadb_ledger_store::{ReadTransaction, StorageBackend, TableId, WriteTransaction, tables};
use inferadb_ledger_types::VaultId;
use snafu::{ResultExt, Snafu};

use crate::{
    binary_keys::{
        InternCategory, decode_relationship_local_key, decode_subj_index_local_key,
        encode_obj_index_local_key, encode_obj_index_prefix, encode_subj_index_local_key,
        encode_subj_index_prefix,
    },
    dictionary::VaultDictionary,
    keys::encode_index_key,
};

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

    /// Dictionary operation failed.
    #[snafu(display("Dictionary error: {source}"))]
    Dictionary {
        source: crate::dictionary::DictionaryError,
        #[snafu(implicit)]
        location: snafu::Location,
    },
}

/// Result type for index operations.
pub type Result<T> = std::result::Result<T, IndexError>;

/// Dual index manager for relationship queries.
///
/// Maintains two non-Merkleized indexes per vault using decomposed B+ tree keys:
/// - **Object index**: one entry per (resource, relation, subject) triple, keyed as
///   `[vault:8BE][res_type:2BE][res_id_len:2LE][res_id][rel:2BE][subj_type:2BE][subj_id_len:
///   2LE][subj_id]` with an empty value. Prefix scans on `[vault][res_type][res_id][rel]` answer
///   "who can access this resource with this relation?"
/// - **Subject index**: one entry per (subject, resource, relation) triple, keyed as
///   `[vault:8BE][subj_type:2BE][subj_id_len:2LE][subj_id][res_type:2BE][res_id_len:
///   2LE][res_id][rel:2BE]` with an empty value. Prefix scans on `[vault][subj_type][subj_id]`
///   answer "what resources can this subject access?"
///
/// These indexes are maintained by
/// [`StateLayer::apply_operations`](crate::StateLayer::apply_operations) when creating or deleting
/// relationships. They are not part of the state root hash.
pub struct IndexManager;

#[allow(clippy::result_large_err)]
impl IndexManager {
    /// Adds a subject to the object index.
    ///
    /// Interns all strings via `dict`, builds the composite binary key, and
    /// inserts an entry with an empty value into the ObjIndex table.
    ///
    /// # Errors
    ///
    /// Returns `IndexError::Dictionary` if string interning fails.
    /// Returns `IndexError::Storage` if the write transaction fails.
    pub fn add_to_obj_index<B: StorageBackend>(
        txn: &mut WriteTransaction<'_, B>,
        dict: &mut VaultDictionary,
        vault: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<()> {
        let (res_type, res_id) = dict
            .intern_typed_id(txn, InternCategory::ResourceType, resource)
            .context(DictionarySnafu)?;
        let rel_id =
            dict.intern(txn, InternCategory::Relation, relation).context(DictionarySnafu)?;
        let (subj_type, subj_id) = dict
            .intern_typed_id(txn, InternCategory::SubjectType, subject)
            .context(DictionarySnafu)?;

        let local_key = encode_obj_index_local_key(res_type, &res_id, rel_id, subj_type, &subj_id);
        let storage_key = encode_index_key(vault, &local_key);

        txn.insert_raw(TableId::ObjIndex, &storage_key, &[]).context(StorageSnafu)?;

        Ok(())
    }

    /// Removes a subject from the object index.
    ///
    /// Resolves strings via `dict` (read-only — strings must already be interned).
    /// If any string is unknown, returns `Ok(())` since the entry cannot exist.
    ///
    /// # Errors
    ///
    /// Returns `IndexError::Storage` if the delete operation fails.
    pub fn remove_from_obj_index<B: StorageBackend>(
        txn: &mut WriteTransaction<'_, B>,
        dict: &VaultDictionary,
        vault: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<()> {
        let Some((res_type, res_id)) =
            resolve_typed_id(dict, InternCategory::ResourceType, resource)
        else {
            return Ok(());
        };
        let Some(rel_id) = dict.get_id(InternCategory::Relation, relation) else {
            return Ok(());
        };
        let Some((subj_type, subj_id)) =
            resolve_typed_id(dict, InternCategory::SubjectType, subject)
        else {
            return Ok(());
        };

        let local_key = encode_obj_index_local_key(res_type, &res_id, rel_id, subj_type, &subj_id);
        let storage_key = encode_index_key(vault, &local_key);

        let _ = txn.delete_raw(TableId::ObjIndex, &storage_key);

        Ok(())
    }

    /// Adds a resource-relation pair to the subject index.
    ///
    /// Interns all strings via `dict`, builds the composite binary key, and
    /// inserts an entry with an empty value into the SubjIndex table.
    ///
    /// # Errors
    ///
    /// Returns `IndexError::Dictionary` if string interning fails.
    /// Returns `IndexError::Storage` if the write transaction fails.
    pub fn add_to_subj_index<B: StorageBackend>(
        txn: &mut WriteTransaction<'_, B>,
        dict: &mut VaultDictionary,
        vault: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<()> {
        let (res_type, res_id) = dict
            .intern_typed_id(txn, InternCategory::ResourceType, resource)
            .context(DictionarySnafu)?;
        let rel_id =
            dict.intern(txn, InternCategory::Relation, relation).context(DictionarySnafu)?;
        let (subj_type, subj_id) = dict
            .intern_typed_id(txn, InternCategory::SubjectType, subject)
            .context(DictionarySnafu)?;

        let local_key = encode_subj_index_local_key(subj_type, &subj_id, res_type, &res_id, rel_id);
        let storage_key = encode_index_key(vault, &local_key);

        txn.insert_raw(TableId::SubjIndex, &storage_key, &[]).context(StorageSnafu)?;

        Ok(())
    }

    /// Removes a resource-relation pair from the subject index.
    ///
    /// Resolves strings via `dict` (read-only — strings must already be interned).
    /// If any string is unknown, returns `Ok(())` since the entry cannot exist.
    ///
    /// # Errors
    ///
    /// Returns `IndexError::Storage` if the delete operation fails.
    pub fn remove_from_subj_index<B: StorageBackend>(
        txn: &mut WriteTransaction<'_, B>,
        dict: &VaultDictionary,
        vault: VaultId,
        resource: &str,
        relation: &str,
        subject: &str,
    ) -> Result<()> {
        let Some((subj_type, subj_id)) =
            resolve_typed_id(dict, InternCategory::SubjectType, subject)
        else {
            return Ok(());
        };
        let Some((res_type, res_id)) =
            resolve_typed_id(dict, InternCategory::ResourceType, resource)
        else {
            return Ok(());
        };
        let Some(rel_id) = dict.get_id(InternCategory::Relation, relation) else {
            return Ok(());
        };

        let local_key = encode_subj_index_local_key(subj_type, &subj_id, res_type, &res_id, rel_id);
        let storage_key = encode_index_key(vault, &local_key);

        let _ = txn.delete_raw(TableId::SubjIndex, &storage_key);

        Ok(())
    }

    /// Returns subjects for a resource and relation (object index lookup).
    ///
    /// Performs a prefix scan over the ObjIndex table, decoding each matching
    /// key to extract the subject type and local ID, then resolving back to
    /// the original string via the dictionary.
    ///
    /// # Errors
    ///
    /// Returns `IndexError::Storage` if the read transaction or iteration fails.
    pub fn get_subjects<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        dict: &VaultDictionary,
        vault: VaultId,
        resource: &str,
        relation: &str,
    ) -> Result<Vec<String>> {
        let Some((res_type, res_id)) =
            resolve_typed_id(dict, InternCategory::ResourceType, resource)
        else {
            return Ok(Vec::new());
        };
        let Some(rel_id) = dict.get_id(InternCategory::Relation, relation) else {
            return Ok(Vec::new());
        };

        let local_prefix = encode_obj_index_prefix(res_type, &res_id, rel_id);
        let prefix = encode_index_key(vault, &local_prefix);

        let iter = txn.iter::<tables::ObjIndex>().context(StorageSnafu)?;

        let mut subjects = Vec::new();
        for (key_bytes, _value) in iter {
            if key_bytes.starts_with(&prefix) {
                // Extract the local key (skip the 8-byte vault prefix).
                let local_key = &key_bytes[8..];
                if let Some((_, _, _, subj_type, subj_id)) =
                    decode_relationship_local_key(local_key)
                    && let Some(subject_str) =
                        dict.resolve_typed_id(InternCategory::SubjectType, subj_type, subj_id)
                {
                    subjects.push(subject_str);
                }
            } else if key_bytes > prefix {
                break;
            }
            // key_bytes < prefix: continue (skip)
        }

        Ok(subjects)
    }

    /// Returns resource-relation pairs for a subject (subject index lookup).
    ///
    /// Performs a prefix scan over the SubjIndex table, decoding each matching
    /// key to extract the resource type, resource local ID, and relation,
    /// then resolving back to original strings via the dictionary.
    ///
    /// # Errors
    ///
    /// Returns `IndexError::Storage` if the read transaction or iteration fails.
    pub fn get_resources<B: StorageBackend>(
        txn: &ReadTransaction<'_, B>,
        dict: &VaultDictionary,
        vault: VaultId,
        subject: &str,
    ) -> Result<Vec<(String, String)>> {
        let Some((subj_type, subj_id)) =
            resolve_typed_id(dict, InternCategory::SubjectType, subject)
        else {
            return Ok(Vec::new());
        };

        let local_prefix = encode_subj_index_prefix(subj_type, &subj_id);
        let prefix = encode_index_key(vault, &local_prefix);

        let iter = txn.iter::<tables::SubjIndex>().context(StorageSnafu)?;

        let mut resources = Vec::new();
        for (key_bytes, _value) in iter {
            if key_bytes.starts_with(&prefix) {
                // Extract the local key (skip the 8-byte vault prefix).
                let local_key = &key_bytes[8..];
                if let Some((_, _, res_type, res_id, rel_id)) =
                    decode_subj_index_local_key(local_key)
                {
                    let resource_str =
                        dict.resolve_typed_id(InternCategory::ResourceType, res_type, res_id);
                    let relation_str = dict.resolve(InternCategory::Relation, rel_id);
                    if let (Some(r), Some(rel)) = (resource_str, relation_str) {
                        resources.push((r, rel.to_owned()));
                    }
                }
            } else if key_bytes > prefix {
                break;
            }
            // key_bytes < prefix: continue (skip)
        }

        Ok(resources)
    }
}

/// Resolves a typed ID string to its interned components without mutating the dictionary.
///
/// Splits on the first colon and looks up the type portion via `dict.get_id`.
/// Returns `None` if the type is not interned (meaning the entry cannot exist in storage).
fn resolve_typed_id(
    dict: &VaultDictionary,
    category: InternCategory,
    typed_id: &str,
) -> Option<(crate::binary_keys::InternId, Vec<u8>)> {
    let (type_str, local_str) = match crate::binary_keys::split_typed_id(typed_id) {
        Some((t, l)) => (t, l),
        None => ("", typed_id),
    };

    let type_id = dict.get_id(category, type_str)?;
    Some((type_id, local_str.as_bytes().to_vec()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, unused_mut)]
mod tests {
    use super::*;
    use crate::{dictionary::VaultDictionary, engine::InMemoryStorageEngine};

    fn setup() -> (InMemoryStorageEngine, VaultId) {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        (engine, VaultId::new(1))
    }

    #[test]
    fn add_and_get_subjects() {
        let (engine, vault) = setup();
        let db = engine.db();
        let mut dict = VaultDictionary::new(vault);

        {
            let mut txn = db.write().expect("begin write");
            IndexManager::add_to_obj_index(
                &mut txn,
                &mut dict,
                vault,
                "doc:123",
                "viewer",
                "user:alice",
            )
            .expect("add");
            IndexManager::add_to_obj_index(
                &mut txn, &mut dict, vault, "doc:123", "viewer", "user:bob",
            )
            .expect("add");
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");
            let subjects =
                IndexManager::get_subjects(&txn, &dict, vault, "doc:123", "viewer").expect("get");
            assert_eq!(subjects.len(), 2);
            assert!(subjects.contains(&"user:alice".to_string()));
            assert!(subjects.contains(&"user:bob".to_string()));
        }
    }

    #[test]
    fn remove_from_obj_index() {
        let (engine, vault) = setup();
        let db = engine.db();
        let mut dict = VaultDictionary::new(vault);

        {
            let mut txn = db.write().expect("begin write");
            IndexManager::add_to_obj_index(
                &mut txn,
                &mut dict,
                vault,
                "doc:123",
                "viewer",
                "user:alice",
            )
            .expect("add");
            IndexManager::add_to_obj_index(
                &mut txn, &mut dict, vault, "doc:123", "viewer", "user:bob",
            )
            .expect("add");
            txn.commit().expect("commit");
        }

        {
            let mut txn = db.write().expect("begin write");
            IndexManager::remove_from_obj_index(
                &mut txn,
                &dict,
                vault,
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
                IndexManager::get_subjects(&txn, &dict, vault, "doc:123", "viewer").expect("get");
            assert_eq!(subjects.len(), 1);
            assert!(subjects.contains(&"user:bob".to_string()));
        }
    }

    #[test]
    fn add_and_get_resources_for_subject() {
        let (engine, vault) = setup();
        let db = engine.db();
        let mut dict = VaultDictionary::new(vault);

        {
            let mut txn = db.write().expect("begin write");
            IndexManager::add_to_subj_index(
                &mut txn,
                &mut dict,
                vault,
                "doc:123",
                "viewer",
                "user:alice",
            )
            .expect("add");
            IndexManager::add_to_subj_index(
                &mut txn,
                &mut dict,
                vault,
                "doc:456",
                "editor",
                "user:alice",
            )
            .expect("add");
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");
            let resources =
                IndexManager::get_resources(&txn, &dict, vault, "user:alice").expect("get");
            assert_eq!(resources.len(), 2);
            assert!(resources.contains(&("doc:123".to_string(), "viewer".to_string())));
            assert!(resources.contains(&("doc:456".to_string(), "editor".to_string())));
        }
    }

    #[test]
    fn remove_from_subj_index() {
        let (engine, vault) = setup();
        let db = engine.db();
        let mut dict = VaultDictionary::new(vault);

        {
            let mut txn = db.write().expect("begin write");
            IndexManager::add_to_subj_index(
                &mut txn,
                &mut dict,
                vault,
                "doc:123",
                "viewer",
                "user:alice",
            )
            .expect("add");
            IndexManager::add_to_subj_index(
                &mut txn,
                &mut dict,
                vault,
                "doc:456",
                "editor",
                "user:alice",
            )
            .expect("add");
            txn.commit().expect("commit");
        }

        {
            let mut txn = db.write().expect("begin write");
            IndexManager::remove_from_subj_index(
                &mut txn,
                &dict,
                vault,
                "doc:123",
                "viewer",
                "user:alice",
            )
            .expect("remove");
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");
            let resources =
                IndexManager::get_resources(&txn, &dict, vault, "user:alice").expect("get");
            assert_eq!(resources.len(), 1);
            assert!(resources.contains(&("doc:456".to_string(), "editor".to_string())));
        }
    }

    #[test]
    fn vault_isolation() {
        let (engine, vault1) = setup();
        let vault2 = VaultId::new(2);
        let db = engine.db();
        let mut dict1 = VaultDictionary::new(vault1);
        let mut dict2 = VaultDictionary::new(vault2);

        {
            let mut txn = db.write().expect("begin write");
            IndexManager::add_to_obj_index(
                &mut txn,
                &mut dict1,
                vault1,
                "doc:123",
                "viewer",
                "user:alice",
            )
            .expect("add");
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");
            let subjects =
                IndexManager::get_subjects(&txn, &dict2, vault2, "doc:123", "viewer").expect("get");
            assert!(subjects.is_empty());
        }
    }

    #[test]
    fn idempotent_add() {
        let (engine, vault) = setup();
        let db = engine.db();
        let mut dict = VaultDictionary::new(vault);

        {
            let mut txn = db.write().expect("begin write");
            IndexManager::add_to_obj_index(
                &mut txn,
                &mut dict,
                vault,
                "doc:123",
                "viewer",
                "user:alice",
            )
            .expect("add");
            IndexManager::add_to_obj_index(
                &mut txn,
                &mut dict,
                vault,
                "doc:123",
                "viewer",
                "user:alice",
            )
            .expect("add again");
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");
            let subjects =
                IndexManager::get_subjects(&txn, &dict, vault, "doc:123", "viewer").expect("get");
            assert_eq!(subjects.len(), 1, "duplicate add should be idempotent");
        }
    }

    #[test]
    fn high_fanout_subjects() {
        let (engine, vault) = setup();
        let db = engine.db();
        let mut dict = VaultDictionary::new(vault);

        {
            let mut txn = db.write().expect("begin write");
            for i in 0..1000 {
                let subject = format!("user:{i}");
                IndexManager::add_to_obj_index(
                    &mut txn,
                    &mut dict,
                    vault,
                    "doc:bigfile",
                    "viewer",
                    &subject,
                )
                .expect("add");
            }
            txn.commit().expect("commit");
        }

        {
            let txn = db.read().expect("begin read");
            let subjects = IndexManager::get_subjects(&txn, &dict, vault, "doc:bigfile", "viewer")
                .expect("get");
            assert_eq!(subjects.len(), 1000);
        }
    }

    #[test]
    fn get_subjects_unknown_resource_returns_empty() {
        let (engine, vault) = setup();
        let db = engine.db();
        let dict = VaultDictionary::new(vault);

        let txn = db.read().expect("begin read");
        let subjects = IndexManager::get_subjects(&txn, &dict, vault, "doc:nonexistent", "viewer")
            .expect("get");
        assert!(subjects.is_empty());
    }

    #[test]
    fn get_resources_unknown_subject_returns_empty() {
        let (engine, vault) = setup();
        let db = engine.db();
        let dict = VaultDictionary::new(vault);

        let txn = db.read().expect("begin read");
        let resources =
            IndexManager::get_resources(&txn, &dict, vault, "user:nobody").expect("get");
        assert!(resources.is_empty());
    }
}
